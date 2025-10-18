import os
import time
import requests
from pytrends.request import TrendReq
from playwright.sync_api import sync_playwright
import json
import csv
import pandas as pd

# Optional: YouTube client
try:
    from googleapiclient.discovery import build
    _YT_AVAILABLE = True
except Exception:
    _YT_AVAILABLE = False

# ========== CONFIG ==========
TMDB_API_KEY = os.getenv("TMDB_API_KEY", "4f013f2a8509b8f4b1ef3205f0ca9f00")
OMDB_API_KEY = os.getenv("OMDB_API_KEY", "a07802fd")
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "fc19e67dc0msh440737eba6375e2p115ca3jsn3f044dbc1ea6")
X_API_KEY = os.getenv("X_API_KEY", "Y1VMYW0tXzlqTXFtdnFxNmFNRDM6MTpjaQ")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "AIzaSyD9aZ53NNbKrX9LApcS5Sow3ezYAkYEx2I")

# RapidAPI hosts
NETFLIX_HOST = "netflix54.p.rapidapi.com"
TIKTOK_HOST = "tiktok-all-in-one.p.rapidapi.com"

# Runtime config
TOP_N = 10
TMDB_TRENDING_PERIOD = "week"
PYTRENDS_TIMEFRAME = "today 12-m"

# Playwright browser instance (reused)
_browser = None
_context = None
_playwright = None

# ========== HELPERS ==========
def safe_json(resp):
    try:
        return resp.json()
    except Exception:
        return {"error": "invalid_json", "status_code": getattr(resp, "status_code", None), "text": getattr(resp, "text", "")[:400]}

def sleep_if_needed(seconds=0.5):
    time.sleep(seconds)

def convert_timestamps(obj):
    """Convert pandas Timestamps to strings recursively"""
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {k: convert_timestamps(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_timestamps(item) for item in obj]
    return obj

def init_playwright():
    """Initialize Playwright browser instance"""
    global _browser, _context, _playwright
    if _browser is None:
        _playwright = sync_playwright().start()
        _browser = _playwright.chromium.launch(headless=True)
        _context = _browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

def close_playwright():
    """Close Playwright browser"""
    global _browser, _context, _playwright
    if _context:
        _context.close()
        _context = None
    if _browser:
        _browser.close()
        _browser = None
    if _playwright:
        _playwright.stop()
        _playwright = None

# ========== TMDb: top trending ==========
def get_tmdb_top_trending(n=10, period="week"):
    if not TMDB_API_KEY:
        print("Error: Missing TMDB API key")
        return []
    
    url = f"https://api.themoviedb.org/3/trending/movie/{period}"
    params = {"api_key": TMDB_API_KEY, "language": "en-US"}
    
    try:
        r = requests.get(url, params=params, timeout=12)
        r.raise_for_status()
        data = safe_json(r)
        
        if not isinstance(data, dict):
            print(f"Error: Invalid response format: {data}")
            return []
            
        results = data.get("results", [])
        
        if not isinstance(results, list):
            print(f"Error: Results is not a list: {results}")
            return []
            
        print(f"Successfully fetched {len(results)} trending movies")
        return results[:n]
        
    except Exception as e:
        print(f"Error fetching TMDb trending: {str(e)}")
        return []

# ========== 1. METADATA ==========
def extract_metadata_from_tmdb_item(tmdb_item):
    metadata = {
        "tmdb_id": tmdb_item.get("id"),
        "title": tmdb_item.get("title") or tmdb_item.get("name"),
        "release_date": tmdb_item.get("release_date"),
        "original_language": tmdb_item.get("original_language"),
        "genre_ids": tmdb_item.get("genre_ids"),
        "popularity": tmdb_item.get("popularity"),
        "overview": tmdb_item.get("overview")
    }
    return metadata

def get_tmdb_movie_details(tmdb_id):
    url = f"https://api.themoviedb.org/3/movie/{tmdb_id}"
    params = {"api_key": TMDB_API_KEY, "language": "en-US"}
    r = requests.get(url, params=params, timeout=12)
    return safe_json(r)

# ========== 2. COMMERCIAL PERFORMANCE ==========
def get_commercial_from_tmdb(tmdb_id):
    url = f"https://api.themoviedb.org/3/movie/{tmdb_id}"
    params = {"api_key": TMDB_API_KEY}
    r = requests.get(url, params=params, timeout=12)
    data = safe_json(r)
    budget = data.get("budget")
    revenue = data.get("revenue")
    opening_weekend = None
    roi = None
    try:
        if budget and revenue and budget > 0:
            roi = round(revenue / budget, 3)
    except Exception:
        roi = None
    distribution = None
    return {"budget": budget, "revenue": revenue, "opening_weekend": opening_weekend, "roi": roi, "distribution": distribution, "raw": data}

# ========== 3. RATINGS ==========
def get_omdb_by_imdb(imdb_id):
    if not OMDB_API_KEY:
        return {"error": "missing_omdb_key"}
    url = "http://www.omdbapi.com/"
    params = {"i": imdb_id, "apikey": OMDB_API_KEY}
    r = requests.get(url, params=params, timeout=10)
    return safe_json(r)

def get_rotten_tomatoes_with_playwright(movie_title):
    """Crawl Rotten Tomatoes using Playwright"""
    try:
        init_playwright()
        page = _context.new_page()
        
        # Search for movie
        search_url = f"https://www.rottentomatoes.com/search?search={movie_title}"
        page.goto(search_url, wait_until="domcontentloaded", timeout=15000)
        page.wait_for_timeout(2000)
        
        # Find first result
        first_result = page.query_selector("search-page-media-row a")
        if not first_result:
            page.close()
            return {"message": "no search result"}
        
        movie_url = "https://www.rottentomatoes.com" + first_result.get_attribute("href")
        
        # Go to movie page
        page.goto(movie_url, wait_until="domcontentloaded", timeout=15000)
        page.wait_for_timeout(2000)
        
        # Extract scores
        score_board = page.query_selector("score-board")
        if score_board:
            result = {
                "url": movie_url,
                "tomatometer": score_board.get_attribute("tomatometerscore"),
                "audience": score_board.get_attribute("audiencescore")
            }
            page.close()
            return result
        else:
            page.close()
            return {"message": "score-board not found"}
            
    except Exception as e:
        return {"error": str(e)}

def get_ratings_full(tmdb_details, movie_title):
    out = {}
    out["tmdb_vote_average"] = tmdb_details.get("vote_average")
    imdb_id = tmdb_details.get("imdb_id")
    if imdb_id:
        out["omdb"] = get_omdb_by_imdb(imdb_id)
    else:
        out["omdb"] = {"message": "no_imdb_id_in_tmdb"}
    
    # Use Playwright for Rotten Tomatoes
    out["rotten_unofficial"] = get_rotten_tomatoes_with_playwright(movie_title)
    return out

# ========== 4. STREAMING POPULARITY ==========
def get_netflix_info(title):
    if not RAPIDAPI_KEY:
        return {"error": "missing_rapidapi_key"}
    url = "https://netflix54.p.rapidapi.com/search/"
    params = {"query": title, "offset": "0", "limit_titles": "3", "lang": "en"}
    headers = {"X-RapidAPI-Key": RAPIDAPI_KEY, "X-RapidAPI-Host": NETFLIX_HOST}
    r = requests.get(url, headers=headers, params=params, timeout=12)
    return safe_json(r)

# ========== 5. SOCIAL & PUBLIC INTEREST ==========
def get_google_trends_for_title(title):
    try:
        pytrends = TrendReq(hl='en-US', tz=360)
        pytrends.build_payload([title], timeframe=PYTRENDS_TIMEFRAME)
        df = pytrends.interest_over_time()
        data = df.reset_index().to_dict(orient="records")
        return {"interest_over_time": convert_timestamps(data)}
    except Exception as e:
        return {"error": str(e)}

def search_x_recent(query, max_results=10):
    url = "https://api.twitter.com/2/tweets/search/recent"
    params = {"query": query, "max_results": str(max_results), "tweet.fields": "created_at,public_metrics,text"}
    headers_bearer = {"Authorization": f"Bearer {X_API_KEY}"}
    try:
        r = requests.get(url, headers=headers_bearer, params=params, timeout=10)
        if r.status_code == 200:
            return safe_json(r)
        headers_x = {"X-Api-Key": X_API_KEY}
        r2 = requests.get(url, headers=headers_x, params=params, timeout=10)
        if r2.status_code == 200:
            return safe_json(r2)
        return {"attempts": [{"bearer_status": r.status_code, "bearer_text": r.text[:300]}, {"x_status": r2.status_code, "x_text": r2.text[:300]}]}
    except Exception as e:
        return {"error": str(e)}

def get_youtube_trailer_stats(title):
    if not _YT_AVAILABLE or not YOUTUBE_API_KEY:
        return {"error": "youtube_not_configured"}
    try:
        youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
        q = f"{title} official trailer"
        res = youtube.search().list(part="snippet", q=q, type="video", maxResults=1).execute()
        items = res.get("items", [])
        if not items:
            return {"message": "no_trailer_found"}
        vid = items[0]["id"]["videoId"]
        stats = youtube.videos().list(part="statistics,snippet", id=vid).execute()
        return stats.get("items", [])
    except Exception as e:
        return {"error": str(e)}

def get_tiktok_with_playwright(title):
    """Crawl TikTok using Playwright"""
    try:
        init_playwright()
        page = _context.new_page()
        
        hashtag = title.lower().replace(" ", "")
        tiktok_url = f"https://www.tiktok.com/tag/{hashtag}"
        
        page.goto(tiktok_url, wait_until="domcontentloaded", timeout=20000)
        page.wait_for_timeout(3000)
        
        # Try to extract video data
        videos = []
        video_elements = page.query_selector_all('[data-e2e="search-card-item"]')[:3]
        
        for elem in video_elements:
            try:
                title_elem = elem.query_selector('[data-e2e="search-card-title"]')
                author_elem = elem.query_selector('[data-e2e="search-card-user-unique-id"]')
                
                video_data = {
                    "title": title_elem.inner_text() if title_elem else None,
                    "author": author_elem.inner_text() if author_elem else None,
                }
                videos.append(video_data)
            except:
                continue
        
        page.close()
        return {"videos": videos, "count": len(videos)}
        
    except Exception as e:
        return {"error": str(e)}

# ========== 6. CREATIVE FACTORS ==========
def get_tmdb_credits(tmdb_id):
    url = f"https://api.themoviedb.org/3/movie/{tmdb_id}/credits"
    params = {"api_key": TMDB_API_KEY}
    r = requests.get(url, params=params, timeout=12)
    return safe_json(r)

def compute_star_power(cast_list):
    return {"actor_count": len(cast_list)}

# ========== 7. AWARDS ==========
def get_awards_placeholder(imdb_id):
    return {"Oscars": None, "GoldenGlobes": None, "FestivalSelections": None}

# ========== 8. CONTEXTUAL DATA ==========
def get_worldbank_gdp(country_code="US"):
    url = f"http://api.worldbank.org/v2/country/{country_code}/indicator/NY.GDP.MKTP.CD?format=json"
    r = requests.get(url, timeout=12)
    return safe_json(r)

# ========== FLATTEN FOR CSV ==========
def flatten_movie_record(movie_record):
    """Flatten nested dict structure for CSV export"""
    flat = {}
    flat['rank'] = movie_record.get('rank')
    
    # Metadata
    meta = movie_record.get('metadata', {})
    flat['tmdb_id'] = meta.get('tmdb_id')
    flat['imdb_id'] = meta.get('imdb_id')
    flat['title'] = meta.get('title')
    flat['release_date'] = meta.get('release_date')
    flat['release_year'] = meta.get('release_year')
    flat['genres'] = ', '.join(meta.get('genres', []))
    flat['production_countries'] = ', '.join(meta.get('production_countries', []))
    flat['language'] = meta.get('language')
    flat['studio_distributor'] = ', '.join(meta.get('studio_distributor', []))
    flat['runtime'] = meta.get('runtime')
    flat['overview'] = meta.get('overview')
    
    # Commercial
    comm = movie_record.get('commercial', {})
    flat['budget'] = comm.get('budget')
    flat['revenue'] = comm.get('revenue')
    flat['roi'] = comm.get('roi')
    
    # Ratings
    ratings = movie_record.get('ratings', {})
    flat['tmdb_vote_average'] = ratings.get('tmdb_vote_average')
    omdb = ratings.get('omdb', {})
    flat['imdb_rating'] = omdb.get('imdbRating') if isinstance(omdb, dict) else None
    flat['metascore'] = omdb.get('Metascore') if isinstance(omdb, dict) else None
    
    rotten = ratings.get('rotten_unofficial', {})
    if isinstance(rotten, dict):
        flat['rotten_tomatometer'] = rotten.get('tomatometer')
        flat['rotten_audience'] = rotten.get('audience')
    
    # Creative
    creative = movie_record.get('creative', {})
    flat['directors'] = ', '.join(creative.get('director', []))
    flat['writers'] = ', '.join(creative.get('writers', []))
    flat['cast_top'] = ', '.join(creative.get('cast_top', []))
    
    return flat

# ========== MAIN MERGE: top N ==========
def build_top_n_dataset(n=TOP_N):
    init_playwright()
    
    trending = get_tmdb_top_trending(n, period=TMDB_TRENDING_PERIOD)
    
    if not isinstance(trending, list) or len(trending) == 0:
        print(f"Error: No trending data available")
        close_playwright()
        return []
    
    output = []
    
    for idx, item in enumerate(trending):
        title = "Unknown"
        try:
            if not isinstance(item, dict):
                print(f"Skipping invalid item at index {idx}: {item}")
                continue
                
            sleep_if_needed(0.4)
            tmdb_id = item.get("id")
            title = item.get("title") or item.get("name") or "Unknown"
            
            print(f"Processing {idx+1}/{n}: {title}")
            
            metadata_basic = extract_metadata_from_tmdb_item(item)
            tmdb_details = get_tmdb_movie_details(tmdb_id)
            
            metadata = {
                "tmdb_id": tmdb_id,
                "imdb_id": tmdb_details.get("imdb_id"),
                "title": tmdb_details.get("title") or title,
                "release_date": tmdb_details.get("release_date"),
                "release_year": (tmdb_details.get("release_date") or "")[:4],
                "genres": [g.get("name") for g in tmdb_details.get("genres", [])],
                "production_countries": [c.get("iso_3166_1") for c in tmdb_details.get("production_countries", [])],
                "language": tmdb_details.get("original_language"),
                "studio_distributor": [p.get("name") for p in tmdb_details.get("production_companies", [])],
                "runtime": tmdb_details.get("runtime"),
                "overview": tmdb_details.get("overview"),
            }
            
            commercial = get_commercial_from_tmdb(tmdb_id)
            ratings = get_ratings_full(tmdb_details, metadata["title"])
            netflix_raw = get_netflix_info(metadata["title"])
            google_trends = get_google_trends_for_title(metadata["title"])
            x_search = search_x_recent(metadata["title"], max_results=5)
            youtube_trailer = get_youtube_trailer_stats(metadata["title"])
            tiktok_info = get_tiktok_with_playwright(metadata["title"])
            
            credits = get_tmdb_credits(tmdb_id)
            director = [c["name"] for c in credits.get("crew", []) if c.get("job") == "Director"] if isinstance(credits, dict) else []
            cast_top = [c["name"] for c in credits.get("cast", [])[:8]] if isinstance(credits, dict) else []
            creative = {
                "director": director,
                "writers": [c["name"] for c in credits.get("crew", []) if "Writer" in c.get("job", "")] if isinstance(credits, dict) else [],
                "cast_top": cast_top,
                "star_power": compute_star_power(cast_top)
            }
            
            awards = get_awards_placeholder(tmdb_details.get("imdb_id"))
            contextual = get_worldbank_gdp("US")
            
            movie_record = {
                "rank": idx + 1,
                "metadata": metadata,
                "commercial": commercial,
                "ratings": ratings,
                "streaming": {"netflix_search": netflix_raw},
                "social": {"google_trends": google_trends, "x_search": x_search, "youtube_trailer": youtube_trailer, "tiktok": tiktok_info},
                "creative": creative,
                "awards": awards,
                "contextual": contextual
            }
            output.append(movie_record)
            print(f"✓ Successfully processed: {title}")
            
        except Exception as e:
            print(f"✗ Error processing '{title}' at index {idx}: {str(e)}")
            output.append({"error": str(e), "title": title, "index": idx})
    
    close_playwright()
    return output

# ========== RUN ==========
if __name__ == "__main__":
    print("=" * 60)
    print("Starting Movie Trending Crawler")
    print("=" * 60)
    
    print("\nFetching top trending movies...")
    top10 = build_top_n_dataset(TOP_N)
    
    if not top10:
        print("\n⚠ No data collected. Exiting.")
        exit(1)
    
    # Print summary
    print("\n" + "=" * 60)
    print("Summary of collected movies:")
    print("=" * 60)
    for m in top10:
        if isinstance(m, dict) and 'error' not in m:
            title = m.get("metadata", {}).get("title", "Unknown")
            rank = m.get("rank", "?")
            print(f"{rank}. {title}")
        else:
            print(f"✗ Error entry: {m.get('title', 'Unknown')}")
    
    # Convert timestamps before saving JSON
    top10_converted = convert_timestamps(top10)
    
    # Save to JSON
    try:
        with open("top10_trending_full.json", "w", encoding="utf-8") as f:
            json.dump(top10_converted, f, ensure_ascii=False, indent=2)
        print("\n✓ Saved top10_trending_full.json")
    except Exception as e:
        print(f"\n✗ Error saving JSON: {str(e)}")
    
    # Save to CSV (flattened)
    csv_data = []
    for movie in top10:
        if 'error' not in movie and isinstance(movie, dict):
            csv_data.append(flatten_movie_record(movie))
    
    if csv_data:
        try:
            with open("top10_trending.csv", "w", encoding="utf-8", newline='') as f:
                writer = csv.DictWriter(f, fieldnames=csv_data[0].keys())
                writer.writeheader()
                writer.writerows(csv_data)
            print("✓ Saved top10_trending.csv")
        except Exception as e:
            print(f"✗ Error saving CSV: {str(e)}")
    else:
        print("⚠ No valid data to save to CSV")
    
    print("\n" + "=" * 60)
    print("✓ Crawler completed!")
    print("=" * 60)
