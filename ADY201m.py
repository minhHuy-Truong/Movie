dua theo file nay tao cho toi file requirement.txt : 
import os
import time
import requests
from pytrends.request import TrendReq
from bs4 import BeautifulSoup
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

NETFLIX_HOST = "netflix54.p.rapidapi.com"
TIKTOK_HOST = "tiktok-all-in-one.p.rapidapi.com"

TOP_N = 10
TMDB_TRENDING_PERIOD = "week"
PYTRENDS_TIMEFRAME = "today 12-m"

# ========== HELPERS ==========
def safe_json(resp):
    try:
        return resp.json()
    except Exception:
        return {"error": "invalid_json", "status_code": getattr(resp, "status_code", None), "text": getattr(resp, "text", "")[:400]}

def sleep_if_needed(seconds=0.5):
    time.sleep(seconds)

def convert_timestamps(obj):
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {k: convert_timestamps(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_timestamps(i) for i in obj]
    return obj

# ========== TMDb ==========
def get_tmdb_top_trending(n=10, period="week"):
    url = f"https://api.themoviedb.org/3/trending/movie/{period}"
    params = {"api_key": TMDB_API_KEY, "language": "en-US"}
    r = requests.get(url, params=params, timeout=12)
    data = safe_json(r)
    results = data.get("results", []) if isinstance(data, dict) else []
    return results[:n]

def extract_metadata_from_tmdb_item(tmdb_item):
    return {
        "tmdb_id": tmdb_item.get("id"),
        "title": tmdb_item.get("title") or tmdb_item.get("name"),
        "release_date": tmdb_item.get("release_date"),
        "original_language": tmdb_item.get("original_language"),
        "genre_ids": tmdb_item.get("genre_ids"),
        "popularity": tmdb_item.get("popularity"),
        "overview": tmdb_item.get("overview")
    }

def get_tmdb_movie_details(tmdb_id):
    url = f"https://api.themoviedb.org/3/movie/{tmdb_id}"
    params = {"api_key": TMDB_API_KEY, "language": "en-US"}
    r = requests.get(url, params=params, timeout=12)
    return safe_json(r)

# ========== Commercial ==========
def get_commercial_from_tmdb(tmdb_id):
    url = f"https://api.themoviedb.org/3/movie/{tmdb_id}"
    params = {"api_key": TMDB_API_KEY}
    r = requests.get(url, params=params, timeout=12)
    data = safe_json(r)
    budget = data.get("budget")
    revenue = data.get("revenue")
    roi = None
    try:
        if budget and revenue and budget > 0:
            roi = round(revenue / budget, 3)
    except Exception:
        roi = None
    return {"budget": budget, "revenue": revenue, "roi": roi, "raw": data}

# ========== Ratings ==========
def get_omdb_by_imdb(imdb_id):
    if not OMDB_API_KEY:
        return {"error": "missing_omdb_key"}
    url = "http://www.omdbapi.com/"
    params = {"i": imdb_id, "apikey": OMDB_API_KEY}
    try:
        r = requests.get(url, params=params, timeout=10)
        data = safe_json(r)
        if not data or "Error" in data:
            # Retry once if API rate limited or failed
            time.sleep(1)
            r2 = requests.get(url, params=params, timeout=10)
            data = safe_json(r2)
        return data
    except Exception as e:
        return {"error": str(e), "imdb_id": imdb_id}

def get_ratings_full(tmdb_details, movie_title):
    out = {"tmdb_vote_average": tmdb_details.get("vote_average")}
    imdb_id = tmdb_details.get("imdb_id")
    if imdb_id:
        omdb_data = get_omdb_by_imdb(imdb_id)
        out["omdb"] = omdb_data
    else:
        out["omdb"] = {"message": "no_imdb_id_in_tmdb"}

    # Rotten Tomatoes unofficial
    try:
        search_url = f"https://www.rottentomatoes.com/search?search={requests.utils.quote(movie_title)}"
        r = requests.get(search_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")
        first = soup.select_one("search-page-media-row a")
        if first:
            movie_url = "https://www.rottentomatoes.com" + first["href"]
            r2 = requests.get(movie_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
            s2 = BeautifulSoup(r2.text, "html.parser")
            sb = s2.select_one("score-board")
            if sb:
                out["rotten_unofficial"] = {
                    "url": movie_url,
                    "tomatometer": sb.get("tomatometerscore"),
                    "audience": sb.get("audiencescore")
                }
            else:
                out["rotten_unofficial"] = {"message": "no_scoreboard"}
        else:
            out["rotten_unofficial"] = {"message": "no_search_result"}
    except Exception as e:
        out["rotten_unofficial_error"] = str(e)

    return out

# ========== Netflix, Trends, X, YouTube, TikTok ==========
def get_netflix_info(title):
    if not RAPIDAPI_KEY:
        return {"error": "missing_rapidapi_key"}
    url = "https://netflix54.p.rapidapi.com/search/"
    params = {"query": title, "offset": "0", "limit_titles": "3", "lang": "en"}
    headers = {"X-RapidAPI-Key": RAPIDAPI_KEY, "X-RapidAPI-Host": NETFLIX_HOST}
    r = requests.get(url, headers=headers, params=params, timeout=12)
    return safe_json(r)

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
    headers = {"Authorization": f"Bearer {X_API_KEY}"}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=10)
        return safe_json(r)
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

def get_tiktok_via_rapidapi(title, count=3):
    if not RAPIDAPI_KEY:
        return {"error": "missing_rapidapi_key"}
    hashtag = title.lower().replace(" ", "")
    url = "https://tiktok-all-in-one.p.rapidapi.com/hashtag/posts"
    headers = {"X-RapidAPI-Key": RAPIDAPI_KEY, "X-RapidAPI-Host": TIKTOK_HOST}
    params = {"name": hashtag, "count": str(count)}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=12)
        return safe_json(r)
    except Exception as e:
        return {"error": str(e)}

# ========== Creative & Others ==========
def get_tmdb_credits(tmdb_id):
    url = f"https://api.themoviedb.org/3/movie/{tmdb_id}/credits"
    params = {"api_key": TMDB_API_KEY}
    r = requests.get(url, params=params, timeout=12)
    return safe_json(r)

def compute_star_power(cast_list):
    return {"actor_count": len(cast_list)}

def get_awards_placeholder(imdb_id):
    return {"Oscars": None, "GoldenGlobes": None, "FestivalSelections": None}

def get_worldbank_gdp(country_code="US"):
    url = f"http://api.worldbank.org/v2/country/{country_code}/indicator/NY.GDP.MKTP.CD?format=json"
    r = requests.get(url, timeout=12)
    return safe_json(r)

# ========== Flatten ==========
def flatten_movie_record(movie_record):
    flat = {}
    flat["rank"] = movie_record.get("rank")

    meta = movie_record.get("metadata", {})
    flat["tmdb_id"] = meta.get("tmdb_id")
    flat["imdb_id"] = meta.get("imdb_id")
    flat["title"] = meta.get("title")
    flat["release_date"] = meta.get("release_date")
    flat["release_year"] = meta.get("release_year")
    flat["genres"] = ", ".join(meta.get("genres", []))
    flat["production_countries"] = ", ".join(meta.get("production_countries", []))
    flat["language"] = meta.get("language")
    flat["runtime"] = meta.get("runtime")

    comm = movie_record.get("commercial", {})
    flat["budget"] = comm.get("budget")
    flat["revenue"] = comm.get("revenue")
    flat["roi"] = comm.get("roi")

    ratings = movie_record.get("ratings", {})
    flat["tmdb_vote_average"] = ratings.get("tmdb_vote_average")
    omdb = ratings.get("omdb", {})
    flat["imdb_rating"] = omdb.get("imdbRating")
    flat["metascore"] = omdb.get("Metascore")

    rotten = ratings.get("rotten_unofficial", {})
    if isinstance(rotten, dict):
        flat["rotten_tomatometer"] = rotten.get("tomatometer")
        flat["rotten_audience"] = rotten.get("audience")

    creative = movie_record.get("creative", {})
    flat["directors"] = ", ".join(creative.get("director", []))
    flat["writers"] = ", ".join(creative.get("writers", []))
    flat["cast_top"] = ", ".join(creative.get("cast_top", []))

    return flat

# ========== Build Dataset ==========
def build_top_n_dataset(n=TOP_N):
    trending = get_tmdb_top_trending(n, period=TMDB_TRENDING_PERIOD)
    output = []
    for idx, item in enumerate(trending):
        try:
            sleep_if_needed(0.5)
            tmdb_id = item.get("id")
            title = item.get("title") or item.get("name")

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
                "runtime": tmdb_details.get("runtime"),
                "overview": tmdb_details.get("overview"),
            }

            commercial = get_commercial_from_tmdb(tmdb_id)
            ratings = get_ratings_full(tmdb_details, metadata["title"])
            netflix_raw = get_netflix_info(metadata["title"])
            google_trends = get_google_trends_for_title(metadata["title"])
            x_search = search_x_recent(metadata["title"], max_results=5)
            youtube_trailer = get_youtube_trailer_stats(metadata["title"])
            tiktok_info = get_tiktok_via_rapidapi(metadata["title"], count=3)

            credits = get_tmdb_credits(tmdb_id)
            director = [c["name"] for c in credits.get("crew", []) if c.get("job") == "Director"]
            cast_top = [c["name"] for c in credits.get("cast", [])[:8]]
            creative = {
                "director": director,
                "writers": [c["name"] for c in credits.get("crew", []) if "Writer" in c.get("job", "")],
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
                "social": {
                    "google_trends": google_trends,
                    "x_search": x_search,
                    "youtube_trailer": youtube_trailer,
                    "tiktok": tiktok_info
                },
                "creative": creative,
                "awards": awards,
                "contextual": contextual
            }
            output.append(movie_record)
        except Exception as e:
            output.append({"error": str(e), "item": item})
    return output

# ========== RUN ==========
if __name__ == "__main__":
    print("Fetching top trending movies...")
    top10 = build_top_n_dataset(TOP_N)

    for m in top10:
        title = m.get("metadata", {}).get("title")
        print(f"{m.get('rank')}. {title}")

    top10_converted = convert_timestamps(top10)
    with open("top10_trending_full.json", "w", encoding="utf-8") as f:
        json.dump(top10_converted, f, ensure_ascii=False, indent=2)
    print("✓ Saved top10_trending_full.json")

    csv_data = []
    for movie in top10:
        if "error" not in movie:
            csv_data.append(flatten_movie_record(movie))

    if csv_data:
        with open("top10_trending.csv", "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=csv_data[0].keys())
            writer.writeheader()
            writer.writerows(csv_data)
        print("✓ Saved top10_trending.csv")
    else:
        print("⚠ No valid data to save to CSV")

    print("✓ Done!")
