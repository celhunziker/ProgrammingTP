"""
=============================================================================
PART 1: Web Scraping - Filmaffinity.com
=============================================================================
Programming for Data Science - Final Project
Universidad Politécnica de Madrid

Description:
    Scrapes top-rated movies from Filmaffinity.com by genre using
    dedicated ranking pages. Each genre has a unique server-side URL
    (ranking.php?rn=ranking_GENRE), so this is STATIC web scraping
    using the 'requests' and 'BeautifulSoup' libraries (no JS needed).

    Approach (based on class material - Web Scraping slides):
      1. Request information from the web server  (requests.get)
      2. Handle the server's response and extract data  (BeautifulSoup)
      3. Automatic interaction: iterate over genres, parse each page
      4. Store structured data in CSV files  (pandas.DataFrame.to_csv)

Resources (one CSV per genre = 5 resources):
    - Drama, Comedy, Thriller, Sci-Fi, Horror

Dependencies:
    pip install requests beautifulsoup4 pandas

Usage:
    python scraper.py
=============================================================================
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
import os


# =============================================================================
# CONFIGURATION
# =============================================================================

BASE_URL = "https://www.filmaffinity.com"
LANG_PATH = "us"

GENRES = {
    "drama":    "ranking_drama",
    "comedy":   "ranking_comedy",
    "thriller": "ranking_thriller",
    "sci_fi":   "ranking_scifi",
    "horror":   "ranking_horror",
}

MAX_MOVIES_PER_GENRE = 30
OUTPUT_DIR = "data"
REQUEST_DELAY = 3

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def clean_text(text):
    """Remove extra whitespace from a string."""
    if text is None:
        return ""
    return re.sub(r'\s+', ' ', text).strip()


def parse_rating(rating_str):
    """Parse a rating string like '9.0' or '7,5' into a float (0-10)."""
    if not rating_str:
        return None
    rating_str = rating_str.strip().replace(',', '.')
    try:
        val = float(rating_str)
        return val if 0 <= val <= 10 else None
    except ValueError:
        return None


def parse_votes(votes_str):
    """
    Parse a vote count string into an integer.
    Handles '178,331', '818', and text with trailing non-digit content
    (e.g. from <i> icon elements inside the same div).
    """
    if not votes_str:
        return 0
    digits = re.sub(r'[^\d]', '', votes_str.replace(',', ''))
    return int(digits) if digits else 0


# =============================================================================
# MOVIE EXTRACTION
# =============================================================================

def extract_movie(item, rank, genre_name):
    """
    Extract all fields from one movie <li> element.

    Selectors match the actual Filmaffinity ranking page HTML (2025):
      - .mc-title a            -> title + URL
      - .mc-year               -> year
      - img.nflag              -> country (alt attribute)
      - .mc-director a         -> director name
      - .mc-cast a             -> cast members
      - .fa-avg-rat-box .avg   -> rating
      - .fa-avg-rat-box .count -> votes
    """
    try:
        # ── TITLE + URL ─────────────────────────────────────────────
        title = ""
        movie_url = ""
        # .mc-title has two <a> tags (desktop + mobile), same URL
        for a in item.select('.mc-title a[href*="/film"]'):
            text = a.get_text(strip=True)
            if text:  # len > 0 to include single-char titles like "M"
                title = clean_text(text)
                href = a.get('href', '')
                movie_url = (href if href.startswith('http')
                             else BASE_URL + href)
                break

        if not title:
            return None

        # ── YEAR ─────────────────────────────────────────────────────
        year = None
        ye = item.select_one('.mc-year')
        if ye:
            m = re.search(r'(\d{4})', ye.get_text())
            if m:
                year = int(m.group(1))

        # ── COUNTRY ──────────────────────────────────────────────────
        country = ""
        flag = item.select_one('img.nflag')
        if flag:
            country = flag.get('alt', '').strip()

        # ── DIRECTOR ─────────────────────────────────────────────────
        director = ""
        dir_elem = item.select_one('.mc-director a')
        if dir_elem:
            director = clean_text(dir_elem.get_text())

        # ── CAST ─────────────────────────────────────────────────────
        cast = ""
        cast_elems = item.select('.mc-cast a')
        if cast_elems:
            cast = ", ".join([clean_text(a.get_text()) for a in cast_elems])

        # ── RATING ───────────────────────────────────────────────────
        # <div class="avg">8.6</div> inside .fa-avg-rat-box
        rating = None
        avg_elem = item.select_one('.fa-avg-rat-box .avg')
        if avg_elem:
            rating = parse_rating(avg_elem.get_text())

        # ── VOTES ────────────────────────────────────────────────────
        # <div class="count">206,406 <i class="fa ..."></i></div>
        votes = 0
        count_elem = item.select_one('.fa-avg-rat-box .count')
        if count_elem:
            votes = parse_votes(count_elem.get_text())

        return {
            "rank": rank, "title": title, "year": year,
            "director": director, "cast": cast, "country": country,
            "rating": rating, "votes": votes,
            "genre": genre_name, "url": movie_url,
        }

    except Exception as e:
        print(f"    WARNING at rank #{rank}: {e}")
        return None


# =============================================================================
# GENRE PAGE SCRAPING
# =============================================================================

def scrape_genre(session, genre_name, ranking_id):
    """
    Scrape the ranking page for one genre.
    URL: https://www.filmaffinity.com/us/ranking.php?rn=ranking_GENRE
    """
    url = f"{BASE_URL}/{LANG_PATH}/ranking.php?rn={ranking_id}"
    print(f"\n{'─' * 60}")
    print(f"  Genre: {genre_name.upper()}  |  URL: {url}")
    print(f"{'─' * 60}")

    movies = []

    try:
        response = session.get(url, timeout=20)
        response.raise_for_status()
        print(f"  Status: {response.status_code} | "
              f"HTML size: {len(response.text)} chars")

        soup = BeautifulSoup(response.text, 'html.parser')

        # The ranking list lives in <ul id="top-movies">
        container = soup.select_one('#top-movies')
        if container:
            items = container.find_all('li', recursive=False)
        else:
            # Fallback: any <li> containing a movie-card div
            items = [li for li in soup.find_all('li')
                     if li.select_one('[data-movie-id]')]

        print(f"  Found {len(items)} movie entries on page")
        items = items[:MAX_MOVIES_PER_GENRE]

        for rank, item in enumerate(items, 1):
            movie = extract_movie(item, rank, genre_name)
            if movie:
                movies.append(movie)
                if rank <= 3:
                    print(f"    #{rank} {movie['title']} ({movie['year']}) "
                          f"R:{movie['rating']}  V:{movie['votes']}")

        print(f"  ✓ Extracted {len(movies)} movies successfully")

    except requests.exceptions.HTTPError as e:
        print(f"  HTTP ERROR: {e}")
    except requests.exceptions.ConnectionError as e:
        print(f"  CONNECTION ERROR: {e}")
    except requests.exceptions.Timeout:
        print(f"  TIMEOUT: Request took longer than 20 seconds")
    except Exception as e:
        print(f"  ERROR: {e}")

    return movies


# =============================================================================
# SAVE TO CSV
# =============================================================================

def save_csv(movies, genre_name):
    """Save list of movies to CSV. One file per genre (= one resource)."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, f"filmaffinity_{genre_name}.csv")
    if movies:
        pd.DataFrame(movies).to_csv(path, index=False, encoding='utf-8-sig')
        print(f"  SAVED: {path} ({len(movies)} movies)")
    else:
        print(f"  WARNING: No data extracted for {genre_name}")
    return path


# =============================================================================
# DATA QUALITY CHECK
# =============================================================================

def data_quality_report(genres):
    """Print a summary and flag any data issues."""
    print("\n  DATA QUALITY CHECK:")
    for g in genres:
        path = os.path.join(OUTPUT_DIR, f"filmaffinity_{g}.csv")
        if os.path.exists(path):
            df = pd.read_csv(path)
            print(f"\n  [{g.upper()}] - {len(df)} movies")
            for i in range(min(3, len(df))):
                row = df.iloc[i]
                print(f"    #{int(row['rank'])} \"{row['title']}\" "
                      f"({int(row['year'])}) "
                      f"Dir: {row['director']} | "
                      f"Rating: {row['rating']}  "
                      f"Votes: {int(row['votes'])}")
            empty_r = df['rating'].isna().sum()
            zero_v = (df['votes'] == 0).sum()
            if empty_r > 0 or zero_v > 0:
                print(f"    ⚠  {empty_r} missing ratings, {zero_v} zero votes")
            else:
                print(f"    ✓  All {len(df)} movies have complete data")


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("=" * 60)
    print("  FILMAFFINITY SCRAPER - Ranking Pages")
    print("  Programming for Data Science - Final Project")
    print("=" * 60)

    session = requests.Session()
    session.headers.update(HEADERS)

    print("\n  Loading cookies from main page...")
    try:
        session.get(f"{BASE_URL}/{LANG_PATH}/main.html", timeout=15)
        print("  ✓ Session ready (cookies loaded)")
    except Exception:
        print("  ⚠ Could not load main page (continuing without cookies)")

    time.sleep(1)

    all_csv_paths = []
    total_movies = 0

    for genre_name, ranking_id in GENRES.items():
        movies = scrape_genre(session, genre_name, ranking_id)
        path = save_csv(movies, genre_name)
        all_csv_paths.append(path)
        total_movies += len(movies)
        time.sleep(REQUEST_DELAY)

    print("\n" + "=" * 60)
    print("  SUMMARY")
    print("=" * 60)
    print(f"  Total movies scraped: {total_movies}")
    print(f"  Genres scraped: {len(GENRES)}")
    print(f"  CSV files generated:")
    for f in all_csv_paths:
        print(f"    - {f}")

    data_quality_report(GENRES)

    print("\n" + "=" * 60)
    print("  SCRAPING COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()