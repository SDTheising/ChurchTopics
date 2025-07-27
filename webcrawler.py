from __future__ import annotations

import os
import time
import csv
from urllib.parse import urlparse, urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup, FeatureNotFound

from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# ─────────────────────────  CONFIG  ───────────────────────── #
CSV_PATH       = "detailed_churches.csv"              
EDGE_DRIVER    = os.getenv("EDGE_DRIVER_PATH")          
HEADLESS       = True                                    
REQUEST_DELAY  = 1.0                                    
# ──────────────────────────────────────────────────────────── #


# ───────────── CSV loader ─────────────
def get_base_urls(path: str) -> list[str]:
    df = pd.read_csv(path)
    for col in ("Website", "URL"):
        if col in df.columns:
            urls = (
                df[col]
                .dropna()
                .astype(str)
                .str.strip()
                .apply(lambda u: u if u.startswith(("http://", "https://"))
                                else f"https://{u}")
            )
            return urls.tolist()
    raise ValueError("CSV must contain a 'Website' or 'URL' column")


# ───── robots.txt ─────
def parse_robots_txt(base_url: str) -> tuple[list[str], list[str]]:
    robots_url = urljoin(base_url.rstrip("/") + "/", "robots.txt")
    try:
        resp = requests.get(robots_url, timeout=10)
        if resp.status_code != 200:
            print(f"[robots] none @ {robots_url} ({resp.status_code})")
            return [], []

        disallowed, sitemaps = [], []
        for raw in resp.text.splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.lower().startswith("disallow:"):
                path = line.split(":", 1)[1].strip()
                if path:                       # ← ignore blank Disallow
                    disallowed.append(path)
            elif line.lower().startswith("sitemap:"):
                sitemaps.append(line.split(":", 1)[1].strip())
        return disallowed, sitemaps

    except Exception as e:
        print(f"[robots] error {robots_url} – {e}")
        return [], []


def find_common_sitemaps(base_url: str) -> list[str]:
    urls = []
    for route in ("/sitemap.xml", "/wp-sitemap.xml"):
        url = urljoin(base_url.rstrip("/") + "/", route.lstrip("/"))
        try:
            r = requests.head(url, timeout=10, allow_redirects=True)
            if r.status_code == 200:
                urls.append(url)
        except Exception:
            pass
    return urls


# ───── sitemap crawl ─────
def crawl_sitemap(url: str) -> list[str]:
    try:
        r = requests.get(url, timeout=10)
        if r.status_code != 200:
            print(f"[sitemap] {url} – HTTP {r.status_code}")
            return []
        try:
            soup = BeautifulSoup(r.content, "xml")
        except FeatureNotFound:
            soup = BeautifulSoup(r.content, "html.parser")
        return [loc.text.strip() for loc in soup.find_all("loc")]
    except Exception as e:
        print(f"[sitemap] error {url} – {e}")
        return []


# ───── helpers ─────
def path_is_disallowed(full_url: str, paths: list[str]) -> bool:
    p = urlparse(full_url).path or "/"
    return any(p.startswith(d) for d in paths)


def fast_head_ok(url: str) -> bool:
    try:
        return requests.head(url, timeout=5, allow_redirects=True).status_code < 400
    except Exception:
        return False


# ───── core YouTube scraper ─────
def collect_youtube_links(
    urls: list[str],
    disallowed: list[str],
    driver_path: str,
    *,
    headless: bool = True,
) -> dict[str, list[str]]:
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")

    service = Service(driver_path)
    found: dict[str, list[str]] = {}

    with webdriver.Edge(service=service, options=opts) as driver:
        for url in urls:
            if path_is_disallowed(url, disallowed):
                continue
            if not fast_head_ok(url):
                continue

            print(f"[visit] {url}")
            try:
                driver.get(url)
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "a"))
                )
                links = [
                    a.get_attribute("href")
                    for a in driver.find_elements(By.TAG_NAME, "a")
                    if (href := a.get_attribute("href")) and "youtube.com" in href
                ]
                if links:
                    found[url] = links
                    break  # ← first hit: stop crawling this domain
            except Exception as e:
                print(f"[error] {url} – {e}")

    return found


# ───── main ─────
def main():
    if not EDGE_DRIVER or not os.path.exists(EDGE_DRIVER):
        raise RuntimeError("EDGE_DRIVER_PATH env var not set or invalid")

    base_urls = get_base_urls(CSV_PATH)
    all_hits: dict[str, list[str]] = {}

    for base in base_urls:
        disallowed, sitemaps = parse_robots_txt(base)
        if not sitemaps:
            sitemaps.extend(find_common_sitemaps(base))

        pages: list[str] = []
        if sitemaps:
            for sm in sitemaps:
                pages.extend(crawl_sitemap(sm))
        else:
            pages.append(base.rstrip("/"))

        yt = collect_youtube_links(pages, disallowed, EDGE_DRIVER, headless=HEADLESS)
        all_hits.update(yt)

        # incremental save
        pd.DataFrame(
            [{"Website": k, "YouTube Links": ", ".join(v)} for k, v in all_hits.items()]
        ).to_csv("youtube_links_output.csv", index=False)

        time.sleep(REQUEST_DELAY)

    print("✓ Done – results in youtube_links_output.csv")


if __name__ == "__main__":
    main()