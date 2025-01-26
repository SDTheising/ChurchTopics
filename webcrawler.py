import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import time
import os

# Function to read URLs from the CSV
def get_urls_from_csv(file_path):
    df = pd.read_csv(file_path)
    return df['Website'].dropna().tolist()

# Function to parse robots.txt and extract disallowed paths and sitemaps
def parse_robots_txt(base_url):
    robots_url = f"{base_url}/robots.txt"
    try:
        response = requests.get(robots_url, timeout=10)
        if response.status_code != 200:
            print(f"No robots.txt found at {robots_url}.")
            return [], []
        
        disallowed_paths = []
        sitemap_urls = []
        for line in response.text.splitlines():
            if line.startswith("Disallow:"):
                disallowed_path = line.split(":")[1].strip()
                disallowed_paths.append(disallowed_path)
            elif line.startswith("Sitemap:"):
                sitemap_urls.append(line.split(": ", 1)[1].strip())
        
        return disallowed_paths, sitemap_urls
    except Exception as e:
        print(f"Error fetching robots.txt from {base_url}: {e}")
        return [], []

# Function to find common sitemap URLs for WordPress and fallback
def find_common_sitemaps(base_url):
    common_sitemaps = [
        f"{base_url}/sitemap.xml",
        f"{base_url}/wp-sitemap.xml"
    ]
    for sitemap_url in common_sitemaps:
        try:
            response = requests.get(sitemap_url, timeout=10)
            if response.status_code == 200:
                print(f"Found sitemap at {sitemap_url}")
                return sitemap_url
        except Exception as e:
            print(f"Error accessing {sitemap_url}: {e}")
    print(f"No sitemaps found for {base_url}")
    return None

# Function to crawl sitemap and extract all URLs
def crawl_sitemap(sitemap_url):
    try:
        response = requests.get(sitemap_url, timeout=10)
        if response.status_code != 200:
            print(f"Failed to fetch sitemap at {sitemap_url}")
            return []

        soup = BeautifulSoup(response.content, "xml")
        urls = [loc.text for loc in soup.find_all("loc")]
        print(f"Found {len(urls)} URLs in sitemap.")
        return urls
    except Exception as e:
        print(f"Error fetching sitemap: {e}")
        return []

# Recursive crawling function
def recursive_crawl(base_url, disallowed_paths, driver, visited=None, depth=3):
    if visited is None:
        visited = set()

    # Stop crawling if maximum depth is reached
    if depth == 0:
        return {}

    youtube_links = {}
    try:
        # Skip URLs that are disallowed or already visited
        if any(base_url.startswith(d) for d in disallowed_paths):
            print(f"Skipping disallowed path: {base_url}")
            return youtube_links
        if base_url in visited:
            return youtube_links

        print(f"Visiting: {base_url}")
        visited.add(base_url)

        # Visit the URL
        driver.get(base_url)
        time.sleep(3)  # Allow the page to load

        # Extract YouTube links on the page
        links = driver.find_elements(By.TAG_NAME, 'a')
        youtube_links[base_url] = [link.get_attribute('href') for link in links
                                   if link.get_attribute('href') and 'youtube.com' in link.get_attribute('href')]

        # Extract all unique internal links
        internal_links = set()
        for link in links:
            href = link.get_attribute('href')
            if href and href.startswith(base_url):  # Only follow internal links
                internal_links.add(href)

        # Recursively crawl internal links
        for link in internal_links:
            youtube_links.update(recursive_crawl(link, disallowed_paths, driver, visited, depth - 1))
    
    except Exception as e:
        print(f"Error processing {base_url}: {e}")
    
    return youtube_links

# Main function
def main():
    # Path to the CSV file
    csv_path = "churchesWithWebsites.csv"  # Adjust the path as needed
    edge_driver_path = os.getenv('EDGE_DRIVER_PATH')  # Path to msedgedriver executable
    service = Service(executable_path=edge_driver_path)
    driver = webdriver.Edge(service=service)

    # Get the list of base URLs
    base_urls = get_urls_from_csv(csv_path)

    all_youtube_links = {}

    try:
        for base_url in base_urls:
            # Parse robots.txt
            disallowed_paths, sitemap_urls = parse_robots_txt(base_url)

            # Check common sitemap locations if none found in robots.txt
            if not sitemap_urls:
                sitemap_url = find_common_sitemaps(base_url)
                if sitemap_url:
                    sitemap_urls.append(sitemap_url)

            # Crawl sitemap URLs or recursively crawl homepage if no sitemap
            urls_to_crawl = []
            if sitemap_urls:
                for sitemap_url in sitemap_urls:
                    urls_to_crawl.extend(crawl_sitemap(sitemap_url))
            else:
                print(f"No sitemap available for {base_url}, falling back to homepage.")
                urls_to_crawl.append(base_url)

            # Perform recursive crawling
            for url in urls_to_crawl:
                youtube_links = recursive_crawl(url, disallowed_paths, driver, depth=3)
                all_youtube_links.update(youtube_links)
    finally:
        driver.quit()

    # Save the results to a CSV
    output_df = pd.DataFrame([
        {"Website": website, "YouTube Links": ", ".join(links)}
        for website, links in all_youtube_links.items()
    ])
    output_df.to_csv("youtube_links_output.csv", index=False)

    print("Crawling complete. Results saved to youtube_links_output.csv.")

if __name__ == "__main__":
    main()
