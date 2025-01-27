import os
import time
import csv
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.common.by import By
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

# Set up the Edge WebDriver (provide the path to your msedgedriver executable)
edge_driver_path = os.getenv('EDGE_DRIVER_PATH')  # Path to msedgedriver executable
service = Service(executable_path=edge_driver_path)
driver = webdriver.Edge(service=service)

# Base URL and tag to navigate through pages
base_url = os.getenv('BASE_URL')  # Base URL from .env file
empty_tag_selector = os.getenv('EMPTY_TAG_SELECTOR', 'p.empty')  # Default to 'p.empty' if not set
tag_selector = os.getenv('TAG_SELECTOR')

# Start from the first page
page_number = 1

# CSV file setup
output_file = 'churches.csv'
with open(output_file, mode='w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["Church Name", "URL"])  # Write headers

    while True:
        # Construct the URL for the current page
        url = f"{base_url}{page_number}"
        driver.get(url)

        # Wait for the page to fully load
        time.sleep(1)

        # Check if the page displays the "No results" message
        try:
            no_results_element = driver.find_element(By.CSS_SELECTOR, empty_tag_selector)
            print("No more results found. Stopping navigation.")
            break  # Exit the loop if no results are found
        except:
            # If the element is not found, continue scraping
            pass

        # Find all <a> tags on the page
        all_links = driver.find_elements(By.TAG_NAME, 'a')

        # Extract relevant links and church names
        for link in all_links:
            href = link.get_attribute('href')
            if href and tag_selector in href:
                church_name = link.text.strip()
                writer.writerow([church_name, href])  # Write church name and URL to CSV

        # Increment the page number to navigate to the next page
        page_number += 1

# Close the WebDriver
driver.quit()
