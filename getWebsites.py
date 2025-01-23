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

# Input CSV containing the URLs to scrape
input_file = 'churches.csv'  # CSV with URLs scraped earlier
output_file = 'detailed_churches.csv'  # CSV for detailed information

# Open the output CSV and prepare to write
with open(output_file, mode='w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["Church Name", "URL", "Website", "Denomination", "Language", "Size"])  # Headers for detailed data

    # Read URLs from the input CSV
    with open(input_file, mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.reader(infile)
        next(reader)  # Skip the header row

        for row in reader:
            church_name, url = row[0], row[1]

            try:
                # Navigate to the URL
                driver.get(url)

                # Wait for the page to load
                time.sleep(1)

                # Scrape the website
                try:
                    website_element = driver.find_element(By.XPATH, '//div[text()="Website"]/following-sibling::div/a')
                    website = website_element.get_attribute('href')
                except:
                    website = "N/A"

                # Scrape the denomination
                try:
                    denomination_element = driver.find_element(By.XPATH, '//div[text()="Denomination"]/following-sibling::div')
                    denomination = denomination_element.get_attribute('innerHTML').replace('<br>', ' ').strip()
                except:
                    denomination = "N/A"

                # Scrape the language
                try:
                    language_elements = driver.find_elements(By.XPATH, '//div[text()="Language"]/following-sibling::div//li')
                    language = ", ".join([element.text for element in language_elements])
                except:
                    language = "N/A"

                # Scrape the size
                try:
                    size_element = driver.find_element(By.XPATH, '//div[text()="Size"]/following-sibling::div')
                    size = size_element.text.strip()
                except:
                    size = "N/A"

                # Write the details to the output CSV
                writer.writerow([church_name, url, website, denomination, language, size])
                print(f"Scraped details for: {church_name}")

            except Exception as e:
                print(f"Error processing {url}: {e}")
                writer.writerow([church_name, url, "Error", "Error", "Error", "Error"])

# Close the WebDriver
driver.quit()
