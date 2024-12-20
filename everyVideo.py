import time
import csv
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class EveryVideo:
    @staticmethod
    def GetIds(channel_url):
        # Set up Edge WebDriver
        edge_driver_path = r'C:\Users\theis\Downloads\edgedriver_win32\msedgedriver.exe'  # Update this path
        service = Service(executable_path=edge_driver_path)
        driver = webdriver.Edge(service=service)

        # Open YouTube channel videos page
        driver.get(channel_url)

        # Wait for the page to load and for the video links to appear
        wait = WebDriverWait(driver, 10)  # Explicit wait, up to 10 seconds
        wait.until(EC.presence_of_element_located((By.XPATH, '//a[contains(@href, "/watch")]')))  # Wait for video links

        # Scroll down to load more videos (if necessary)
        scroll_pause_time = 2
        last_height = driver.execute_script("return document.documentElement.scrollHeight")

        while True:
            # Scroll to the bottom of the page
            driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
            
            # Wait for new videos to load
            time.sleep(scroll_pause_time)
            
            # Check if we've reached the bottom of the page
            new_height = driver.execute_script("return document.documentElement.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

        # Find all video elements by XPath and extract URLs
        video_elements = driver.find_elements(By.XPATH, '//a[contains(@href, "/watch")]')

        # Use a set to store unique video IDs
        video_ids = set()
        for element in video_elements:
            video_id = element.get_attribute('href').split('=')[-1]
            video_ids.add(video_id)  # Add video ID to the set

        # Write unique video IDs to a CSV file
        with open('ID_HoldingCell.csv', mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            for video_id in video_ids:
                writer.writerow([video_id])  # Write each unique video ID on a new line

        # Confirm completion
        print(f"Extracted {len(video_ids)} unique video IDs and saved to ID_HoldingCell.csv")

        # Close the WebDriver
        driver.quit()
