from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.common.by import By
import time

# Set up the Edge WebDriver (provide the path to your msedgedriver executable)
edge_driver_path = 'C:\Users\theis\Downloads\edgedriver_win32'  # Update this path

driver = webdriver.Edge()

# Go to the webpage you want to scrape
url = 'https://peoples.church/indianapolis/'  # Replace with your target URL
driver.get(url)

# Wait for the page to fully load
time.sleep(5)

# Find all <a> tags on the page
all_links = driver.find_elements(By.TAG_NAME, 'a')

# Filter out YouTube video links
youtube_links = [link.get_attribute('href') for link in all_links if link.get_attribute('href') and 'youtube.com/watch' in link.get_attribute('href')]

# Print the found YouTube links
for yt_link in youtube_links:
    print(yt_link)

# Close the WebDriver
driver.quit()
