from youtube_transcript_api import YouTubeTranscriptApi as yta
import csv
import time
import os

# Read video IDs from the input CSV
input_file = 'video_ids.csv'  # Name of the input CSV file containing video IDs
output_file = 'transcripts.csv'  # Name of the output CSV file
video_ids = []

with open(input_file, 'r', encoding='utf-8') as csvfile:
    reader = csv.reader(csvfile)
    for row in reader:
        if row:  # Check if the row is not empty
            video_ids.append(row[0])  # Assuming the ID is in the first column

# Check if the output file already exists
if os.path.exists(output_file):
    # Open the output CSV in append mode
    mode = 'a'  # Append mode if file exists
    file_exists = True
else:
    # Create the file if it doesn't exist
    mode = 'w'  # Write mode if file doesn't exist
    file_exists = False

# Using a single variable to hold the text for CSV
csv_content = []

# Iterate over each video ID to get transcripts
for video_id in video_ids:
    try:
        print(f"Retrieving transcript for video ID: {video_id}")
        # Get transcript for the video ID
        data = yta.get_transcript(video_id)
        
        # Concatenate all text into a single string for this video
        transcript_text = ' '.join(item['text'] for item in data)
        
        # Remove any line breaks within the transcript to avoid issues in CSV
        transcript_text = transcript_text.replace('\n', ' ').replace('\r', ' ')
        
        # Append the video ID and cleaned transcript as a new row (ID in column 1, transcript in column 2)
        csv_content.append([video_id, transcript_text]) 
        
        # Optional: Add a delay to avoid rate-limiting issues
        time.sleep(2)  # Sleep for 2 seconds between requests
        
    except Exception as e:
        print(f"Error retrieving transcript for {video_id}: {e}")
        # Append the error message with video ID
        csv_content.append([video_id, f"Error: {e} for ID {video_id}"])

# Write to the output CSV
with open(output_file, mode, newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    
    # If the file doesn't exist, write headers (Video ID and Transcript)
    if not file_exists:
        writer.writerow(["Video ID", "Transcript"])  # Write headers
        
    # Write each video ID and transcript (append mode will start from the first empty row)
    writer.writerows(csv_content)

print(f"Transcripts have been appended to {output_file}")
