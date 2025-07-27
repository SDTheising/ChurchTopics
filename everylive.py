import os
import json
import subprocess
import csv

INPUT_CSV = "youtube_links_output.csv"
OUTPUT_DIR = "dumps"
MIN_DURATION = 1800  # 30 minutes

os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_channel_info(url):
    if "/channel/" in url:
        return "channel", url.split("/channel/")[-1].split("/")[0]
    if "/@" in url:
        return "handle", url.split("/@")[-1].split("/")[0]
    return None, None

def run_yt_dlp(url):
    try:
        result = subprocess.run(
            ["yt-dlp", "--flat-playlist", "--dump-json", url],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=60
        )
        if result.returncode != 0:
            print(f"⚠ yt-dlp error:\n{result.stderr.strip()}")
            return []

        entries = []
        for line in result.stdout.splitlines():
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            duration = entry.get("duration") or 0
            if duration >= MIN_DURATION:
                entries.append({
                    "id": entry.get("id"),
                    "title": entry.get("title"),
                    "duration": duration,
                    "url": f"https://youtube.com/watch?v={entry.get('id')}",
                })
        return entries

    except subprocess.TimeoutExpired:
        print("⚠ yt-dlp timed out.")
        return []

def main():
    print(f"📄 Reading {INPUT_CSV}")
    seen = set()

    # Read second column (index 1), skip header
    with open(INPUT_CSV, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        next(reader, None)  # skip header row
        urls = [row[1].strip() for row in reader if len(row) > 1 and row[1].startswith("http")]

    for idx, url in enumerate(sorted(set(urls))):
        print(f"[{idx}] 🔍 Resolving: {url}")
        channel_type, channel_ref = get_channel_info(url)
        if not channel_ref:
            print(f"[{idx}] ⚠ Skipped (invalid format): {url}")
            continue

        if channel_ref in seen:
            print(f"[{idx}] ✅ Already processed: {channel_ref}")
            continue
        seen.add(channel_ref)

        outfile = os.path.join(OUTPUT_DIR, f"{channel_ref}.json")
        if os.path.exists(outfile):
            print(f"[{idx}] ✅ Already scraped file: {outfile}")
            continue

        # First try the /streams tab
        if channel_type == "handle":
            target = f"https://www.youtube.com/@{channel_ref}/streams"
        else:
            target = f"https://www.youtube.com/channel/{channel_ref}/streams"

        data = run_yt_dlp(target)

        # Fallback to /videos if no qualifying streams
        if not data:
            print(f"[{idx}] ⚠ No qualifying livestreams, falling back to videos tab.")
            if channel_type == "handle":
                target = f"https://www.youtube.com/@{channel_ref}/videos"
            else:
                target = f"https://www.youtube.com/channel/{channel_ref}/videos"
            data = run_yt_dlp(target)

        if data:
            with open(outfile, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            print(f"[{idx}] 💾 Saved {len(data)} entries to {outfile}")
        else:
            print(f"[{idx}] ❌ No qualifying videos found.")

if __name__ == "__main__":
    main()
