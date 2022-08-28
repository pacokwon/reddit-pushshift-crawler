import requests

# https://www.reddit.com/r/redditdev/comments/fxfslf/how_to_scrape_data_from_a_time_period/

if __name__ == "__main__":
    # dao subreddit:ethereum
    url = "https://old.reddit.com/r/ethereum/"
    # Headers to mimic a browser visit
    headers = {'User-Agent': 'Mozilla/5.0'}

    # Returns a requests.models.Response object
    page = requests.get(url, headers=headers)
