from datetime import datetime
import csv
import json
import math
import os
import os.path
import requests
import sys

# Ethereum reddit forum에서 2016-2018년 사이에 dao 키워드를 포함한 게시물과 그 댓글

# https://www.datacamp.com/tutorial/scraping-reddit-python-scrapy

posts_dir = "posts"
comments_dir = "comments"

def cache_posts(keyword="dao", target_dir="./pushshift"):
    cache_dir = f"{target_dir}/{posts_dir}/{keyword}"

    if os.path.exists(cache_dir):
        return

    os.makedirs(cache_dir)
    start_time = int(datetime.strptime('2016-01-01T00:00:00', '%Y-%m-%dT%H:%M:%S').timestamp())
    end_time = int(datetime.strptime('2018-12-31T23:59:59', '%Y-%m-%dT%H:%M:%S').timestamp())
    page_size = 250

    url = "https://api.pushshift.io/reddit/search/submission"
    params = {
        "q": keyword,
        "after": start_time,
        "before": end_time,
        "subreddit": "ethereum",
        "sort": "asc",
        "sort_type": "created_utc",
        "size": page_size,
        "metadata": "true"
    }

    initial_response = requests.get(url, params=params)
    response_json = initial_response.json()
    total_results = response_json["metadata"]["total_results"]
    pages = math.ceil(total_results / page_size)
    with open(f"{cache_dir}/post1.json", "w") as f:
        f.write(initial_response.text)
    print("----------- Started Caching Posts  -----------")

    print(f"Total Results: {total_results} posts")
    print(f"Total Pages: {pages} pages")

    last_time = response_json["data"][-1]["created_utc"]
    # we've already fetched page 1
    for i in range(1, pages):
        params = {
            **params,
            "after": last_time + 1,
        }
        response = requests.get(url, params=params)
        response_json = response.json()
        with open(f"{cache_dir}/post{i + 1}.json", "w") as f:
            f.write(response.text)
        last_time = response_json["data"][-1]["created_utc"]

        print(f"{(i + 1):5} / {pages} Fetched.", end="\r")

    print("----------- Finished Caching Posts -----------")

def cache_comments(keyword="dao", target_dir="./pushshift"):
    cache_dir = f"{target_dir}/{comments_dir}/{keyword}"

    if os.path.exists(cache_dir):
        return

    os.makedirs(cache_dir)
    start_time = int(datetime.strptime('2016-01-01T00:00:00', '%Y-%m-%dT%H:%M:%S').timestamp())
    end_time = int(datetime.strptime('2018-12-31T23:59:59', '%Y-%m-%dT%H:%M:%S').timestamp())
    page_size = 250

    url = "https://api.pushshift.io/reddit/search/comment"
    params = {
        "q": keyword,
        "after": start_time,
        "before": end_time,
        "subreddit": "ethereum",
        "sort": "asc",
        "sort_type": "created_utc",
        "size": page_size,
        "metadata": "true"
    }

    initial_response = requests.get(url, params=params)
    response_json = initial_response.json()
    total_results = response_json["metadata"]["total_results"]
    pages = math.ceil(total_results / page_size)
    with open(f"{cache_dir}/comment1.json", "w") as f:
        f.write(initial_response.text)
    print("----------- Started Caching Comments  -----------")

    print(f"Total Results: {total_results} comments")
    print(f"Total Pages: {pages} pages")

    last_time = response_json["data"][-1]["created_utc"]
    # we've already fetched page 1
    for i in range(1, pages):
        params = {
            **params,
            "after": last_time + 1,
        }
        response = requests.get(url, params=params)
        response_json = response.json()
        with open(f"{cache_dir}/comment{i + 1}.json", "w") as f:
            f.write(response.text)
        last_time = response_json["data"][-1]["created_utc"]

        print(f"{(i + 1):5} / {pages} Fetched.", end="\r")

    print("----------- Finished Caching Comments -----------")

def resolve_post_content(post):
    # determine a post's main content. it might be just text, or a link
    if "selftext" not in post:
        return "[deleted]"

    if post["selftext"] != "":
        return post["selftext"]

    # selftext is empty from now
    if post["is_self"]:
        return "[empty]"

    return post["url"]

def process_posts(keyword="dao", target_dir="./pushshift"):
    cache_dir = f"{target_dir}/{posts_dir}/{keyword}"

    if not os.path.exists(cache_dir):
        print(f"Error: Cache directory {cache_dir} for keyword {keyword} not found.", file=sys.stderr)

    fieldnames = ["num_post", "title", "author", "date", "contents", "comments", "votes", "link"]
    with open(f"{target_dir}/posts.csv", "w", newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()

        post_no = 1
        index = 1
        while os.path.exists(f"{cache_dir}/post{post_no}.json"):
            with open(f"{cache_dir}/post{post_no}.json") as f:
                data = json.loads(f.read())

            for post in data["data"]:
                contents = resolve_post_content(post)
                writer.writerow({
                    "num_post": index,
                    "title": post["title"],
                    "author": post["author"],
                    "date": datetime.fromtimestamp(post["created_utc"]),
                    "contents": contents,
                    "comments": post["num_comments"],
                    "votes": post["score"],
                    "link": post["full_link"],
                })
                index += 1

            post_no += 1

    print("Finished")

def process_comments(keyword="dao", target_dir="./pushshift"):
    cache_dir = f"{target_dir}/{comments_dir}"

    if not os.path.exists(cache_dir):
        print(f"Error: Cache directory {cache_dir} for keyword {keyword} not found.", file=sys.stderr)

    fieldnames = ["num_comment", "author", "date", "contents", "votes", "post_link", "comment_link", "reply_to"]
    with open(f"{cache_dir}/comments.csv", "w", newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()

        comment_no = 1
        index = 1
        while os.path.exists(f"{cache_dir}/comment{comment_no}.json"):
            with open(f"{cache_dir}/comment{comment_no}.json") as f:
                data = json.loads(f.read())

            for comment in data["data"]:
                post_id = comment["link_id"][3:]
                # there are some comments that do not contain "nest_level". In that case compare the link id with the parent id.
                is_reply = comment["nest_level"] > 1 if "nest_level" in comment else comment["link_id"] == comment["parent_id"]
                reply_id = comment["parent_id"][3:]

                writer.writerow({
                    "num_comment": index,
                    "author": comment["author"],
                    "date": datetime.fromtimestamp(comment["created_utc"]),
                    "contents": comment["body"],
                    "votes": comment["score"],
                    "post_link": f"https://www.reddit.com/comments/{post_id}",
                    "comment_link": f"https://www.reddit.com/comments/{post_id}/comment/{comment['id']}",
                    "reply_to": f"https://www.reddit.com/comments/{post_id}/comment/{reply_id}" if is_reply else "",
                })
                index += 1

            comment_no += 1

    print("Finished")

def setup(keyword="dao", target_dir="./pushshift"):
    cache_posts(keyword, target_dir)
    cache_comments(keyword, target_dir)


# NOTE: might be helpful - https://www.reddit.com/comments/b8yd3r/.json
if __name__ == "__main__":
    if len(sys.argv) == 2:
        target_dir = sys.argv[1]
    else:
        target_dir = "./pushshift"

    setup()
    process_posts()
    # process_comments(target_dir)
