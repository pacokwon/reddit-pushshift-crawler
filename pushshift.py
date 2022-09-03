import argparse
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

def cache_posts(keyword, after, before, subreddit, target_dir="./cache/pushshift"):
    cache_dir = f"{target_dir}/{posts_dir}/{keyword}"

    if os.path.exists(cache_dir):
        return

    os.makedirs(cache_dir)
    start_time = int(datetime.strptime(after, '%Y-%m-%dT%H:%M:%S').timestamp())
    end_time = int(datetime.strptime(before, '%Y-%m-%dT%H:%M:%S').timestamp())
    page_size = 250

    url = "https://api.pushshift.io/reddit/search/submission"
    params = {
        "q": "" if keyword == "all" else keyword,
        "after": start_time,
        "before": end_time,
        "subreddit": subreddit,
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
    print(f"Cache directory is {cache_dir}", end="\n\n")

def cache_comments(keyword, after, before, subreddit, target_dir="./cache/pushshift"):
    cache_dir = f"{target_dir}/{comments_dir}/{keyword}"

    if os.path.exists(cache_dir):
        return

    os.makedirs(cache_dir)
    start_time = int(datetime.strptime(after, '%Y-%m-%dT%H:%M:%S').timestamp())
    end_time = int(datetime.strptime(before, '%Y-%m-%dT%H:%M:%S').timestamp())
    page_size = 250

    url = "https://api.pushshift.io/reddit/search/comment"
    params = {
        "q": keyword,
        "after": start_time,
        "before": end_time,
        "subreddit": subreddit,
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
    print(f"Cache directory is {cache_dir}", end="\n\n")

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

def process_posts(keyword="dao", target_dir="./cache/pushshift", output_dir="./results"):
    cache_dir = f"{target_dir}/{posts_dir}/{keyword}"

    if not os.path.exists(cache_dir):
        print(f"Error: Cache directory {cache_dir} for keyword {keyword} not found.", file=sys.stderr)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    fieldnames = ["num_post", "title", "author", "date", "contents", "comments", "votes", "link"]
    with open(f"{output_dir}/{keyword}-posts.csv", "w", newline='') as csvfile:
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

    print(f"Finished processing posts. Results are at {output_dir}/{keyword}-posts.csv")

def process_comments(keyword, target_dir="./cache/pushshift", output_dir="./results"):
    cache_dir = f"{target_dir}/{comments_dir}"

    if not os.path.exists(cache_dir):
        print(f"Error: Cache directory {cache_dir} for keyword {keyword} not found.", file=sys.stderr)

    fieldnames = ["num_comment", "author", "date", "contents", "votes", "post_link", "comment_link", "reply_to"]
    with open(f"{output_dir}/{keyword}-comments.csv", "w", newline='') as csvfile:
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

    print(f"Finished processing comments. Results are at {output_dir}/{keyword}-comments.csv")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("keyword", help='Which keyword to search from pushshift. Pass "all" to search all posts. ex> "ethereum", "dao"')
    parser.add_argument("--cache", help='Supported values: "none" | "post" | "comment" | "both" (without double quotes).', default="both")
    parser.add_argument("--process", help='Supported values: "none" | "post" | "comment" | "both" (without double quotes)', default="none")
    parser.add_argument(
        "--after",
        help='Posts or comments posted after this date will be collected. format> 2016-01-01',
        type=lambda s: f'{s}T00:00:00'
    )
    parser.add_argument(
        "--before",
        help='Posts or comments posted before this date will be collected format> 2018-12-31',
        type=lambda s: f'{s}T23:59:59'
    )
    parser.add_argument("--subreddit", help='Which subreddit to search posts or comments from. ex> "ethereum"', default="ethereum")

    args = parser.parse_args()
    keyword = args.keyword

    subreddit = args.subreddit
    if args.cache != "none":
        if args.after is None:
            print("The --after option must be specified for --cache", file=sys.stderr)
        if args.before is None:
            print("The --before option must be specified for --cache", file=sys.stderr)

    if args.cache == "both":
        cache_posts(keyword, args.after, args.before, subreddit)
        cache_comments(keyword, args.after, args.before, subreddit)
    elif args.cache == "post":
        cache_posts(keyword, args.after, args.before, subreddit)
    elif args.cache == "comment":
        cache_comments(keyword, args.after, args.before, subreddit)

    if args.process == "both":
        process_posts(keyword)
        process_comments(keyword)
    elif args.process == "post":
        process_posts(keyword)
    elif args.process == "comment":
        process_comments(keyword)
