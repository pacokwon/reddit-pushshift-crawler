import argparse
import requests
import csv
import os
import os.path
import json
import sys
from datetime import datetime

posts_dir = "./cache/posts"
comments_dir = "./reddit/comments"

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

def process_posts(posts_dir, target_dir):
    fieldnames = ["num_post", "title", "author", "date", "contents", "comments", "votes", "link", "upvote_ratio"]
    with open(f"{target_dir}/posts.csv", "w", newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()

        post_no = 1
        index = 1
        while os.path.exists(f"{posts_dir}/updated_post{post_no}.json"):
            with open(f"{posts_dir}/updated_post{post_no}.json") as f:
                data = json.loads(f.read())

            for post in data["data"]:
                contents = resolve_post_content(post)
                if "upvote_ratio" not in post:
                    print(post)

                writer.writerow({
                    "num_post": index,
                    "title": post["title"],
                    "author": post["author"],
                    "date": datetime.fromtimestamp(post["created_utc"]),
                    "contents": contents,
                    "comments": post["num_comments"],
                    "votes": post["score"],
                    "link": post["full_link"],
                    "upvote_ratio": post["upvote_ratio"]
                })
                index += 1

            post_no += 1

    print(f"Finished. Results stored in {target_dir}/posts.csv")

def process_comments(comments_dir, target_dir):
    fieldnames = ["num_comment", "author_id", "author_name", "comment_id", "date", "contents", "votes", "post_link", "comment_link", "reply_to", "nested_level"]
    with open(f"{target_dir}/comments.csv", "w", newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()

        post_no = 1
        index = 1
        while os.path.exists(f"{comments_dir}/post{post_no}_comment.json"):
            with open(f"{comments_dir}/post{post_no}_comment.json") as f:
                data = json.loads(f.read())

            nmap = make_nested_map(data)
            for comment in data:
                post_id = comment["link_id"][3:]
                # there are some comments that do not contain "nest_level". In that case compare the link id with the parent id.
                is_reply = comment["link_id"] != comment["parent_id"]
                reply_id = comment["parent_id"][3:]
                comment_id = comment['id']

                writer.writerow({
                    "num_comment": index,
                    "author_id": comment["author_id"],
                    "author_name": comment["author_name"],
                    "comment_id": comment_id,
                    "date": datetime.fromtimestamp(comment["created_utc"]),
                    "contents": comment["body"],
                    "votes": comment["score"],
                    "post_link": f"https://www.reddit.com/comments/{post_id}",
                    "comment_link": f"https://www.reddit.com/comments/{post_id}/comment/{comment_id}",
                    "reply_to": f"https://www.reddit.com/comments/{post_id}/comment/{reply_id}" if is_reply else "",
                    "nested_level": nmap[comment_id] + 1,
                })
                index += 1

            post_no += 1

    print(f"Finished. Results stored in {target_dir}/comments.csv")

def make_nested_map(data):
    parent_map = dict()
    nested_map = dict()

    def record_nest(comment_id):
        if comment_id in nested_map:
            return

        parent = parent_map[comment_id]
        if parent == "":
            nested_map[comment_id] = 0
            return

        if parent not in nested_map:
            record_nest(parent)

        nested_map[comment_id] = 1 + nested_map[parent]

    for record in data:
        parent_id = record["parent_id"]
        if parent_id[1] == "3":
            parent_map[record["id"]] = ""
        else:
            parent_map[record["id"]] = parent_id[3:]

    for record in data:
        record_nest(record["id"])

    return nested_map

# Check if posts that have their contents collected as "[deleted]" from reddit
# is also collected as "[deleted]" from pushshift.
def check_posts():
    count = 0
    post_no = 1
    while os.path.exists(f"{posts_dir}/updated_post{post_no}.json"):
        with open(f"{posts_dir}/updated_post{post_no}.json") as f:
            data = json.loads(f.read())

        ids = []
        for post in data["data"]:
            if "selftext" not in post:
                continue

            if post["selftext"] == "[deleted]":
                ids.append(post["id"])
                count += 1

                if len(ids) > 20:
                    results = requests.get("http://api.pushshift.io/reddit/search/submission?ids=" + ",".join(ids))
                    data = results.json()["data"]
                    for record in data:
                        if record["selftext"] != "[deleted]":
                            print(record["id"], record["selftext"])

                    ids = []

        if ids:
            results = requests.get("http://api.pushshift.io/reddit/search/submission?ids=" + ",".join(ids))
            data = results.json()["data"]
            for record in data:
                if record["selftext"] != "[deleted]":
                    print(record["id"], record["selftext"])

        post_no += 1

    print(count)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", help='Directory to write results to. ./results by default', default="./results")
    parser.add_argument("--posts-dir", help='Where posts are cached at. ex> ./cache/pushshift/posts/dao')
    parser.add_argument("--comments-dir", help='Where comments are cached at. ex> ./cache/praw/comments/dao')
    args = parser.parse_args()

    output_dir = args.output_dir

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    if args.posts_dir is None and args.comments_dir is None:
        print("No arguments are specified. Exiting...")
        sys.exit(1)

    if args.posts_dir is not None:
        process_posts(args.posts_dir, output_dir)

    if args.comments_dir is not None:
        process_comments(args.comments_dir, output_dir)
