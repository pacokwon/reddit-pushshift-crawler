import argparse
import praw
import json
import os
import os.path
import pathlib
import logging
import sys
import traceback
from datetime import datetime

def update_post_chunk(reddit, filename, updated_filename, comment_filename):
    with open(filename, 'r') as f:
        data = json.loads(f.read())

    comments_list = []

    index = 0
    for post in data["data"]:
        update_submission_and_crawl_comments(reddit, post, comments_list)
        print(post["id"])
        index += 1

    with open(updated_filename, 'w') as f:
        f.write(json.dumps(data, indent=4, ensure_ascii=False))

    with open(comment_filename, 'w') as f:
        f.write(json.dumps(comments_list, indent=4, ensure_ascii=False))

def update_submission_and_crawl_comments(reddit, post, comments_list):
    submission = reddit.submission(post["id"])

    submission.comments.replace_more(limit=None)
    # https://praw.readthedocs.io/en/stable/code_overview/models/comment.html
    comments_count = 0
    for comment in submission.comments.list():
        comments_count += 1
        author = comment.author
        try:
            if author is None or not hasattr(author, 'id'):
                author_id = "[deleted]"
            else:
                author_id = author.id
        except Exception as _:
            logging.error(traceback.format_exc())
            print(f"[{datetime.now().isoformat()}] submission: {post['id']}\tcomment: {comment.id}", flush=True, file=sys.stderr)
            author_id = "[deleted]"

        author_name = "[deleted]" if author is None else author.name
        comments_list.append({
            "author_id": author_id,
            "author_name": author_name,
            "body": comment.body,
            "body_html": comment.body_html,
            "created_utc": int(comment.created_utc),
            "distinguished": comment.distinguished,
            "edited": int(comment.edited),
            "id": comment.id,
            "is_submitter": comment.is_submitter,
            "link_id": comment.link_id,
            "parent_id": comment.parent_id,
            "permalink": comment.permalink,
            "saved": comment.saved,
            "score": comment.score,
            "stickied": comment.stickied,
            "subreddit_id": comment.subreddit_id,
        })

    post["score"] = submission.score
    post["upvote_ratio"] = submission.upvote_ratio
    post["num_comments"] = comments_count

def crawl_comments(reddit, posts_dir):
    post_no = 1
    posts_path = pathlib.PurePath(posts_dir)
    keyword = posts_path.stem
    comments_dir = str(posts_path.parents[2].joinpath(f"praw/comments/{keyword}"))

    if not os.path.exists(comments_dir):
        os.makedirs(comments_dir)

    while os.path.exists(f"{posts_dir}/post{post_no}.json"):
        filename = f"{posts_dir}/post{post_no}.json"
        updated_filename = f"{posts_dir}/updated_post{post_no}.json"
        comments_filename = f"{comments_dir}/post{post_no}_comment.json"
        update_post_chunk(reddit, filename, updated_filename, comments_filename)
        post_no += 1

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--posts-dir", help='Where posts are cached at (by pushshift). ex> ./cache/pushshift/posts/dao', required=True)
    args = parser.parse_args()

    # About User Agent Naming Convention: https://github.com/reddit-archive/reddit/wiki/API
    reddit = praw.Reddit("bot", user_agent="python3:ethereum-dao-search:v1.0 (by /u/pacokwon)")
    crawl_comments(reddit, args.posts_dir)
