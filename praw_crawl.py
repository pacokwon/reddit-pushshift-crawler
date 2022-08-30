import praw
import json
import os.path
import logging
import sys
import traceback
from datetime import datetime

posts_dir = "reddit/posts"
comments_dir = "reddit/comments"

def update_post_chunk(reddit, filename, updated_filename, comment_filename):
    with open(filename, 'r') as f:
        data = json.loads(f.read())

    comments_list = []

    index = 0
    for post in data["data"]:
        update_submission(reddit, post, comments_list)
        index += 1

    with open(updated_filename, 'w') as f:
        f.write(json.dumps(data, indent=4, ensure_ascii=False))

    with open(comment_filename, 'w') as f:
        f.write(json.dumps(comments_list, indent=4, ensure_ascii=False))

def update_submission(reddit, post, comments_list):
    submission = reddit.submission(post["id"])

    submission.comments.replace_more(limit=None)
    # https://praw.readthedocs.io/en/stable/code_overview/models/comment.html
    comments_count = 0
    for comment in submission.comments.list():
        comments_count += 1
        author = comment.author
        try:
            if author is None or not hasattr(author, 'id'):
                author_id = ""
            else:
                author_id = author.id
        except Exception as _:
            logging.error(traceback.format_exc())
            print(f"submission: {post['id']}\tcomment: {comment.id}", flush=True, file=sys.stderr)
            author_id = ""

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

if __name__ == "__main__":
    # About User Agent Naming Convention: https://github.com/reddit-archive/reddit/wiki/API
    reddit = praw.Reddit("bot", user_agent="python3:ethereum-dao-search:v1.0 (by /u/pacokwon)")

    post_no = 5
    while os.path.exists(f"{posts_dir}/post{post_no}.json"):
        print(f"Caching post{post_no}...", flush=True)
        filename = f"{posts_dir}/post{post_no}.json"
        updated_filename = f"{posts_dir}/updated_post{post_no}.json"
        comments_filename = f"{comments_dir}/post{post_no}_comment.json"
        update_post_chunk(reddit, filename, updated_filename, comments_filename)
        print(f"Post {post_no} caching finished at {datetime.now().isoformat()}", flush=True)
        post_no += 1

