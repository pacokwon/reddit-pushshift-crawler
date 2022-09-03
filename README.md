# Reddit Crawler

## Requirements
* `python 3.8+`
* `requests==2.28.1`
* `praw==7.6.0`

## Overview of the Crawling Process
1. Collect post data from pushshift into the filesystem
2. Collect comment data from the list of post data (from 1)
3. Process post and comment data into csv files

### Collecting post data from pushshift
Example:
```bash
python3 pushshift.py --cache --after 2016-01-01 --before 2018-12-31 --subreddit ethereum dao
# Running this command will collect posts containing the keyword "dao" from ethereum between the time period 2016-01-01 to 2018-12-31
```

### Collecting comment data from the list of pushshift post data
```bash
python3 praw_crawl.py --posts-dir cache/pushshift/posts/dao
```

### Process post and comment data into csv files
Example:
```bash
$ python3 praw_process.py --posts-dir cache/pushshift/posts/dao
Finished. Results stored in ./results/posts.csv
```

Example:
```bash
$ python3 praw_process.py --comments-dir cache/praw/comments/dao
Finished. Results stored in ./results/comments.csv
```
