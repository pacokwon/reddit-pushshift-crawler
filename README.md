# Reddit Crawler

## Requirements
* `python 3.8+`
* `requests==2.28.1`
* `praw==7.6.0`

To install `requests` and `praw`, run the following:
```bash
pip install requests praw
```

## Configuring `praw.ini`
Before running any scripts, the `praw.ini` should be populated with the client's id and secret.

Copy the `praw.ini.example` file and create a `praw.ini` file in the same directory.
[Register a reddit application](https://www.integromat.com/en/help/app/reddit) and edit the `praw.ini` file.
The `praw.ini` file should have the form of: (random example)
```plain
[bot]
client_id=myclientidblahblah
client_secret=12l149x8cVaLsdfjk
```

## Overview of the Crawling Process
1. Collect post data from pushshift into the filesystem
2. Collect comment data from the list of post data (from 1)
3. Process post and comment data into csv files

### Collecting post data from pushshift
Example:
```bash
python3 pushshift.py --cache both --after 2016-01-01 --before 2018-12-31 --subreddit ethereum dao
# Running this command will collect posts containing the keyword "dao" from ethereum between the time period 2016-01-01 to 2018-12-31
# Check ./cache/pushshift/posts/<keyword> after running this command!
```

### Collecting comment data from the list of pushshift post data
```bash
python3 praw_crawl.py --posts-dir cache/pushshift/posts/dao
# Check ./cache/praw/comments/<keyword> after running this command!
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
