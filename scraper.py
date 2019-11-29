import os
import json
import urllib.request
import requests
import hashlib

from datetime import datetime
from io import BytesIO
from multiprocessing import Pool

from PIL import Image
import numpy as np

import pdb

##########
# PARAMS #
##########

start_day = datetime(2015, 1, 1, 0, 0)  # 01/01/2015
timestamp = int(start_day.timestamp())
score_threshold = 10
subreddit = 'earthporn'
threads = 8
span = 86400                            # Get posts per day

# Auto
savedir = './{}'.format(subreddit)
# Make savedir
if not os.path.exists(savedir):
    os.makedirs(savedir)

#########
# UTILS #
#########

# Get hash of removed image from imgur (very unfortunate we have to do this)
#removed = Image.open('removed.png')
#removed_hash = hashlib.md5(np.array(removed)).hexdigest()
removed_hash = '20fe0773c5e630ce437b2d81a58ec60c'

def is_bad_image(img):
    '''
    Check if bad image (removed image from imgur)
    '''
    hsh = hashlib.md5(np.array(img)).hexdigest()
    return removed_hash == hsh

def query(after, before):
    '''
    Hit the pushshift api (seems to be free and public?)
    `after` and `before` are unix time dates
    '''

    url = "https://api.pushshift.io/reddit/submission/search/?after={}&before={}&sort_type=score&sort=desc&subreddit={}&size=1000".format(after, before, subreddit)
    return urllib.request.urlopen(url).read()

def download_image(submission):
    '''
    Download an image given a submission (element of pushshift query)
    '''
    # If above score threshold
    if submission['score'] > score_threshold:
        try:
            response = requests.get(submission['url'])

            if response.status_code == 200:
                img = Image.open(BytesIO(response.content))
                if not is_bad_image(img):
                    img_name = submission['created_utc']
                    img.save(os.path.join(savedir, '{}.jpg'.format(img_name)))
                    print('Saved image {}'.format(img_name))
        except:
            pass

def download_day(timestamp):
    '''
    Download an entire day
    1. Hit pushshift API
    2. Cycle through submissions on given day and download each image
    '''
    # Print info
    human_readable_time = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")
    print('Scraping {}'.format(human_readable_time))

    # Get posts
    contents = query(timestamp, timestamp+span)
    contents = json.loads(contents)

    # Download images
    for submission in contents['data']:
        download_image(submission)

    # Go to next day
    timestamp += span

##########
# SCRAPE #
##########

# Multiprocess by day
pool = Pool(threads)
total_days = (datetime.today() - start_day).days
timestamps = [timestamp + n*span for n in range(total_days)]
pool.map(download_day, timestamps)
