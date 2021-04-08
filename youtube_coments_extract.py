# searches for videos in a Youtube channel matching a specific keyword and date range
# fetches comments and comment replies and stores them in a CSV file
# uses YouTube Data API
# needs a Google account and Google OAuth 2.0 credentials (https://developers.google.com/youtube/v3/getting-started)
import os

import pickle
import csv
from tqdm import tqdm

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from datetime import datetime, timezone

import pytz
import sys

# https://stackoverflow.com/questions/63837906/any-way-to-use-google-api-without-every-time-authentication
# https://python.gotrained.com/youtube-api-extracting-comments/

# The CLIENT_SECRETS_FILE variable specifies the name of a file that contains
# the OAuth 2.0 credentials for this application, including its client_id and
# client_secret.
# place it in the same location as this python file
CLIENT_SECRETS_FILE = "client_secret_sb.json"

# This OAuth 2.0 access scope allows for full read/write access to the
# authenticated user's account and requires requests to use an SSL connection.
SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']
API_SERVICE_NAME = 'youtube'
API_VERSION = 'v3'

# maximum results per page
maxResults = 50

# The file token.pickle stores the user's access and refresh tokens, and is
# created automatically when the authorization flow completes for the first
# time.
def get_authenticated_service():
    credentials = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            credentials = pickle.load(token)
    #  Check if the credentials are invalid or do not exist
    if not credentials or not credentials.valid:
        # Check if the credentials have expired
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                CLIENT_SECRETS_FILE, SCOPES)
            credentials = flow.run_console()

        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(credentials, token)

    return build(API_SERVICE_NAME, API_VERSION, credentials=credentials)


def write_to_csv(comments):
    with open('comments.csv', 'w', newline='', encoding="utf-8") as comments_file:
        comments_writer = csv.writer(comments_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        # comments_writer.writerow(['Video ID', 'Title', 'Comment','Comment_id'])
        comments_writer.writerow(
            ['Video ID', 'Video Date', 'Title', 'Comment Text', 'Comment Id', 'Comment Date', 'Comment Likes', 'Comment Replies', 'Reply Text',
             'Reply Date', 'Reply Likes'])
        for row in comments:
            comments_writer.writerow(list(row))

# called once, cost=100
def get_videos(service, **kwargs):
    final_results = []
    results = service.search().list(**kwargs).execute()

    i = 0
    # get maximum of 150 videos (3 * 50 results/page)
    max_pages = 3
    while results and i < max_pages:
        final_results.extend(results['items'])

        # Check if another page exists
        if 'nextPageToken' in results:
            kwargs['pageToken'] = results['nextPageToken']
            results = service.search().list(**kwargs).execute()
            i += 1
        else:
            break
    return final_results

# called once for each video, cost=5
def get_video_comments(service, **kwargs):
    comments = []
    results = service.commentThreads().list(**kwargs).execute()

    i = 0
    # get maximum of 1000 comments (20 * 50 results/page)
    max_pages = 20
    while results and i < max_pages:
        for item in results['items']:
            comments.append(item)

        if 'nextPageToken' in results:
            kwargs['pageToken'] = results['nextPageToken']
            results = service.commentThreads().list(**kwargs).execute()
            i += 1
        else:
            break
    return comments

# called once for each comment with a reply, cost = 2
def get_comment_replies(service, **kwargs):
    comment_replies = []
    results = service.comments().list(**kwargs).execute()

    i = 0
    # get maximum of 100 replies (2 * 50 results/page)
    max_pages = 2
    while results and i < max_pages:
        for item in results['items']:
            comment_replies.append(item)
        if 'nextPageToken' in results:
            kwargs['pageToken'] = results['nextPageToken']
            results = service.comments().list(**kwargs).execute()
            i += 1
        else:
            break
    return comment_replies


def search_videos_by_keyword(service, **kwargs):
    final_result = []

    try:
        results = get_videos(service, **kwargs)
    except Exception as e:
        print("Error in get_videos: ", e)
        write_to_csv(final_result)
        sys.exit()

    for item in tqdm(results):
        title = item['snippet']['title']
        video_id = item['id']['videoId']
        video_date = item['snippet']['publishedAt']
        # print(title, "-", video_id)
        try:
            comments = get_video_comments(service, part='snippet,replies', maxResults=maxResults, order='relevance', videoId=video_id,
                                      textFormat='plainText')
        except Exception as e:
            print("Error in get_video_comments: ", e)
            write_to_csv(final_result)
            sys.exit()

        for item_comment in tqdm(comments):
            comment_text = item_comment['snippet']['topLevelComment']['snippet']['textDisplay']
            comment_id = item_comment['id']
            comment_likes = item_comment['snippet']['topLevelComment']['snippet']['likeCount']
            comment_date = item_comment['snippet']['topLevelComment']['snippet']['publishedAt']
            comment_replies_count = item_comment['snippet']['totalReplyCount']
            # print(comment_text, "-", comment_id, "-", comment_likes, '-', comment_replies_count)
            # fetch replies only if there are more than 5 replies to a comment
            if comment_replies_count > 5:
                try:
                    comment_replies = get_comment_replies(service, part='snippet', maxResults=maxResults, parentId=comment_id,
                                                      textFormat='plainText')
                    # make a tuple consisting of the video id, title, comment and add the result to
                    # the final list
                    final_result.extend([(video_id, video_date, title, comment_text,
                                      comment_id, comment_date, comment_likes, comment_replies_count,
                                      comment_reply['snippet']['textDisplay'],
                                      comment_reply['snippet']['publishedAt'],
                                      comment_reply['snippet']['likeCount']) for comment_reply in comment_replies])
                except Exception as e:
                    print("Error in get_comment_replies: ", e)
                    write_to_csv(final_result)
                    sys.exit()
            else:
                final_result.append([video_id, video_date, title, comment_text,
                                      comment_id, comment_date, comment_likes, comment_replies_count])
    write_to_csv(final_result)


def format_input_date(input_time):
    # convert from IST to UTC
    local_tz = pytz.timezone("Asia/Kolkata")
    local_date_time = datetime.strptime(input_time, '%d-%b-%Y %H:%M:%S')
    local_dt_with_tz = local_tz.localize(local_date_time, is_dst=None)
    utc_dt = local_dt_with_tz.astimezone(pytz.utc)
    rfc3339_format = utc_dt.isoformat().split('+', 1)[0]
    rfc3339_format = rfc3339_format + "Z"
    return rfc3339_format


if __name__ == '__main__':
    # When running locally, disable OAuthlib's HTTPs verification. When
    # running in production *do not* leave this option enabled.
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
    service = get_authenticated_service()
    print("maxResults set to: ", maxResults)
    keyword = input('Enter a keyword: ')

    while True:
        channelId = input('Enter a channelId:')
        if not channelId:
            print("channenlId is a required input")
            continue
        else:
            break

    print("Please do not enter a date range of more than 7 days since this can fetch a maximum of 150 videos")
    fromTime = input('Enter a fromTime(dd-Mon-yyyy hh24:mi:ss):')
    toTime = input('Enter a toTime(dd-Mon-yyyy hh24:mi:ss):')

    #sys.exit()

    if fromTime and toTime:
        fromTime_formatted = format_input_date(fromTime)
        toTime_formatted = format_input_date(toTime)
        search_videos_by_keyword(service, q=keyword, part='id,snippet', maxResults=maxResults, type='video',
                                 channelId=channelId, publishedBefore=toTime_formatted,
                                 publishedAfter=fromTime_formatted)
    else:
        print("Some date parameters not provided. Executing search without date range.")
        print("A maximum of 150 videos will be processed.")
        search_videos_by_keyword(service, q=keyword, part='id,snippet', maxResults=maxResults, type='video',
                                 channelId=channelId)