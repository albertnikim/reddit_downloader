import pandas as pd
import praw
import argparse
from config import REDDIT_CONFIG
from tqdm import tqdm
import re


def connect_reddit():
    reddit_api = praw.Reddit(
        user_agent=REDDIT_CONFIG['user_agent'],
        client_id=REDDIT_CONFIG['client_id'],
        client_secret=REDDIT_CONFIG['client_secret']
    )
    return reddit_api


def downloader(subreddit_names, limit, output_file, sorting, include_comments, include_post, include_subreddit,
               include_username):
    reddit = connect_reddit()

    processed_posts = set()
    comments = []

    for subreddit_name in subreddit_names:
        subreddit = reddit.subreddit(subreddit_name)

        if sorting == 'top':
            submissions = subreddit.top(limit=limit)
        elif sorting == 'hot':
            submissions = subreddit.hot(limit=limit)
        elif sorting == 'new':
            submissions = subreddit.new(limit=limit)
        else:
            raise ValueError("Invalid sorting option. Choose between 'top', 'hot', or 'new'.")

        for submission in tqdm(submissions, desc=f"Subreddit: {subreddit_name}", unit="submission"):
            if submission.id in processed_posts:
                continue

            processed_posts.add(submission.id)

            if include_comments:
                submission.comments.replace_more(limit=None)
                for comment in submission.comments.list():
                    comment_data = {}
                    if include_comments:
                        comment_data['Comment'] = comment.body
                    if include_post:
                        title = re.sub(r'[\r\n]+', ' ', submission.title)
                        selftext = re.sub(r'[\r\n]+', ' ', submission.selftext)
                        comment_data['Post'] = title + " " + selftext
                    if include_subreddit:
                        comment_data['Subreddit'] = submission.subreddit.display_name
                    if include_username:
                        comment_data['Username'] = comment.author.name if comment.author else '[deleted]'
                    comments.append(comment_data)
            else:
                post_data = {}
                if include_post:
                    title = re.sub(r'[\r\n]+', ' ', submission.title)
                    selftext = re.sub(r'[\r\n]+', ' ', submission.selftext)
                    post_data['Post'] = title + " " + selftext
                if include_subreddit:
                    post_data['Subreddit'] = submission.subreddit.display_name
                if include_username:
                    post_data['Username'] = submission.author.name if submission.author else '[deleted]'
                comments.append(post_data)

    df = pd.DataFrame(comments)
    df.to_csv(output_file, sep='\t', index=False)


def main():
    parser = argparse.ArgumentParser(description='Download Reddit comments from specified subreddits')
    parser.add_argument('--input', type=str, help='File containing subreddit names, one per line',
                        default='subreddit_names.txt', required=True)
    parser.add_argument('--limit', type=int, help='Limit of posts to download from each subreddit', default=100)
    parser.add_argument('--output', type=str, help='Output file name', default='reddit_comments.tsv', required=True)
    parser.add_argument('--sorting', type=str, help="Sorting option: 'top', 'hot', or 'new'", default='new')
    parser.add_argument('--include-comments', action='store_true', help='Include comments in the output')
    parser.add_argument('--include-post', action='store_true', help='Include the text of the submission')
    parser.add_argument('--include-subreddit', action='store_true', help='Include subreddit names in the output')
    parser.add_argument('--include-username', action='store_true', help='Include the username in the output')
    parser.add_argument('--include-all', action='store_true', help='Include all fields in the output')

    args = parser.parse_args()

    if args.include_all:
        args.include_comments = True
        args.include_post = True
        args.include_subreddit = True
        args.include_username = True

    with open(args.input, 'r') as file:
        subreddit_names = [line.strip() for line in file]

    downloader(
        subreddit_names,
        args.limit,
        args.output,
        args.sorting,
        args.include_comments,
        args.include_post,
        args.include_subreddit,
        args.include_username
    )


if __name__ == "__main__":
    main()
