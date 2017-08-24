
import config
import json
import luigi
from luigi.format import UTF8
import praw
from praw.models import MoreComments
import time
import util.common as util

class GetRedditContent(luigi.Task):
    """
    This task retrieves the comments for the top posts on the /r/coffee subreddit
    """
    reddit = praw.Reddit(client_id=config.REDDIT_CLIENT_ID,
                         client_secret=config.REDDIT_CLIENT_SECRET,
                         user_agent=config.USER_AGENT,
                         username=config.USERNAME,
                         password=config.PASSWORD)

    def output(self):
        return luigi.LocalTarget('tmp/raw.json')

    def run(self):
        results = []
        subreddit = self.reddit.subreddit('coffee')
        now = int(time.time())
        start = now - 86400
        end = now
        for submission in subreddit.submissions(start, end):
            results.append({
                "id": submission.id,
                "type": "post",
                "title": submission.title,
                "text": submission.selftext,
                "created": int(submission.created),
                "score": submission.score,
                "gilded": submission.gilded
            })
            submission.comments.replace_more(limit=0)
            for comment in submission.comments.list():
                results.append({
                    "id": comment.id,
                    "type": "comment",
                    "text": comment.body,
                    "created": int(comment.created),
                    "score": comment.score,
                    "gilded": comment.gilded
                })

        with self.output().open('w') as output:
            json.dump(results, output)

if __name__ == '__main__':
    luigi.run()
