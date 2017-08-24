
import config
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

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget('tmp/raw.tsv', format=UTF8)

    def run(self):
        subreddit = self.reddit.subreddit('coffee')
        now = int(time.time())
        start = now - 86400
        end = now
        with self.output().open('w') as out_file:
            for submission in subreddit.submissions(start, end):
                submission.comments.replace_more(limit=0)
                for comment in submission.comments.list():
                    # replace new lines to separate comments
                    text = comment.body.replace('\n', ' ')
                    # print(comment.body)
                    out_file.write('{}\n'.format(comment.body))

if __name__ == '__main__':
    luigi.run()
