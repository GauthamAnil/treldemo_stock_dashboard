import tweepy, json, yaml, os, unittest, croniter, time, datetime, tempfile, sys
import treldev

def crawl(hashtag, credentials, tweets_per_query = 100, max_tweets = 1000000, since_id=None, logger=None, until=None):
    ''' Yields tweets for the hashtag '''
    authentication = tweepy.OAuthHandler(credentials['consumer_key'], credentials['consumer_secret'])
    authentication.set_access_token(credentials['access_token'], credentials['access_secret'])
    api = tweepy.API(authentication, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    
    min_id = None
    tweet_count = 0
    while tweet_count < max_tweets:
        extra_args = {} if min_id is None else {'max_id': str(min_id - 1)}
        res = api.search(q=hashtag, count=min(max_tweets-tweet_count,tweets_per_query), result_type="recent", lang='en', **extra_args)
        #p#rint( 'tweet_count', tweet_count, min_id, len(res), until, extra_args )
        if not res:
            return
        
        for tweet in res:
            #print(json.dumps(tweet._json))
            if since_id is not None and tweet._json['id'] < since_id:
                return

            try:
                tweet._json['created_ts'] = str(datetime.datetime.strptime(tweet._json['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))
                yield tweet, tweet._json
            except Exception as ex:
                if logger:
                    logger.warning(f"could not process tweet {tweet._json['id']}. Got exception: {str(ex)}")
        '''
        {'id':tweet._json['id'],
        'created_at':str(datetime.datetime.strptime(tweet._json['created_at'],'%a %b %d %H:%M:%S +0000 %Y')),
        'user_id': tweet._json['user']['id'],
        'text':tweet._json.get('text'),
        'truncated':tweet._json['truncated'],
        'verified': tweet._json['user']['verified'],
        }
        '''
        
        tweet_count += len(res)	
        min_id = res[-1].id

class Test(unittest.TestCase):

    def test_crawl(self):
        import logging
        credentials = {}

        with open(os.path.expanduser("~/.twitter.creds.treldemo_stock_dashboard")) as f:
            credentials['twitter'] = yaml.load(f)
        print(credentials)

        for e,r in crawl('#amc',credentials['twitter'], max_tweets=13, tweets_per_query=3, logger=logging.getLogger()):
            print(r)
        
class TwitterSensor(treldev.Sensor):

    def __init__(self, config, credentials, *args, **kwargs):
        super().__init__(config, credentials, *args, **kwargs)
        
        self.instance_ts_precision = self.config['instance_ts_precision']
        self.cron_constraint = self.config['cron_constraint']
        self.hashtag = self.config['hashtag']
        self.lookback_seconds = self.config['lookback_seconds'] # how far we should backfill missing datasets
        self.locking_seconds = self.config.get('locking_seconds',600)
    
    def get_new_datasetspecs(self, datasets):
        ''' If there is data ready to be inserted, this should return a datasetspec. Else, return None '''
        len_to_keep = {'D':10, 'H':13,'M':16,'S':19}[self.instance_ts_precision]
        
        existing_tss = set([ ds['instance_ts'][:len_to_keep] for ds in datasets if ds['instance_ts_precision'] == self.instance_ts_precision ])

        # we will this set with all the timestamps that should have been there in the catalog, but are missing
        missing_tss = set([])

        now = datetime.datetime.now()
        index_ts = now
        itr = croniter.croniter(self.cron_constraint, now)
        # go back twice to make sure you have a complete window ahead.
        # from        - - - - | - - - - | - x
        # one back    - - - - | - - - - x - -
        # two back    - - - - x - - - - | - -
        # complete window     ^^^^^^^^^^^  after index_ts
        index_ts = itr.get_prev(datetime.datetime) 
        index_ts = itr.get_prev(datetime.datetime)
            
        lookback_delta = datetime.timedelta(seconds=self.lookback_seconds)
        while index_ts > now - lookback_delta:
            if str(index_ts)[:len_to_keep] not in existing_tss:
                missing_tss.add(index_ts)
            index_ts = itr.get_prev(datetime.datetime)
        if self.debug:
            self.logger.debug(f"missing_tss {missing_tss}")
        
        for missing_ts in sorted(missing_tss, reverse=True):
            yield missing_ts, { 'instance_prefix':None,
                                'instance_ts': str(missing_ts),
                                'instance_ts_precision':self.instance_ts_precision,
                                'locking_seconds': self.locking_seconds }

    def save_data_to_path(self, load_info, uri):
        ''' if the previous call to get_new_datasetspecs returned a (load_info, datasetspec) tuple, then this call should save the data to the provided path, given the corresponding (load_info, path). '''
        ts = load_info
        ts_next = croniter.croniter(self.cron_constraint, ts).get_next(datetime.datetime)
        if self.debug:
            self.logger.debug(f"ts {ts} ts_next {ts_next}")
        
        if getattr(self, 'crawler',None) is not None and self.last_tweet['created_at'] >= str(ts_next):
            # crawler is good enough
            if self.debug:
                self.logger.debug(f"reuse crawler")
            pass
        else:
            # reset the crawler
            if self.debug:
                self.logger.debug(f"new crawler")
            self.crawler = crawl(self.hashtag, json.loads(self.credentials['twitter']), until=ts_next, logger=self.logger, max_tweets=100)
        folder = tempfile.mkdtemp()
        if self.debug:
            self.logger.debug(f"folder: {folder}")
        
        with open(folder+'/part-00000','w') as f:
            while True:
                try:
                    _, self.last_tweet = next(self.crawler)
                    if self.debug:
                        self.logger.debug(f"tweet: {self.last_tweet}")
                    if self.last_tweet['created_ts'] >= str(ts_next):
                        if self.debug:
                            self.logger.debug(f"tweet: {self.last_tweet['created_ts']} > {str(ts_next)}. continuing")
                        continue
                    if self.last_tweet['created_ts'] >= str(ts):
                        json.dump(self.last_tweet, f)
                        f.write('\n')
                    else:
                        if self.debug:
                            self.logger.debug(f"tweet: {self.last_tweet['created_ts']} < {str(ts)}. breaking")
                        break
                except StopIteration:
                    self.crawler = None
                    self.last_tweet = None
                    break
                
        s3_commands = treldev.S3Commands(credentials=self.credentials)
        assert uri.endswith('/')
        uri = uri[:-1]
        s3_commands.upload_folder(folder, uri, logger=self.logger)
        self.logger.info(f"Uploaded {load_info} to {uri}")
        sys.stderr.flush()
        assert folder.startswith("/tmp/") # to avoid accidentally deleting something important
        os.system(f"rm -rf {folder}")

        '''
        bquri = treldev.gcputils.BigQueryURI(uri) # wraps credential management and improves readability
        from google.cloud import bigquery
        schema = [
            bigquery.SchemaField("id","string", mode="REQUIRED"),
            bigquery.SchemaField("created_at","datetime", mode="REQUIRED"),
            bigquery.SchemaField("user_id","string", mode="NULLABLE"),
            bigquery.SchemaField("text","string", mode="NULLABLE"),
            bigquery.SchemaField("truncated","string", mode="NULLABLE"),
            bigquery.SchemaField("verified","string", mode="NULLABLE"),
        ]
        bquri.load_file(folder+'/part-00000', {"source_format":bigquery.job.SourceFormat.NEWLINE_DELIMITED_JSON,
                                               "schema":schema})
        self.logger.info(f"Uploaded {load_info} to {uri}")
        
        assert folder.startswith("/tmp/") # to avoid accidentally deleting something important
        os.system(f"rm -rf {folder}")

        '''
        
        
if __name__ == '__main__':
    treldev.Sensor.init_and_run(TwitterSensor)
    
