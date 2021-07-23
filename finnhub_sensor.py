import json, yaml, os, unittest, croniter, time, datetime, tempfile, sys
import finnhub
import treldev

def crawl(ticker, min_ts, max_ts, credentials, logger=None):
    ''' Yields ticks '''
    finnhub_client = finnhub.Client(api_key=credentials['api_key'])
    res = finnhub_client.stock_candles(ticker, 1, min_ts, max_ts)
    if res['s'] != 'ok':
        return []
    keys = res.keys() - {'s'}
    for i in range(len(res['t'])):
        d = { k:res[k][i] for k in keys }
        d['ticker'] = ticker
        yield d
    finnhub_client.close()

class Test(unittest.TestCase):

    def test_crawl(self):
        credentials = {}

        with open("credentials.yml") as f:
            credentials = yaml.safe_load(f)
        now = datetime.datetime.now()
        min_ts = int(time.mktime((now - datetime.timedelta(hours=3)).timetuple()))
        max_ts = int(time.mktime(now.timetuple()))
        for v in crawl('AMC', min_ts, max_ts, json.loads(credentials['finnhub'])):
            print(v)
        
class FinnhubSensor(treldev.Sensor):

    def __init__(self, config, credentials, *args, **kwargs):
        super().__init__(config, credentials, *args, **kwargs)
        
        self.instance_ts_precision = self.config['instance_ts_precision']
        self.cron_constraint = self.config['cron_constraint']
        self.tickers = self.config['tickers']
        self.lookback_seconds = self.config['max_instance_age_seconds'] - 1 # how far we should backfill missing datasets
        self.locking_seconds = self.config.get('locking_seconds',600)
        self.delay_seconds = 30
    
    def get_new_datasetspecs(self, datasets):
        ''' If there is data ready to be inserted, this should return a datasetspec. Else, return None '''
        return self.get_new_datasetspecs_with_cron_and_precision(datasets)

    def save_data_to_path(self, load_info, uri):
        ''' if the previous call to get_new_datasetspecs returned a (load_info, datasetspec) tuple, then this call should save the data to the provided path, given the corresponding (load_info, path). '''
        ts = load_info
        ts_next = croniter.croniter(self.cron_constraint, ts).get_next(datetime.datetime)
        if self.debug:
            self.logger.debug(f"ts {ts} ts_next {ts_next}")
            
        folder = tempfile.mkdtemp()
        if self.debug:
            self.logger.debug(f"folder: {folder}")
        
        with open(folder+'/part-00000','w') as f:
            min_ts = int(time.mktime(ts.timetuple()))
            max_ts = int(time.mktime((ts_next - datetime.timedelta(seconds=1)).timetuple()))
            for ticker in self.tickers:
                for e in crawl(ticker, min_ts, max_ts, json.loads(self.credentials['finnhub'])):
                    json.dump(e,f)
                    f.write('\n')
                
        s3_commands = treldev.S3Commands(credentials=self.credentials)
        assert uri.endswith('/')
        uri = uri[:-1]
        s3_commands.upload_folder(folder, uri, logger=self.logger)
        self.logger.info(f"Uploaded {load_info} to {uri}")
        sys.stderr.flush()
        assert folder.startswith("/tmp/") # to avoid accidentally deleting something important
        os.system(f"rm -rf {folder}")
        
if __name__ == '__main__':
    treldev.Sensor.init_and_run(FinnhubSensor)
    
