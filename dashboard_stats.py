

'''
This jobs can take a dataset of tweet_stats and a list of tweets_condensed datasets to produce a new tweet_stats datasets.

tweet_stats is optional and tweets_condensed can be variable sized, but cannot be empty.

'''

import argparse, yaml
from treldev.gcputils import BigQueryURI
from treldev import sql_repr, instance_ts_str_to_ts_precision

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--tweet_stats")
    parser.add_argument("--stock_series_30day")
    parser.add_argument("--dashboard_stats")
    parser.add_argument("--_schedule_instance_ts")
    args, _ = parser.parse_known_args()

    output_bq = BigQueryURI(args.dashboard_stats)
    tweet_stats_bq = BigQueryURI(args.tweet_stats)
    stock_series_30day_bq = BigQueryURI(args.stock_series_30day)

    output_bq.save_sql_results(f"""
with 
series as (SELECT * FROM `{tweet_stats_bq.path}`)
,stock_series as (SELECT * FROM `{stock_series_30day_bq.path}`)
,max_ as (select cast("{args._schedule_instance_ts}" as datetime) max_ts)


,stock_series_5mins as (select *
  from stock_series)
,joined as (select 
  datetime(timestamp(coalesce(series.ts, stock_series.ts)),"America/New_York") ts_ny, 
  coalesce(series.ts, stock_series.ts) ts, 
  series.* EXCEPT(ts), stock_series. * EXCEPT (ts) 
from series full outer join stock_series on series.ts = stock_series.ts)

select * from joined order by ts desc
""")
