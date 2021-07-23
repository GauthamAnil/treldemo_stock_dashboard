

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
    parser.add_argument("--dashboard_stats")
    parser.add_argument("--_schedule_instance_ts")
    args, _ = parser.parse_known_args()

    output_bq = BigQueryURI(args.dashboard_stats)
    tweet_stats_bq = BigQueryURI(args.tweet_stats)

    output_bq.save_sql_results(f"""
with 
series as (SELECT * FROM `{tweet_stats_bq.path}`)
,max_ as (select cast("{args._schedule_instance_ts}" as datetime) max_ts)
,recent as (select series.* from series cross join max_ where datetime_diff(max_ts, ts, HOUR) < 5)
,older as (select series.* from series cross join max_ where datetime_diff(max_ts, ts, HOUR) >= 5)
, older_agg as (select datetime(timestamp_seconds(cast(avg(unix_seconds(timestamp(ts))) as int64))) ts, avg(tweets) tweets, avg(verified_tweets) verified_tweets from older group by substr(cast(ts as string),1,13))
,combined as (select * from older_agg UNION ALL select * from recent)
,res as (select * from combined order by ts desc limit 300)

select * from res
    """)
