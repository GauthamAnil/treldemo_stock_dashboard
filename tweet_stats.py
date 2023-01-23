

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
    parser.add_argument("--_args")
    parser.add_argument("--output")
    parser.add_argument("--_schedule_instance_ts")
    args, _ = parser.parse_known_args()

    with open(args._args) as f:
        full_args = yaml.safe_load(f)

    if args.tweet_stats is not None:
        tweet_stats_bq = BigQueryURI(args.tweet_stats)
        tweet_stats_sql = f"select * from `{tweet_stats_bq.path}`"
    else:
        tweet_stats_sql = f"select * from tweet_stats_null"
        
    output_bq = BigQueryURI(args.output)
    

    select_sqls = []
    for input_ in full_args['inputs']['tweets_condensed']:
        tweets_bq = BigQueryURI(input_['uri'])
        instance_ts,precision = instance_ts_str_to_ts_precision(input_['instance_ts_str'])
        select_sqls.append( f'select *, cast("{instance_ts}" as datetime) instance_ts from `{tweets_bq.path}`' )

    select_sql_joined = ' UNION ALL \n'.join( select_sqls )
    
    output_bq.save_sql_results(f"""
with 
params as (select cast("{full_args['schedule_instance_ts']}" as datetime) schedule_instance_ts)
,tweet_stats_null as (select * 
from (select 
    cast(NULL as datetime) ts, 
    cast(NULL as int64) tweets, 
    cast(NULL as int64) verified_tweets, 
  )
where False)
,tweet_stats as ({tweet_stats_sql})
,tweets as ({select_sql_joined})

,tweet_stats_limited as (select tweet_stats.* from tweet_stats cross join params 
where datetime_diff(schedule_instance_ts, ts, day) <= 30) 

,new_stats as (select 
  instance_ts ts, 
  count(*) tweets, 
  count(if(verified,1,NULL)) verified_tweets 
from tweets 
group by ts)

,res as (select * from tweet_stats_limited
UNION ALL
select * from new_stats)

select * from res
    """)

