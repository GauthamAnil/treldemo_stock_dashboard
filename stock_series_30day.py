import argparse, yaml
from treldev.gcputils import BigQueryURI
from treldev import sql_repr, instance_ts_str_to_ts_precision

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--stock_series_30day", dest='stock_series')
    parser.add_argument("--_args")
    parser.add_argument("--output")
    parser.add_argument("--_schedule_instance_ts")
    args, _ = parser.parse_known_args()

    with open(args._args) as f:
        full_args = yaml.safe_load(f)

    if args.stock_series is not None:
        stock_series_bq = BigQueryURI(args.stock_series)
        stock_series_sql = f"select * from `{stock_series_bq.path}`"
    else:
        stock_series_sql = f"select * from stock_series_null"

    select_sqls = []
    for input_ in full_args['inputs']['stock_ticks']:
        ticks_bq = BigQueryURI(input_['uri'])
        #instance_ts,precision = instance_ts_str_to_ts_precision(input_['instance_ts_str'])
        select_sqls.append( f'select  datetime(timestamp_seconds(t)) ts, * EXCEPT (t)  from `{ticks_bq.path}`' )

    select_sql_joined = ' UNION ALL \n'.join( select_sqls )
    


        
    output_bq = BigQueryURI(args.output)
    
    output_bq.save_sql_results(f"""
with 
params as (select cast("{full_args['schedule_instance_ts']}" as datetime) schedule_instance_ts)
,stock_series_null as (select datetime(timestamp_seconds(t)) ts, * EXCEPT (t) from `{ticks_bq.path}` where False)
,stock_series as ({stock_series_sql})
,stock_series_limited as (select stock_series.* from stock_series cross join params 
where datetime_diff(ts, schedule_instance_ts, day) <= 30) 
,ticks as ({select_sql_joined})

,res as (select * from stock_series 
UNION ALL
select * from ticks)

select * from res
    """)

