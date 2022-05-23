import argparse, yaml
from treldev.gcputils import BigQueryURI
from treldev import get_args
        
def main(inputs, outputs, schedule_instance_ts, credentials, **kwargs):

    assert len(inputs) == 1 # Only one input dataset class can be provided.
    input_datasets = list(inputs.values())[0]
    assert len(input_datasets) == 2 # This should have only two datasets. One to be pushed, and one the latest.
    if len(set(v['uri'] for v in input_datasets)) == 2: # if they are different, do nothing. This way, we only push the latest.
        print("Not pushing path to view")
        return

    input_bq_uri = BigQueryURI(input_datasets[0]['uri'])
    view_bq_uri = BigQueryURI(list(outputs.values())[0][0]['uri'])
    print(f"Pushing path {input_bq_uri.path} to view {view_bq_uri.path}")
    view_bq_uri.save_sql_as_view(f"select * from `{input_bq_uri.path}` -- {input_datasets[0]['instance_ts_str']}")

if __name__ == '__main__':
    main(**get_args())
