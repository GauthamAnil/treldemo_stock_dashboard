import argparse, yaml
from treldev.gcputils import BigQueryURI
        
def main(inputs, outputs, schedule_instance_ts, credentials, **kwargs):

    assert len(inputs) == 2 # Two inputs has to be provided. One to be pushed, and one the latest.
    if len(set(v['uri'] for v in inputs)) == 2: # if they are different, do nothing. This way, we only push the latest.
        return

    input_bq_uri = BigQueryURI(inputs[0]['uri'])
    view_bq_uri = BigQueryURI(outputs[0]['uri'])
    view_bq_uri.save_sql_as_view(f"select * from `{input_bq_uri.path}` -- {inputs[0]['instance_ts_str']}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--_args")
    parser.add_argument("--_creds")
    args, _ = parser.parse_known_args()
    
    with open(args._args) as f:
        args_contents = yaml.safe_load(f)
    
    with open(args._creds) as f:
        credentials = yaml.safe_load(f)

    main(credentials=credentials, **args_contents)
