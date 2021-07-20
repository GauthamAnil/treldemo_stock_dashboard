import os, re, datetime, requests, yaml, subprocess, tempfile, argparse, json, sys
import unittest
from treldev import gcputils, S3Commands

def parse_and_load(tweets, tweets_condensed):
    with open("credentials.yml") as f:
        credentials = yaml.safe_load(f)
    s3_commands = S3Commands(credentials=credentials)
    folder = tempfile.mkdtemp()
    s3_commands.download_folder(tweets, folder)
    
    entries = {}
    with open(os.path.join(folder,'part-00000')) as f:
        for line in f:
            d = json.loads(line)
            e = {'id':str(d['id']),
                 'created_at':str(datetime.datetime.strptime(d['created_at'],'%a %b %d %H:%M:%S +0000 %Y')),
                 'user_id': str(d['user']['id']),
                 'text':d.get('text'),
                 'truncated':d['truncated'],
                 'verified': d['user']['verified'],
            }
            entries[e['id']] = e
    
    output_file = tempfile.mkstemp()[1]
    print("Output file", output_file)
    with open(output_file,'w') as f:
        for entry in entries.values():
            json.dump(entry,f)
            f.write("\n")
    
    bquri = gcputils.BigQueryURI(tweets_condensed) # wraps credential management and improves readability
    from google.cloud import bigquery
    schema = [
        bigquery.SchemaField("id","string", mode="REQUIRED"),
        bigquery.SchemaField("created_at","datetime", mode="REQUIRED"),
        bigquery.SchemaField("user_id","string", mode="NULLABLE"),
        bigquery.SchemaField("text","string", mode="NULLABLE"),
        bigquery.SchemaField("truncated","boolean", mode="NULLABLE"),
        bigquery.SchemaField("verified","boolean", mode="NULLABLE"),
    ]
    bquri.load_file(output_file, {"source_format":bigquery.job.SourceFormat.NEWLINE_DELIMITED_JSON,
                                               "schema":schema})
    assert folder.startswith("/tmp/") # to avoid accidentally deleting something important
    subprocess.check_call(['rm','-rf', folder, output_file])
    
def parse():
    parser = argparse.ArgumentParser(prog="Twitter transformer and loader")
    parser.add_argument("--tweets")
    parser.add_argument("--tweets_condensed")
    args, _ = parser.parse_known_args()
    return args

if __name__ == '__main__':
    args = parse()
    parse_and_load(**args.__dict__)

