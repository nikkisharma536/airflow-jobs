from urllib.parse import urlparse
import boto3


def split_s3_path(s3_path):
    o = urlparse(s3_path.strip())
    print(o)
    return o.netloc, o.path[1:]


def copy_to_s3(dest_path, log_files):
    bucket, key = split_s3_path(dest_path)
    print('bucket : %s and path : %s ' % (bucket, key))
    s3 = boto3.resource('s3')
    bkt = s3.Bucket(bucket)

    for log in log_files:
        filename = log.split("/")[-1]
        print('Copying file %s to S3 loaction s3://%s' % (log, bucket + '/' + key + filename))
        bkt.upload_file(log, key + filename)


# Create a file on S3 by passing a string and s3 path
def upload_file(content, s3_path):
    client = boto3.client('s3')
    bucket, key = split_s3_path(s3_path)
    client.put_object(Body=content, Bucket=bucket, Key=key)


# Delete File from s3 path
def delete_file(s3_path):
    client = boto3.client('s3')
    bucket, key = split_s3_path(s3_path)
    client.delete_object(Bucket=bucket, Key=key)

