#!/usr/bin/env python3

import argparse
import subprocess
import boto3
import os
from datetime import datetime, timedelta

DEFAULT_SOCKET = "/var/run/mysqld/mysqld.sock"
DEFAULT_RETENTION_DAYS = 7

# System databases we want to exclude
SYSTEM_DATABASES = {
    "information_schema",
    "performance_schema",
    "mysql",
    "sys"
}

def get_user_databases(user: str, socket: str) -> list:
    """
    Return a list of non-builtin databases from MariaDB/MySQL.
    """
    cmd = [
        "mysql",
        f"-u{user}",
        f"-S{socket}",
        "-N",            # skip column names
        "-e", "SHOW DATABASES;"
    ]
    result = subprocess.run(cmd, check=True, capture_output=True, text=True)
    all_dbs = result.stdout.strip().split("\n")
    # Filter out system databases
    user_dbs = [db for db in all_dbs if db not in SYSTEM_DATABASES]
    return user_dbs

def dump_databases(user: str, socket: str, db_list: list, dump_file: str) -> None:
    """
    Dump the given list of databases into the dump_file using mysqldump.
    """
    if not db_list:
        raise ValueError("No user databases found. Nothing to dump.")

    cmd = [
        "mysqldump",
        f"-u{user}",
        f"-S{socket}",
        "--databases"
    ] + db_list

    with open(dump_file, "w") as f:
        subprocess.run(cmd, check=True, stdout=f)

def get_s3_client(endpoint_url: str,
                   aws_credentials_file: str,
                   aws_profile: str = None):
    """
    Returns an S3 client. We set the AWS_SHARED_CREDENTIALS_FILE env var and
    optionally use a profile from that file.
    """
    os.environ["AWS_SHARED_CREDENTIALS_FILE"] = aws_credentials_file

    if aws_profile:
        session = boto3.session.Session(profile_name=aws_profile)
    else:
        session = boto3.session.Session()

    return session.client("s3", endpoint_url=endpoint_url)

def upload_to_s3(
    file_path: str,
    bucket: str,
    object_name: str,
    s3_client
):
    """
    Upload a file to an S3-compatible server using the provided s3_client.
    """
    s3_client.upload_file(file_path, bucket, object_name)

def cleanup_old_backups(
    bucket: str,
    prefix: str,
    s3_client,
    retention_days: int,
):
    """
    List all objects in the bucket with the given prefix and remove any
    that are older than `retention_days`.
    Expects the key format:  <prefix>-YYYYMMDD-HHMMSS.sql
    We'll parse the date from the middle portion of the filename.
    """
    now = datetime.utcnow()
    cutoff = now - timedelta(days=retention_days)

    continuation_token = None
    while True:
        if continuation_token:
            response = s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                ContinuationToken=continuation_token
            )
        else:
            response = s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix
            )

        if "Contents" not in response:
            break

        for obj in response["Contents"]:
            key = obj["Key"]
            if not key.endswith(".sql"):
                continue

            try:
                base_name = key[:-4]
                ts_str = base_name.rsplit('-', 2)[1] + '-' + base_name.rsplit('-', 2)[2]
                backup_time = datetime.strptime(ts_str, "%Y%m%d-%H%M%S")
            except Exception:
                continue

            if backup_time < cutoff:
                print(f"Deleting old backup: {key}")
                s3_client.delete_object(Bucket=bucket, Key=key)

        if response.get("IsTruncated"):
            continuation_token = response.get("NextContinuationToken")
        else:
            break

def main():
    parser = argparse.ArgumentParser(
        description="Dump local MariaDB and upload to an S3-compatible storage, then clean up old backups."
    )
    parser.add_argument("--socket", default=DEFAULT_SOCKET, help="Path to MariaDB socket.")
    parser.add_argument("--user", default="root", help="MariaDB user for dumping.")
    parser.add_argument("--bucket", required=True, help="S3 bucket name.")
    parser.add_argument("--endpoint-url", required=True, help="S3-compatible endpoint URL.")
    parser.add_argument("--aws-credentials-file", required=True, help="Path to AWS credentials file.")
    parser.add_argument("--aws-profile", help="AWS profile to use within the credentials file.")
    parser.add_argument("--prefix", required=True, help="Prefix for backup file names.")
    parser.add_argument("--retention-days", type=int, default=DEFAULT_RETENTION_DAYS, help="Number of days to keep backups.")
    parser.add_argument("--output-dir", default="/tmp", help="Directory to place the dump file before upload.")

    args = parser.parse_args()

    print("Fetching user databases...")
    user_databases = get_user_databases(args.user, args.socket)
    if not user_databases:
        print("No user databases found. Exiting.")
        return

    timestamp_str = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    dump_filename = f"{args.prefix}-{timestamp_str}.sql"
    dump_filepath = os.path.join(args.output_dir, dump_filename)

    print(f"Dumping databases to {dump_filepath}...")
    dump_databases(args.user, args.socket, user_databases, dump_filepath)

    print("Creating S3 client...")
    s3_client = get_s3_client(
        endpoint_url=args.endpoint_url,
        aws_credentials_file=args.aws_credentials_file,
        aws_profile=args.aws_profile
    )

    print(f"Uploading {dump_filepath} to bucket {args.bucket}...")
    upload_to_s3(
        file_path=dump_filepath,
        bucket=args.bucket,
        object_name=dump_filename,
        s3_client=s3_client
    )
    print("Upload complete.")

    print(f"Removing {dump_filepath}")
    os.remove(dump_filepath)

    print(f"Cleaning up backups older than {args.retention_days} days...")
    cleanup_old_backups(
        bucket=args.bucket,
        prefix=args.prefix,
        s3_client=s3_client,
        retention_days=args.retention_days
    )
    print("Cleanup complete.")

    print("All done.")


if __name__ == "__main__":
    main()