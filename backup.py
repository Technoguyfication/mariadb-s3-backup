#!/usr/bin/env python3

import argparse
import subprocess
import boto3
import os
from datetime import datetime, timedelta

DEFAULT_HOSTNAME = "127.0.0.1"
DEFAULT_PORT = 3306
DEFAULT_USER = "root"
DEFAULT_RETENTION_DAYS = 7

# System databases we want to exclude
SYSTEM_DATABASES = {
    "information_schema",
    "performance_schema",
    "mysql",
    "sys"
}

class EnvDefault(argparse.Action):
    def __init__(self, envvar, required=True, default=None, **kwargs):
        if envvar:
            if envvar in os.environ:
                default = os.environ[envvar]
        if required and default:
            required = False
        
        super(EnvDefault, self).__init__(default=default, required=required, 
                                         **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values)

def get_user_databases(user: str, password: str | None, hostname: str, port: int) -> list[str]:
    """
    Return a list of non-builtin databases from MariaDB/MySQL.
    """

    auth_args = [
        f"-u{user}",
        f"-h{hostname}",
        f"-P{port}",
    ]

    if password:
        auth_args.append(f"--password={password}")

    cmd = [
        "mysql",
        *auth_args,
        "-N",            # skip column names
        "-e", "SHOW DATABASES;"
    ]
    result = subprocess.run(cmd, check=False, capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(result.stderr)
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
        description="Dump local MariaDB and upload to an S3-compatible storage, then clean up old backups.\n" +
            "Every commandline argument can be set using the ENVIRONMENT_VARIABLE in its description."
    )
    parser.add_argument("--user", action=EnvDefault, envvar="MYSQL_USER", help="MySQL user name. (MYSQL_USER)")
    parser.add_argument("--password", action=EnvDefault, required=False, envvar="MYSQL_PASSWORD", help="MySQL Password (MYSQL_PASSWORD)")
    parser.add_argument("--hostname", action=EnvDefault, default="127.0.0.1", envvar="MYSQL_HOSTNAME", help="MySQL Hostname (MYSQL_HOSTNAME)")
    parser.add_argument("--port", action=EnvDefault, type=int, default=3306, envvar="MYSQL_PORT", help="MySQL Port (MYSQL_PORT)")
    parser.add_argument("--bucket", action=EnvDefault, envvar="S3_BUCKET", help="S3 bucket name (S3_BUCKET)")
    parser.add_argument("--endpoint-url", action=EnvDefault, envvar="S3_ENDPOINT", help="S3-compatible endpoint URL (S3_ENDPOINT)")
    parser.add_argument("--prefix", action=EnvDefault, envvar="PREFIX", help="Prefix for backup file names (S3_PREFIX)")
    parser.add_argument("--retention-days", action=EnvDefault, envvar="RETENTION_DAYS", type=int, default=DEFAULT_RETENTION_DAYS, help="Number of days to keep backups (RETENTION_DAYS)")

    args = parser.parse_args()

    session = boto3.Session()
    s3_client = session.client("s3", endpoint_url=args.endpoint_url)

    print("Fetching user databases...")
    user_databases = get_user_databases(args.user, args.password, args.hostname, args.port)
    if not user_databases:
        print("No user databases found. Exiting.")
        return

    print(f"Found databases: {' '.join(user_databases)}")

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