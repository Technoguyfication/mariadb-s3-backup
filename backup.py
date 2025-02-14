#!/usr/bin/env python3

import argparse
import subprocess
from threading import Thread
import time
import boto3
import os
from datetime import datetime, timedelta, timezone

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

class MySQLDumpStream:
    def __init__(self, process: subprocess.Popen):
        self.process = process
        self.num_read = 0

    def read(self, size=-1):
        """ Read `size` bytes from mysqldump stdout """
        buffer = self.process.stdout.read(size) if self.process.poll() is None else b''
        self.num_read += len(buffer)
        return buffer

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

    cmd = [
        "mysql",
        f"-u{user}",
        f"-h{hostname}",
        f"-P{port}",
        f"--password={password}" if password else "",
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

def open_dump_process(user: str, password: str, hostname: str, port: int, db_list: list) -> subprocess.Popen:
    if not db_list:
        raise ValueError("No user databases found. Nothing to dump.")

    cmd = [
        "mysqldump",
        f"-u{user}",
        f"-h{hostname}",
        f"-P{port}",
        f"--password={password}" if password else "",
        "--databases"
    ] + db_list

    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    return process

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

    print("Fetching databases...")
    user_databases = get_user_databases(args.user, args.password, args.hostname, args.port)
    if not user_databases:
        print("No non-system databases found. Exiting.")
        return

    print(f"Found databases: {' '.join(user_databases)}")

    timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    dump_filename = f"{args.prefix}-{timestamp_str}.sql"
    mysqldump = open_dump_process(args.user, args.password, args.hostname, args.port, user_databases)

    print("Streaming mysqldump to S3...")

    input_stream = MySQLDumpStream(mysqldump)

    upload_thread = Thread(target=lambda: s3_client.upload_fileobj(input_stream, args.bucket, dump_filename))
    upload_thread.start()

    while True:
        print(f"Uploaded {input_stream.num_read} bytes", end='\r')
        if not upload_thread.is_alive():
            print()
            break
        time.sleep(0.1)

    upload_thread.join()
    mysqldump.wait()
    if mysqldump.returncode != 0:
        raise Exception(f"mysqldump failed: {mysqldump.stderr}")

    print("Upload complete.")

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