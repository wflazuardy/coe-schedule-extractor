import os
import re
from enum import Enum
from typing import Optional

import boto3
import psycopg2
import typer
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder

# Load environment variables
load_dotenv()

app = typer.Typer()


class LoadMode(str, Enum):
    APPEND = "append"
    REPLACE = "replace"


def extract_year_from_filename(file_path: str) -> Optional[int]:
    """Extract year from a COE schedule filename.

    Parses the filename to find a 4-digit year pattern.

    Args:
        file_path: Path to the file (can be full path or just filename).

    Returns:
        The extracted year as an integer, or None if no year found.

    Examples:
        >>> extract_year_from_filename("COE_Bidding_Schedule_2025.jsonl")
        2025
        >>> extract_year_from_filename("data/schedule.jsonl")
        None
    """
    filename = os.path.basename(file_path)
    match = re.search(r'(\d{4})', filename)
    if match:
        return int(match.group(1))
    return None


def upload_to_s3(file_path: str) -> Optional[str]:
    """Upload a file to S3.

    Uploads the specified file to the configured S3 bucket under
    the 'data/coe_bidding_schedule/' prefix.

    Args:
        file_path: Local path to the file to upload.

    Returns:
        The S3 URI (s3://bucket/key) if successful, None otherwise.
    """
    bucket_name = os.getenv("S3_BUCKET_NAME")
    if not bucket_name:
        print("Error: S3_BUCKET_NAME not set in environment variables.")
        return None

    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION")
    )

    file_name = os.path.basename(file_path)
    object_name = f"data/coe_bidding_schedule/{file_name}"

    try:
        print(f"Uploading {file_path} to s3://{bucket_name}/{object_name}...")
        s3_client.upload_file(file_path, bucket_name, object_name)
        return f"s3://{bucket_name}/{object_name}"
    except Exception as e:
        print(f"Error uploading to S3: {e}")
        return None


def _get_redshift_connection(
    host: str, port: int, dbname: str, user: str, password: str
) -> psycopg2.extensions.connection:
    """Create a psycopg2 connection to Redshift.

    Args:
        host: Redshift cluster endpoint hostname.
        port: Redshift cluster port.
        dbname: Database name.
        user: Database username.
        password: Database password.

    Returns:
        A psycopg2 connection object.
    """
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )


def load_to_redshift(
    s3_path: str, mode: LoadMode = LoadMode.APPEND, year: Optional[int] = None
) -> None:
    """Load data from S3 into Redshift.

    Connects to Redshift (optionally via SSH tunnel) and loads JSONL data
    from S3 using the COPY command.

    Args:
        s3_path: S3 URI to the JSONL file (e.g., 's3://bucket/path/file.jsonl').
        mode: Load mode - APPEND adds data, REPLACE deletes existing year's
            data before loading.
        year: Year to replace when mode is REPLACE. Required for REPLACE mode.
    """
    host = os.getenv("REDSHIFT_HOST")
    port = int(os.getenv("REDSHIFT_PORT", "5439"))
    dbname = os.getenv("REDSHIFT_DB")
    user = os.getenv("REDSHIFT_USER")
    password = os.getenv("REDSHIFT_PASSWORD")
    iam_role = os.getenv("REDSHIFT_IAM_ROLE")
    table = os.getenv("REDSHIFT_TABLE")

    # SSH Config
    ssh_host = os.getenv("SSH_HOST")
    ssh_user = os.getenv("SSH_USER")
    ssh_key_path = os.getenv("SSH_KEY_PATH")
    ssh_port = int(os.getenv("SSH_PORT", "22"))

    if not all([host, dbname, user, password, iam_role]):
        print("Error: Redshift configuration missing in environment variables.")
        return

    conn = None
    server = None

    try:
        # Check if SSH tunnel is needed
        if ssh_host and ssh_user and ssh_key_path:
            print(f"Establishing SSH tunnel to {ssh_host}...")
            server = SSHTunnelForwarder(
                (ssh_host, ssh_port),
                ssh_username=ssh_user,
                ssh_pkey=ssh_key_path,
                remote_bind_address=(host, port)
            )
            server.start()
            print(f"SSH tunnel established. Local bind port: {server.local_bind_port}")
            
            # Connect using local forwarded port
            conn = _get_redshift_connection(
                host="127.0.0.1",
                port=server.local_bind_port,
                dbname=dbname,
                user=user,
                password=password
            )
        else:
            print("Connecting directly to Redshift...")
            conn = _get_redshift_connection(
                host=host,
                port=port,
                dbname=dbname,
                user=user,
                password=password
            )

        cur = conn.cursor()

        # 1. Create table if not exists
        print(f"Creating table {table} if not exists...")
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            month VARCHAR(255),
            exercise_start_datetime TIMESTAMPTZ,
            exercise_end_datetime TIMESTAMPTZ
        );
        """
        cur.execute(create_table_query)

        # 2. Delete existing data if in replace mode
        if mode == LoadMode.REPLACE:
            if year:
                print(f"Deleting existing data for year {year}...")
                delete_query = f"""
                DELETE FROM {table}
                WHERE month LIKE '%{year}%';
                """
                cur.execute(delete_query)
                print(f"Deleted rows matching year {year}.")
            else:
                print("Warning: Replace mode specified but no year provided. Skipping delete.")

        # 3. Copy data from S3
        print(f"Copying data from {s3_path} to Redshift...")
        copy_query = f"""
        COPY {table}
        FROM '{s3_path}'
        IAM_ROLE '{iam_role}'
        FORMAT AS JSON 'auto'
        TIMEFORMAT 'auto';
        """
        cur.execute(copy_query)
        
        conn.commit()
        print("Redshift load completed successfully.")
        
        cur.close()

    except Exception as e:
        print(f"Error loading to Redshift: {e}")
    finally:
        if conn:
            conn.close()
        if server:
            print("Closing SSH tunnel...")
            server.stop()

@app.command()
def main(
    file_path: str = typer.Argument(..., help="Path to the JSONL file to load"),
    upload_s3: bool = typer.Option(True, "--upload-s3/--no-upload-s3", help="Upload to S3"),
    load_redshift: bool = typer.Option(True, "--load-redshift/--no-load-redshift", help="Load to Redshift"),
    mode: LoadMode = typer.Option(LoadMode.APPEND, "--mode", "-m", help="Load mode: 'append' adds data, 'replace' deletes existing year's data first")
):
    """
    Upload COE bidding schedule JSONL to S3 and load into Redshift.
    """
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"Error: File not found: {file_path}")
        return

    # Extract year from filename for replace mode
    year = extract_year_from_filename(file_path)
    if mode == LoadMode.REPLACE and not year:
        print("Warning: Could not extract year from filename. Replace mode may not work correctly.")

    s3_path = None
    if upload_s3:
        s3_path = upload_to_s3(file_path)
    
    if load_redshift and s3_path:
        load_to_redshift(s3_path, mode=mode, year=year)
    elif load_redshift and not s3_path:
        print("Skipping Redshift load because S3 upload failed or was skipped.")

if __name__ == "__main__":
    app()
