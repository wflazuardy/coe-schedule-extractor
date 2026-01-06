# COE Bidding Schedule Extractor

This tool extracts Certificate of Entitlement (COE) bidding schedules from PDF documents using AI (OpenAI GPT-4o or Google Gemini 2.0 Flash) and loads the data into Amazon Redshift via S3.

## Prerequisites

-   **Python 3.13** or higher
-   **uv** (Python package manager)
-   An **OpenAI API Key** or **Google Gemini API Key**
-   (Optional) **AWS Credentials** and **Redshift Connection Details**
-   (Optional) **SSH Bastion Details** (if Redshift is not public)

## Setup

1.  **Clone the repository** (if you haven't already).

2.  **Install dependencies**:
    ```bash
    uv sync
    ```

3.  **Configure Environment Variables**:
    Create a `.env` file in the root directory using `.env.example` as a template:
    ```bash
    # AI Provider Keys
    OPENAI_API_KEY=your_openai_key
    GEMINI_API_KEY=your_gemini_key

    # AWS S3 Config (Required for S3 upload)
    AWS_ACCESS_KEY_ID=your_access_key
    AWS_SECRET_ACCESS_KEY=your_secret_key
    AWS_REGION=ap-southeast-1
    S3_BUCKET_NAME=your_bucket_name

    # Redshift Config (Required for Redshift load)
    REDSHIFT_HOST=your_redshift_cluster_endpoint
    REDSHIFT_PORT=5439
    REDSHIFT_DB=your_db_name
    REDSHIFT_USER=your_db_user
    REDSHIFT_PASSWORD=your_db_password
    REDSHIFT_IAM_ROLE=arn:aws:iam::123456789012:role/MyRedshiftRole
    REDSHIFT_TABLE=public.coe_bidding_schedule  # Optional, defaults to public.coe_bidding_schedule

    # SSH Tunnel Config (Required if connecting via bastion)
    SSH_HOST=your_bastion_host_ip
    SSH_PORT=22
    SSH_USER=your_ssh_username
    SSH_KEY_PATH=/path/to/your/key.pem
    ```

## Usage

The process is split into two steps: Extraction and Loading.

### Step 1: Extraction
Run the `extract_schedule.py` script to extract data from the PDF and save it locally as a JSONL file.

**Basic Usage (Defaults to Gemini):**
```bash
uv run extract_schedule.py data/schedule_pdf/COE_Bidding_Schedule_for_Year_2025.pdf
```

**Using OpenAI:**
```bash
uv run extract_schedule.py data/schedule_pdf/COE_Bidding_Schedule_for_Year_2025.pdf --provider openai
```

**Output:**
This will generate a file like `data/COE_Bidding_Schedule_2025.jsonl`.

### Step 2: Load to S3 and Redshift
Run the `load_schedule.py` script to upload the generated JSONL file to S3 and load it into Redshift.

```bash
uv run load_schedule.py data/COE_Bidding_Schedule_2025.jsonl
```

**Options:**
-   `--mode` / `-m`: Load mode (default: `append`)
    - `append`: Simply adds new data to the table (default behavior)
    - `replace`: Deletes existing rows for the same year before loading (prevents duplicates)
-   `--no-upload-s3`: Skip S3 upload.
-   `--no-load-redshift`: Skip Redshift load.

**Example: Adding 2026 data without affecting 2025:**
```bash
uv run load_schedule.py data/COE_Bidding_Schedule_2026.jsonl --mode append
```

**Example: Replacing 2026 data (deletes existing 2026 rows first):**
```bash
uv run load_schedule.py data/COE_Bidding_Schedule_2026.jsonl --mode replace
```

**SSH Tunneling**:
If `SSH_HOST`, `SSH_USER`, and `SSH_KEY_PATH` are set in your `.env` file, the script will automatically establish an SSH tunnel to the bastion host and forward the Redshift connection through it.

## Data Flow

1.  **Extraction (`extract_schedule.py`)**:
    -   PDF is converted to images.
    -   AI model extracts schedule data.
    -   Data is saved locally to `data/COE_Bidding_Schedule_{YEAR}.jsonl`.

2.  **Loading (`load_schedule.py`)**:
    -   **S3 Upload**: File is uploaded to `s3://{S3_BUCKET_NAME}/data/coe_bidding_schedule/COE_Bidding_Schedule_{YEAR}.jsonl`.
    -   **Redshift Load**:
        -   (Optional) Opens SSH Tunnel to bastion.
        -   Creates table `public.coe_bidding_schedule` if it doesn't exist.
        -   Executes `COPY` command to load data from S3 into the table.

## Redshift Table Schema
```sql
CREATE TABLE public.coe_bidding_schedule (
    month VARCHAR(255),
    exercise_start_datetime TIMESTAMPTZ,
    exercise_end_datetime TIMESTAMPTZ
);
```
