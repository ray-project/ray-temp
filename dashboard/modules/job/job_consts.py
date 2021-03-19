import os

# Job agent consts
JOB_DIR = "{temp_dir}/job/{job_id}/"
JOB_UNPACK_DIR = os.path.join(JOB_DIR, "package")
JOB_DRIVER_ENTRY_FILE = os.path.join(JOB_DIR, "driver-{uuid}.py")
# Downloader constants
DOWNLOAD_BUFFER_SIZE = 10 * 1024 * 1024  # 10MB
DOWNLOAD_PACKAGE_FILE = os.path.join(JOB_DIR, "package.zip")
# Redis key
REDIS_KEY_JOB_COUNTER = "JobCounter"
JOB_CHANNEL = "JOB"
RETRY_GET_ALL_JOB_INFO_INTERVAL_SECONDS = 2
