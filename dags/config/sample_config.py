from datetime import datetime
import pathlib

DIR_BASE = pathlib.Path().cwd()

CURRENT_DATE = datetime.now().strftime('%Y-%m-%d')
EXECUTION_DATE = '{{ next_ds }}'

S3_BUCKET_RAW_DATA = 'your-raw-bucket'
S3_BUCKET_TRANSFORMED_DATA = 'your-transform-bucket'
S3_BUCKET_SCRIPTS = 'your-scripts-bucket'

LOCAL_FILE_PATH_RAW_DATA = DIR_BASE.joinpath('files/patents').resolve()
LOCAL_FILE_PATH_PATENT_QUERY = DIR_BASE.joinpath('files/patents_query.json').resolve()
LOCAL_FILE_PATH_SPARK_SCRIPTS = DIR_BASE.joinpath('spark/scripts').resolve()
