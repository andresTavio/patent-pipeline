from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook


class LocalToS3Operator(BaseOperator):
    """Loads a file from a local directory to S3.

    Attributes:
        s3_conn_id: string, connection ID to S3
        s3_bucket: string, name of bucket to upload to
        s3_key: string, key of the file to upload
        local_file_path: string, path to the file to upload
        replace: boolean, if True then allow an overwrite of an existing key
    """

    template_fields = ['s3_key', 'local_file_path']

    @apply_defaults
    def __init__(self,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 local_file_path,
                 replace=False,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.local_file_path = local_file_path
        self.replace = replace

    def execute(self, context):
        print('Uploading {} to {}/{}'.format(self.local_file_path, self.s3_bucket, self.s3_key))

        hook = S3Hook(self.s3_conn_id)

        hook.load_file(self.local_file_path,
                       self.s3_key,
                       self.s3_bucket,
                       replace=self.replace)

        print('Upload successful')
