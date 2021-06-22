from contextlib import asynccontextmanager

import aioboto3

from config.app_config import NUM_DOCUMENTS_LIMIT, MAX_RECORD_SIZE_BYTES
from config.credentials import AWS_SETTINGS
from src.utils.benchmark import timeit
from src.utils.log import getLogger


class AWS3Client:
    def __init__(self,
                 AWS_SECRET_ACCESS_KEY=AWS_SETTINGS['Access Key'],
                 AWS_ACCESS_KEY_ID=AWS_SETTINGS['Access ID'],
                 BUCKET_NAME=AWS_SETTINGS['Bucket name']
                 ):
        self.s3 = None
        self.AWS_SECRET_ACCESS_KEY = AWS_SECRET_ACCESS_KEY
        self.AWS_ACCESS_KEY_ID = AWS_ACCESS_KEY_ID
        self.BUCKET_NAME = BUCKET_NAME
        self.log = getLogger('[AWS]')

    @asynccontextmanager
    async def s3_client(self):
        self.log.info(f'connecting to S3 bucket "{self.BUCKET_NAME}"')
        async with aioboto3.client('s3',
                                   aws_access_key_id=self.AWS_ACCESS_KEY_ID,
                                   aws_secret_access_key=self.AWS_SECRET_ACCESS_KEY,
                                   region_name='eu-west-1') as s3:
            self.s3 = s3
            self.log.info(f'connected to S3 bucket "{self.BUCKET_NAME}"')
            try:
                yield self.s3
            finally:
                self.log.info(f'closed connection to S3 bucket "{self.BUCKET_NAME}"')
                await self.s3.close()

    async def list_docs(self, limit=NUM_DOCUMENTS_LIMIT):
        bucket_contents = await self.s3.list_objects_v2(Bucket=self.BUCKET_NAME)
        return [obj['Key'] for obj in bucket_contents['Contents'][:limit]]

    async def get_file(self, filename):
        with timeit(f'{filename} S3 download'):
            s3_response = await self.s3.get_object(
                Bucket=self.BUCKET_NAME,
                Key=filename
            )
            byte_arrays = []
            data = await s3_response['Body'].read(MAX_RECORD_SIZE_BYTES)
            while data:
                byte_arrays.append(data)
                data = await s3_response['Body'].read(MAX_RECORD_SIZE_BYTES)
            file_data = b"".join(byte_arrays)
            return file_data
