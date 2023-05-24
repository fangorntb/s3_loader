"""boto3 file system module"""

import os
import re
from contextlib import suppress
from functools import wraps
from multiprocessing.pool import ThreadPool
from pathlib import Path
from typing import List

from boto3 import Session
from botocore.config import Config


def close(func):
    @wraps(func)
    def _(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except (KeyboardInterrupt, SystemExit):
            map(
                lambda x: x.close() if not isinstance(x, Session) else None,
                self.session_pool.session_pool.sessions.values()
            )
            exit()

    return _


def iterate(iterator):
    while True:
        try:
            next(iterator)
        except StopIteration:
            break


def get_files_names(path: str | Path):
    for _dir, _, filenames in os.walk(path):
        for f in filenames:
            yield os.path.abspath(os.path.join(_dir, f))


def remove_starting_strings(lst: list[str]):
    return filter(
        lambda x: x is not None, (i if not any(re.match(f'^{i}', s) for s in lst if s != i) else None for i in lst)
    )


class S3SessionPoolFactory:
    sessions = dict()

    def __new__(
            cls,
            access_key_id: str = None,
            secret_access_key: str = None,
            aws_session_token: str = None,
            region_name: str = None,
            botocore_session: str = None,
            profile_name: str = None,
    ):
        if cls.sessions.get(access_key_id) is None:
            cls.sessions[access_key_id] = Session(
                access_key_id,
                secret_access_key,
                aws_session_token,
                region_name,
                botocore_session,
                profile_name,
            )
            cls.__del__ = lambda x: cls.sessions.pop(x, None)
        return cls


class S3SessionPool:
    service_name = 's3'

    def __init__(
            self,
            access_key_id: str = None,
            secret_access_key: str = None,
            aws_session_token: str = None,
            region_name: str = None,
            botocore_session: str = None,
            profile_name: str = None,
    ):
        self.session_pool = S3SessionPoolFactory(
            access_key_id,
            secret_access_key,
            aws_session_token,
            region_name,
            botocore_session,
            profile_name,
        )

    def __setitem__(self, access_key_id: str, config: dict):
        if isinstance(self.session_pool.sessions.get(access_key_id), Session):
            self.session_pool.sessions[access_key_id] = self.session_pool.sessions.get(access_key_id).resource(
                service_name=self.service_name,
                config=Config(retries={'max_attempts': 100}),
                **config,
            )

    def __getitem__(self, access_key_id: str):
        return self.session_pool.sessions[access_key_id]


class S3:
    def __init__(self, session_pool: S3SessionPool, access_key_id: str, threads: int, **config):
        self.session_pool = session_pool
        self.session_pool[access_key_id] = config
        self.threadpool = ThreadPool(threads)

    @close
    def list_dirs(
            self,
            access_key_id: str,
            bucket: str | bytes,
            prefix: str = '',
    ):
        """get list of files"""
        bucket = self.session_pool[access_key_id].Bucket(bucket)
        return list(map(lambda x: x.key, bucket.objects.filter(Prefix=prefix).all()))

    @staticmethod
    def normalize_path(_s: str) -> str:
        return (_s[0].replace('.', 'data') + _s[1:].replace('\\', '/')) if _s else ''

    @close
    def upload(
            self,
            access_key_id: str,
            bucket: str | bytes,
            local_path: str,
            s3_path: str = None,
            existing_s3_paths: List[str] = None,
    ):
        """upload single file"""
        obj = self.session_pool[access_key_id].Object(bucket, key=s3_path)
        if existing_s3_paths is None:
            existing_s3_paths = []
        if s3_path is None:
            s3_path = local_path.replace('.', 'data').replace('\\', '/')
        s3_path = s3_path.lstrip('/')
        if s3_path not in existing_s3_paths:
            with open(Path(local_path), 'rb') as f:
                obj.put(Body=f.read(), )

    @close
    def upload_from_bytes(
            self,
            access_key_id: str,
            bucket: str | bytes,
            _bytes: bytes,
            s3_path: str,
    ):
        """upload single bytes array"""
        return self.session_pool[access_key_id].put_object(
            Bucket=bucket,
            Key=s3_path,
            Body=_bytes,
        )

    @close
    def download(
            self,
            access_key_id: str,
            bucket: str | bytes,
            local_path: str,
            s3_path: str = None,
            update: bool = True,
    ):
        """download single file"""
        Path('/'.join(local_path.split('/')[:-1])).mkdir(exist_ok=True, parents=True)
        with suppress(IsADirectoryError):
            if not Path(local_path).exists() or update:
                with open(local_path, 'wb') as f:
                    obj = self.session_pool[access_key_id].Object(bucket, s3_path)
                    body = obj.get().get('Body')
                    if body:
                        f.write(body.read())

    @close
    def download_files_list(
            self,
            access_key_id: str,
            bucket: str,
            local_paths: List[str],
            s3_paths: List[str],
            update: bool = True,
    ):
        """download list of files"""
        args = map(lambda x: [access_key_id] + [bucket] + list(x) + [update], zip(local_paths, s3_paths))
        downloader = lambda x: self.download(*x)
        iterate(self.threadpool.imap_unordered(downloader, args))

    @close
    def download_directory(
            self,
            access_key_id: str,
            bucket: str,
            local_directory: str,
            s3_directory: str = '',
            update: bool = True,
            count: int = None,
    ):
        """download dir from s3"""
        s3_paths = self.list_dirs(access_key_id, bucket, s3_directory)
        if not local_directory.endswith('/'):
            local_directory += '/'
        if count is None:
            count = len(s3_paths)
        local_paths = list(
            map(lambda x: str(Path(local_directory).joinpath(x)).replace(s3_directory, ''), s3_paths)
        )[:count]
        self.download_files_list(access_key_id, bucket, local_paths, s3_paths, update)

    @close
    def read(
            self,
            access_key_id: str,
            bucket: str | bytes,
            key: str
    ):
        """read bytes data from s3"""
        return self.session_pool[access_key_id].get_object(
            Bucket=bucket,
            Key=key,
        ).get('Body').read()

    @close
    def read_dir(
            self,
            access_key_id: str,
            bucket: str | bytes,
            prefix: str,
    ) -> List[bytes]:
        """read dir to list"""
        _ = []
        for o in frozenset(self.list_dirs(access_key_id, bucket, prefix)):
            key = o.get('Key')
            _.append(
                (key, self.session_pool[access_key_id].get_object(
                    Bucket=bucket,
                    Key=key,
                ).read())
            )
        return _

    @close
    def upload_files_list(
            self,
            access_key_id: str,
            bucket: str | bytes,
            local_files: List[str],
            s3_paths: List[str] = None,
            update: bool = True,
    ):
        """upload list of files to s3"""
        existing_s3_paths = []
        if s3_paths is None:
            s3_paths = local_files
        if not update:
            existing_s3_paths = self.list_dirs(access_key_id, bucket, '')
        uploader = lambda x: self.upload(*x)
        args = map(
            lambda x: [access_key_id] + [bucket] + list(x) + [existing_s3_paths, ],
            zip(list(map(Path, local_files)), s3_paths)
        )
        iterate(self.threadpool.imap_unordered(uploader, args))

    @close
    def upload_directory(
            self,
            access_key_id: str,
            bucket: str,
            local_directory: str,
            s3_directory: str = '',
            update: bool = True,
            count: int = None,
    ):
        """upload directory to s3"""
        local_files = list(get_files_names(local_directory))
        if count is None:
            count = len(local_files)
        s3_path = lambda x: str(
            self.normalize_path(s3_directory) + x.replace(
                self.normalize_path(f'{Path(local_directory).absolute()}'),
                '')
        )
        self.upload_files_list(
            access_key_id,
            bucket,
            local_files=local_files,
            s3_paths=list(map(s3_path, local_files))[:count],
            update=update,
        )

    def __del__(self):
        self.threadpool.close()
        del self
