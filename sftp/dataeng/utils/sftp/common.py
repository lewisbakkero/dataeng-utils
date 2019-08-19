#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from stat import S_ISDIR

import paramiko
from paramiko import SSHException

from dataeng.utils.gcs import upload_object_to_gcs


def get_sftp_client(logger, host, port, username, password=None, key_rep=None, key_path=None, key_passphrase=None):
    """
    Gets an SFTP client.

    :param logger: logger
    :param host: SFTP server address
    :param port: SFTP port
    :param username: SFTP username
    :param password: SFTP password
    :param key_rep: Private key representation
    :param key_path: Path to private key
    :param key_passphrase: Private key passphrase

    :type logger: logging.Logger
    :type host: str
    :type port: int
    :type username: str
    :type password: str
    :type key_rep: str
    :type key_path: str
    :type key_passphrase: str

    :returns: SFTP client and associated Transport, or None if failed to create the client
    :rtype: paramiko.sftp_client.SFTPClient, paramiko.transport.Transport
    """
    sftp_client = None
    transport = None
    key = None

    try:
        if key_path is not None:
            if key_rep.upper() == "DSA":
                key_representation = paramiko.DSSKey
            elif key_rep.upper() == "RSA":
                key_representation = paramiko.RSAKey
            else:
                logger.warning("Invalid key representation - {key_rep}".format(key_rep=key_rep))
                return

            if key_passphrase is not None:
                key = key_representation.from_private_key_file(key_path, key_passphrase)
            else:
                key = key_representation.from_private_key_file(key_path)

        transport = paramiko.Transport((host, port))
        transport.connect(None, username, password, key)
        sftp_client = paramiko.SFTPClient.from_transport(transport)

        return sftp_client, transport
    except SSHException as e:
        logger.error(e.__str__())

        if sftp_client is not None:
            sftp_client.close()

        if transport is not None:
            transport.close()


def list_files(logger, sftp_client, sftp_file_path, recursive=False, sort=False):
    """
    Lists files in a directory on an SFTP server.

    :param logger: logger
    :param sftp_client: SFTP client
    :param sftp_file_path: path for directory on SFTP server
    :param recursive: list files recursively
    :param sort: sort the files in natural order

    :type logger: logging.Logger
    :type sftp_client: paramiko.SFTPClient
    :type sftp_file_path: str
    :type recursive: bool
    :type sort: bool

    :returns: list of files or None if failed to list files
    :rtype: list
    """
    try:
        file_list = []

        if not sftp_file_path:
            logger.warning("Empty path is not accepted, return empty list.")
            return file_list

        for path, files in _sftp_walk(sftp_client, sftp_file_path, recursive):
            for f in files:
                file_path = f

                if path != "/":
                    full_path = "{path}/{f}".format(path=path, f=f)
                    file_path = full_path[len(sftp_file_path):]

                file_list.append(file_path)

        if sort:
            file_list.sort()

        logger.debug(file_list)

        if sftp_file_path == "/":
            return file_list
        else:
            sftp_file_path = sftp_file_path[1:]
            return ["{sftp_file_path}{f}".format(sftp_file_path=sftp_file_path, f=f) for f in file_list]
    except IOError as e:
        logger.error(e.__str__())


def _sftp_walk(sftp_client, sftp_path, recursive):
    """

    :param sftp_client: SFTP client
    :param sftp_path: path for directory on SFTP server
    :param recursive: Whether to recursively find the files or not

    :type sftp_client: paramiko.SFTPClient
    :type sftp_path: str
    :type recursive: boolean

    :return: None
    """
    files = []
    directories = []

    for f in sftp_client.listdir_attr(sftp_path):
        if S_ISDIR(f.st_mode):
            if recursive:
                directories.append(f.filename)
        else:
            files.append(f.filename)

    if files:
        yield sftp_path, files

    if recursive:
        for d in directories:
            new_path = os.path.join(sftp_path, d)

            for further in _sftp_walk(sftp_client, new_path, recursive):
                yield further


def close_sftp_connection(logger, sftp_client, transport):
    """
    Closes SFTP client and its underlying Transport.

    :param logger: logger
    :param sftp_client: SFTP client
    :param transport: Transport

    :type logger: logging.Logger
    :type sftp_client: paramiko.sftp_client.SFTPClient
    :type transport: paramiko.transport.Transport
    """
    if sftp_client is not None:
        try:
            sftp_client.close()
        except OSError as e:
            logger.warning(e.__str__())

    if transport is not None:
        transport.close()


def copy_to_gcs(logger, sftp_client, gcs_storage_client, sftp_files, gcs_bucket, gcs_prefix):
    """
    Copies files from SFTP server to GCS.
    
    :param logger: logger
    :param sftp_client: SFTP client
    :param gcs_storage_client: GCS Storage client
    :param sftp_files: list of files to transfer
    :param gcs_bucket: GCS bucket
    :param gcs_prefix: GCS keys
    
    :param logger: logging.Logger
    :param sftp_client: paramiko.sftp_client.SFTPClient
    :param gcs_storage_client: google.cloud.storage.client.Client
    :param sftp_files: list
    :param gcs_bucket: str
    :param gcs_prefix: list
    """
    for index, sftp_file in enumerate(sftp_files):
        with sftp_client.file(sftp_file, "rb") as file_to_transfer:
            logger.info("Transferring file {sftp_file} to gs://{gcs_bucket}/{gcs_prefix}".format(sftp_file=sftp_file,
                                                                                                 gcs_bucket=gcs_bucket,
                                                                                                 gcs_prefix=gcs_prefix[
                                                                                                     index]))
            upload_object_to_gcs(logger, gcs_storage_client, file_to_transfer, gcs_bucket, gcs_prefix[index])


def verify_copy_to_gcs(logger, sftp_client, gcs_storage_client, sftp_file, gcs_bucket, gcs_key):
    """

    :param logger: logger
    :param sftp_client: SFTP client
    :param gcs_storage_client: GCS Storage client
    :param sftp_file: SFTP File
    :param gcs_bucket: GCS bucket
    :param gcs_key: GCS key

    :type logger: logging.Logger
    :type sftp_client : google.cloud.storage.client.Client
    :type gcs_storage_client: google.cloud.storage.client.Client
    :type sftp_file: str
    :type gcs_bucket: str
    :type gcs_key: str

    :return: If the copy to GCS was successful
    :rtype: Boolean
    """
    local_size = sftp_client.stat(sftp_file).st_size
    bucket = gcs_storage_client.get_bucket(gcs_bucket)
    blob = bucket.get_blob(gcs_key)
    remote_size = blob.size
    logger.debug(
        "Local file size is {local_size}, remote GCS object size is {remote_size}.".format(local_size=local_size,
                                                                                           remote_size=remote_size))
    return local_size == remote_size


def rename_file(logger, sftp_client, original_file, renamed_file):
    """
    Renames files on SFTP server.

    :param sftp_client: SFTP client
    :param original_file: original file names
    :param renamed_file: renamed file names

    :type sftp_client: paramiko.sftp_client.SFTPClient
    :type original_file: list
    :type renamed_file: list
    """
    try:
        mkdir_p(sftp_client, os.path.split(renamed_file.rstrip("/"))[0])
        sftp_client.chdir("/")
        sftp_client.rename(original_file, renamed_file)
    except (IOError, OSError) as e:
        logger.warning(
            "Failed to rename {original} to {renamed} - {error}".format(original=original_file,
                                                                        renamed=renamed_file,
                                                                        error=e.__str__()))


def mkdir_p(sftp_client, sftp_dir):
    """
    Makes directory recursively on SFTP server.

    :param sftp_client: SFTP client
    :param sftp_dir: SFTP directory to be created

    :type sftp_client: paramiko.sftp_client.SFTPClient
    :type sftp_dir: str
    """
    if sftp_dir == "/":
        sftp_client.chdir("/")
        return

    if sftp_dir == "":
        return

    try:
        sftp_client.chdir(sftp_dir)
    except IOError:
        dir, subdir = os.path.split(sftp_dir.rstrip("/"))
        mkdir_p(sftp_client, dir)
        sftp_client.mkdir(subdir)
        sftp_client.chdir(subdir)

        return True
