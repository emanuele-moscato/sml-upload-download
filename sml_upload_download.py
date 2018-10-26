#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Upload from local machine into SherlockML Datasets via `sherlockml.filesystem`.
"""

import glob
import os
import time
import logging
import pickle
import click

import sherlockml.filesystem as sfs
# from slacker import Slacker
from tenacity import *


PROJECT_ID = "65a8472a-16e9-4cdc-a023-8e3ca7a19c89"
LOCAL_DIR = os.path.expanduser("~/Documents/sml-upload-download/")
REMOTE_DIR = "/output/"
ACTION = "upload"  # Either "upload", "download" or "check_download_dirs"

SLACK_TOKEN = ''

WAIT_TIME = 10*60  # seconds
MAX_RETRIES = 6


def post(message):
    slack = Slacker(SLACK_TOKEN)
    slack.chat.post_message('#eti-upload', message)


def create_local_dir_tree(output_dirs):
    """
    Given a list of directories and subdirectories, creates a local (in the
    directory where the script is run) tree of (empty) directories that
    reproduces it.
    """
    for dirpath in output_dirs:
        if os.path.isdir("."+dirpath):
            pass
        else:
            print("Creating directory", dirpath)
            os.makedirs("."+dirpath)


def initialise_logger(logger_name):
    """
    Creates a logging.logger objects that logs stout and stderr.
    """
    logger = logging.getLogger('logger_name')
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(logger_name+'.log')
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


@retry(stop=stop_after_attempt(5))
def download_file(filepath, project_id):
    """
    Wrapper around the sfs.get() method. Allows for the use of retry.
    """
    sfs.get(filepath, "."+filepath, project_id)


@retry(stop=stop_after_attempt(5))
def upload_file(filepath, project_id):
    """
    Wrapper around the sfs.get() method. Allows for the use of retry.
    """
    sfs.put(filepath, "."+filepath, project_id)


class Uploader(object):
    def __init__(self):
        self.num_retries = 0

    def upload(self,
               project_id,
               local_file,
               remote_dir,
               logger,
               uploaded_files,
               failed_uploads):
        remote_file = os.path.join(remote_dir, os.path.basename(local_file))
        # post('Uploading {} ...'.format(local_file))
        print(f"Uploading file: {local_file}")
        logger.info(f"Uploading file: {local_file}")

        try:
            sfs.put(local_file, remote_file, project_id)
            uploaded_files.append(local_file)
            print(f"Uploaded file: {remote_file}")
            logger.info(f"Uploaded file: {remote_file}")
        except:
            # post('Something went wrong, trying again ...')
            self.num_retries += 1
            if self.num_retries < MAX_RETRIES:
                time.sleep(WAIT_TIME)
                self.upload(local_file, logger)
            else:
                # post('Maximum retries reached :( moving on ...')
                print(f"Failed to upload file: {local_file}")
                logger.info(f"Failed to upload file: {local_file}")
                failed_uploads.append(local_file)


class Downloader(object):
    def __init__(self):
        pass

    def download(self, LOCAL_DIR, REMOTE_DIR, logger):
        pass


def upload(project_id, local_dir, remote_dir):
    os.environ["SHERLOCKML_DOMAIN"] = 'services.sherlockml.com'
    os.environ["SHERLOCK_PROJECT_ID"] = project_id

    logger = initialise_logger("sml_upload_output")

    print('Starting upload session! Good luck! :)')
    logger.info('Starting upload session! Good luck! :)')

    local_files = glob.glob(os.path.join(local_dir, "*"))

    print(f"There are {len(local_files)} files in total ...")
    logger.info(f"There are {len(local_files)} files in total ...")

    uploaded_files = []
    failed_uploads = []

    for local_file in local_files:
        uploader = Uploader()
        uploader.upload(
            project_id,
            local_file,
            remote_dir,
            logger,
            uploaded_files,
            failed_uploads
        )

    with open("uploaded_files.pkl", "wb") as f:
        pickle.dump(uploaded_files, f)

    with open("failed_uploads.pkl", "wb") as f:
        pickle.dump(failed_uploads, f)

    print(f"Uploaded files: {len(uploaded_files)}. Failed uploads: {len(failed_uploads)}.")
    logger.info(f"Uploaded files: {len(uploaded_files)}. Failed uploads: {len(failed_uploads)}.")


@click.command()
@click.option("--action", default='', help="Upload or download")
@click.option("--project-id", default='', help="SherlockML project ID")
@click.option("--local-dir", default='', help="Local directory")
@click.option("--remote-dir", default='', help="Remote directory")
def sml_upload_download(action, project_id, local_dir, remote_dir):
    local_dir = os.path.expanduser(local_dir)

    if action == "upload":
        upload(project_id, local_dir, remote_dir)

    elif action == "download":
        print("Download not implemented yet!")

    else:
        print(f"Action should be either 'upload' or 'download'. Currently: action={action}")


def main():
    # Needed since the migration of SherlockML cloud
    os.environ["SHERLOCKML_DOMAIN"] = 'services.sherlockml.com'
    os.environ["SHERLOCK_PROJECT_ID"] = PROJECT_ID

    if ACTION == "upload":
        logger = logging.getLogger('sml_upload_output')
        logger.setLevel(logging.DEBUG)

        fh = logging.FileHandler('sml_upload_output.log')
        fh.setLevel(logging.DEBUG)

        ch = logging.StreamHandler()
        ch.setLevel(logging.ERROR)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        logger.addHandler(fh)
        logger.addHandler(ch)

        print('Starting upload session! Good luck! :)')
        logger.info('Starting upload session! Good luck! :)')
        # post('Starting upload session! Good luck! :)')

        local_files = glob.glob(os.path.join(LOCAL_DIR, "*"))

        print(f"There are {len(local_files)} files in total ...")
        logger.info(f"There are {len(local_files)} files in total ...")
        # post(f"There are {len(local_files)} files in total ...")

        uploaded_files = []
        failed_uploads = []

        for local_file in local_files:
            uploader = Uploader()
            uploader.upload(
                PROJECT_ID,
                local_file,
                REMOTE_DIR,
                logger,
                uploaded_files,
                failed_uploads
            )

        with open("uploaded_files.pkl", "wb") as f:
            pickle.dump(uploaded_files, f)

        with open("failed_uploads.pkl", "wb") as f:
            pickle.dump(failed_uploads, f)

        # post('All done!')
        print(f"Uploaded files: {len(uploaded_files)}. Failed uploads: {len(failed_uploads)}.")
        logger.info(
            f"Uploaded files: {len(uploaded_files)}. Failed uploads: {len(failed_uploads)}.")

    elif ACTION == 'download':
        logger = logging.getLogger('sml_download_output')
        logger.setLevel(logging.DEBUG)

        fh = logging.FileHandler('sml_download_output.log')
        fh.setLevel(logging.DEBUG)

        ch = logging.StreamHandler()
        ch.setLevel(logging.ERROR)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        logger.addHandler(fh)
        logger.addHandler(ch)

        output_files = [
            filepath for filepath in sfs.ls(REMOTE_DIR, PROJECT_ID) if "." in filepath
        ]
        output_dirs = [
            filepath for filepath in sfs.ls(REMOTE_DIR, PROJECT_ID) if filepath[-1] == "/"
        ]

        print("Creating local folder structure...")
        logger.info("Creating local folder structure...")
        create_local_dir_tree(output_dirs)

        # The length of the check_local_dirs list should
        # equal the length of the output_dirs list that
        # was used to create the local directories using
        # the create_local_dir_tree() function.
        check_local_dirs = []

        for root, subdirs, files in os.walk("./output/"):
            check_local_dirs += [root]

        print(f'Directories created: {len(output_dirs)}')
        print(f'Directories checked: {len(check_local_dirs)}')
        logger.info(f'Directories created: {len(output_dirs)}')
        logger.info(f'Directories checked: {len(check_local_dirs)}')

        downloaded_files = []
        failed_downloads = []

        for filepath in output_files:
            print("Downloading:", filepath)
            logger.info(f'Downloading: {filepath}')
            try:
                download_file(filepath, PROJECT_ID)
                print("Downloaded:", filepath)
                logger.info(f'Downloaded: {filepath}')
                downloaded_files.append(filepath)
            except Exception:
                failed_downloads.append(filepath)
                logger.exception(f'Error downloading file {filepath}')
                print(f'Error downloading file {filepath}')

        print(f'Files to download: {len(output_files)}')
        logger.info(f'Files to download: {len(output_files)}')
        print(f'Files downloaded: {len(downloaded_files)}')
        logger.info(f'Files downloaded: {len(downloaded_files)}')

        with open("downloaded_files.pkl", "wb") as f:
            pickle.dump(downloaded_files, f)

        with open("failed_downloads.pkl", "wb") as f:
            pickle.dump(failed_downloads, f)

    elif ACTION == "check_download_dirs":
        output_dirs = [
            filepath for filepath in sfs.ls(REMOTE_DIR, PROJECT_ID) if filepath[-1] == "/"
        ]

        print("Creating local folder structure...")
        create_local_dir_tree(output_dirs)

        # The length of the check_local_dirs list should
        # equal the length of the output_dirs list that
        # was used to create the local directories using
        # the create_local_dir_tree() function.
        check_local_dirs = []

        for root, subdirs, files in os.walk(LOCAL_DIR):
            check_local_dirs += [root]

        print(f'Directories created: {len(output_dirs)}')
        print(f'Directories checked: {len(check_local_dirs)}')

    else:
        print(f"Action should be either 'upload' or 'download'. Currently: ACTION={ACTION}")


if __name__ == "__main__":
    main()
