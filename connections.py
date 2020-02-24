import gcsfs
import json
import os


def get_GCS_client():
    """
    Return GCS File System
    """
    if 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ:
        credentials = json.loads(os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'))
    elif 'GOOGLE_APPLICATION_CREDENTIALS_FILE' in os.environ:
        fname = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_FILE')
        fname = os.path.abspath(os.path.expanduser(fname))
        with open(fname) as f:
            credentials = json.load(f)
    else:
        fname = '~/.secrets/tiagobbatalhao-personal-11b8d02c4989.json'
        fname = os.path.abspath(os.path.expanduser(fname))
        with open(fname) as f:
            credentials = json.load(f)
    gcs = gcsfs.GCSFileSystem(
        project='tiagobbatalhao-personal',
        token=credentials,
    )
    return gcs
