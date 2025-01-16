"""
Module manages loading and saving json and pickle files
Patryk Leszowski
APTIV
ADCAM MID
CALIBRATION
"""
import pickle
import json
import logging
import re
import lzma
import os

logger = logging.getLogger(__name__)


def save_pkl(obj, path, file):
    """
    Save object to pickle
    :param obj: object to be serialized
    :param path: path to folder
    :param file: file name
    :return:
    """
    logger.info(f'saving pickle: {file}')
    if not re.match(r'.+\.pi?c?kle?', file):
        file += '.pickle'

    try:
        path_file = os.path.join(path, file)
        with open(path_file, 'wb+') as f:
            pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)
    except (FileNotFoundError, PermissionError, UnicodeDecodeError) as e:
        logger.error(f'failed to save pickle: {file}')
        logger.exception(e)


def load_pkl(path, file):
    """
    Load object
    :param path: path to folder
    :param file: pickle file name
    :return:
    """
    path_file = os.path.join(path, file)
    ext = file.split('.')[-1]
    logger.info(f'loading pickle: {file}')

    if ext == 'xz':
        with lzma.open(path_file, 'rb') as f:
            return pickle.load(f)

    else:
        if not re.match(r'.+\.pi?c?kle?', file):
            path_file += '.pickle'
            # file += '.pkl'

        try:
            with open(path_file, 'rb') as f:
                return pickle.load(f)
        except (FileNotFoundError, PermissionError, UnicodeDecodeError) as e:
            logger.error(f'failed to load pickle: {file}')
            logger.exception(e)
            raise e


def save_json(obj, path, file=None):
    """
    Save object to json
    :param obj: object to be serialized
    :param path: path to folder
    :param file: file name
    :return:
    """
    logger.info(f'saving json: {file}')
    if file:
        filename = json_file_name(file)
        path_file = os.path.join(path, filename)
    else:
        path_file = path
    try:
        with open(path_file, 'w') as f:
            json.dump(obj, f, indent=2)
    except (FileNotFoundError, PermissionError, UnicodeDecodeError) as e:
        logger.error(f'failed to save json: {file}')
        logger.exception(e)
        raise e


def load_json(path, file=None):
    """
     Load object
    :param path: path to folder
    :param file: json file name
    :return:
    """
    logger.info(f'loading json: {file}')
    if file:
        filename = json_file_name(file)
        path_file = os.path.join(path, filename)
    else:
        path_file = path
    try:
        with open(path_file, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, PermissionError, UnicodeDecodeError) as e:
        logger.error(f'failed to load json: {file}')
        logger.exception(e)
        raise e


def json_file_name(file):
    """
    :param file: filename
    :return:
    If json extension is not given add it
    """
    if '.json' not in file:
        return file + '.json'
    else:
        return file
