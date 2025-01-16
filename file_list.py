"""
Module manages file lists
Patryk Leszowski
APTIV
ADCAM MID
CALIBRATION
"""
import logging
import os


class FileList:
    """
    Class holds a list of files with a given extension
    """

    def __init__(self, path, ext):
        self.path = path
        self.ext = ext
        self.files = []
        self.logger = logging.getLogger(__name__)

    def get_file_list(self):
        """
        :return: None
        Method reads all files from folder in path_log_file
        and returns a list of files with the extension ext.
        """

        if '.' not in self.ext:
            self.ext = '.' + self.ext

        try:
            allfiles = os.listdir(self.path)
        except FileNotFoundError as e:
            self.logger.critical(f'{self.path} does not exist')
            self.logger.exception(e)
            raise e
        except NotADirectoryError as e:
            self.logger.critical(f'{self.path} is not a directory')
            self.logger.exception(e)
            raise e
        except PermissionError as e:
            self.logger.critical(f'{self.path} permission denied')
            self.logger.exception(e)
            raise e
        else:
            if allfiles:
                self.logger.info(f'getting *{self.ext} file list from {self.path}')
                for file in allfiles:
                    if file.endswith(self.ext):
                        self.files.append(file)
                if not self.files:
                    self.logger.error(f'{self.path} does not contain any *{self.ext} files')
                    self.logger.exception(FileNotFoundError)
                    raise FileNotFoundError
            else:
                self.logger.error(f'{self.path} does not contain any files')
                self.logger.exception(FileNotFoundError)
                raise FileNotFoundError
