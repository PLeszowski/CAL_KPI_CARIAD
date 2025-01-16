"""
Module sets up and starts the kpi report sheet generation sequences
Patryk Leszowski
APTIV
ADCAM MID
CALIBRATION
"""
import logging
import argparse
import re
import const
import os
import sys
import time
import datetime
from sequencer import Sequencer
from pickle_to_dict import PickleToDict
from get_prelabels import GetPrelabel
from get_prelabels import MakePrelabel
from measurements import Measurements

def main():
    """
    :return: None
    Main function: Set up logger, get args, initialize Sequencer, run kpi sequence
    """

    class CustomFormatter(logging.Formatter):
        """Logging Formatter to add colors and count warning / errors"""

        bright = "\x1b[1m"
        white = "\x1b[33;30m"
        green = "\x1b[33;32m"
        yellow = "\x1b[33;33m"
        red = "\x1b[31;31m"
        bold_red = "\x1b[31;1m"
        reset = "\x1b[0m"
        format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"

        FORMATS = {
            logging.DEBUG: white + bright + format + reset,
            logging.INFO: bright + format + reset,
            logging.WARNING: yellow + format + reset,
            logging.ERROR: red + format + reset,
            logging.CRITICAL: bold_red + format + reset
        }

        def format(self, record):
            log_fmt = self.FORMATS.get(record.levelno)
            fm = logging.Formatter(log_fmt)
            return fm.format(record)

    # Configure root logger (used in all modules)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)  # Set logging level (DEBUG generates very large file)
    file_handler = logging.FileHandler('log/log_main.log', mode='w')
    file_handler.setFormatter(CustomFormatter())
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(CustomFormatter())
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.debug('Debug Color')
    logger.info('Info Color')
    logger.warning('Warning Color')
    logger.error('Error Color')

    _start_time = time.time()
    logger.info('Script start time: ' + str(datetime.datetime.now().time()))

    # default paths
    prelabel_dict_folder = const.PRELABEL_DICT_FOLDER
    prelabel_dict_file = const.PRELABEL_DICT_FILE
    log_pickle_folder = const.LOG_PICKLE_FOLDER
    log_pickle_ext = const.LOG_PICKLE_EXT
    pkl_dict_folder = const.CAL_DICT_PICKLE_FOLDER
    pkl_dict_ext = const.CAL_PKL_DICT_EXT
    path_report = const.TEST_REPORT_FOLDER
    partial_results_folder = const.PARTIAL_RESULTS_FOLDER
    path_gt_data = const.GT_DATA_FOLDER
    measurement_folder = const.MEASUREMENT_DFS_FOLDER
    sw_package_num = const.SW_PACKAGE_NUM
    output_arg_ok = 0

    # get args
    parser = argparse.ArgumentParser(epilog='Edit paths in const.py to use just run_cal_kpi.py without args')
    parser.add_argument('-i', '--input', help='input pkl_dict_folder to log files')
    parser.add_argument('-o', '--output', help='output pkl_dict_folder for report files')
    parser.add_argument('-t', '--log_file_type', help='change log file type - can be json or pkl (default json)')
    parser.add_argument('-s', '--software', help='set software package number (default 0)')
    args = parser.parse_args()

    if args.input:
        pkl_dict_folder = args.input
        if re.search(r'\\', pkl_dict_folder):
            pkl_dict_folder = re.sub(r'\\+', '/', pkl_dict_folder)

    if args.output:
        output_arg_ok = 1
        path_report = args.output
        if re.search(r'\\', path_report):
            path_report = re.sub(r'\\+', '/', path_report)

    if args.log_file_type:
        pkl_dict_ext = args.log_file_type

    if args.software:
        sw_package_num = args.software

    if not args.input and not args.output:
        logger.info('Using default paths from const.py')
        print('Using default paths from const.py')

    if pkl_dict_folder[-1] is not '/':
        pkl_dict_folder += '/'
    if path_report[-1] is not '/':
        path_report += '/'

    # check input folders
    if not os.path.isdir(prelabel_dict_folder):
        logger.error(f'ERROR: Path to prelabel dictionary {prelabel_dict_folder} does not exist!!!')
        sys.exit()
    if not os.path.isdir(path_gt_data):
        logger.error(f'ERROR: Path to GT data {path_gt_data} does not exist!!!')
        sys.exit()
    if not os.path.isdir(log_pickle_folder):
        logger.error(f'ERROR: Path to pickle files {log_pickle_folder} does not exist!!!')
        sys.exit()

    # make output folders
    if not os.path.isdir(pkl_dict_folder):
        os.makedirs(pkl_dict_folder, exist_ok=True)
    if not os.path.isdir(path_report):
        os.makedirs(path_report, exist_ok=True)
    if not os.path.isdir(partial_results_folder):
        os.makedirs(partial_results_folder, exist_ok=True)
    if not os.path.isdir(measurement_folder):
        os.makedirs(measurement_folder, exist_ok=True)

    logger.info(f'input pkl_dict_folder {pkl_dict_folder}')
    logger.info(f'output report pkl_dict_folder {path_report}')
    logger.info(f'output json pkl_dict_folder {partial_results_folder}')
    logger.info(f'gt data pkl_dict_folder {path_gt_data}')

    report_file_name = const.TEST_REPORT_FILE_NAME + datetime.datetime.now().strftime(
        '_%Y%m%d_%H%M%S.') + const.TEST_REPORT_FILE_EXT

    args_dict = {'prelabel_dict_folder': prelabel_dict_folder,
                 'prelabel_dict': prelabel_dict_file,
                 'log_pickle_folder': log_pickle_folder,
                 'log_pickle_ext': log_pickle_ext,
                 'pkl_dict_folder': pkl_dict_folder,
                 'pkl_dict_ext': pkl_dict_ext,
                 'path_report': path_report,
                 'report_file_name': report_file_name,
                 'partial_results_folder': partial_results_folder,
                 'measurement_folder': measurement_folder,
                 'path_gt_data': path_gt_data,
                 'sw_package_num': sw_package_num}

    # Get prelabels and save for future use (use if prelabel_dict.json has not been made)
    def get_prelabels(arguments):
        pl = GetPrelabel(arguments)
        pl.get_prelabel()
        pl.save_prelabel_dict()

    # Make manual prelabels
    def make_prelabels(arguments):
        m = Measurements(arguments)
        m.get_measurement_ids()
        measurement_ids = m.measurement_id_list
        mp = MakePrelabel(arguments, measurement_ids, gt_id="0099", road_type="city")
        mp.make_prelabel()

    # Make 'Core_Calibration_Dynamic' dictionary pickles from split pickles.xz
    def make_cal_pickles(arguments):
        p2d = PickleToDict(arguments)
        p2d.get_cal_dict_pickles()

    # Make measurement dataframe pickles from 'Core_Calibration_Dynamic' dictionary pickles
    def make_measurements(arguments):
        pl = GetPrelabel(arguments)
        pl.load_prelabel_dict()
        arguments['prelabel_dict'] = pl.prelabel_dict
        logger.info('Initializing Sequencer')
        sq = Sequencer(arguments)
        sq.get_measurement_ids()
        sq.splits_to_measurements()  # also puts prelabels into measurements

    # Run KPI sequences on measurement dataframe pickles
    def run_kpi_sequence(arguments):
        logger.info('Initializing Sequencer')
        sq = Sequencer(arguments)
        sq.get_measurement_ids()
        sq.run_kpi_sequence()

    # Choose script function
    script_function = 4

    # 1: Get prelabels and save for future use (use if prelabel_dict.json has not been made)
    if script_function == 1:
        get_prelabels(args_dict)
    # 2: Make CAL dictionary pickles from split pickles.xz
    elif script_function == 2:
        make_cal_pickles(args_dict)
    # 3: Make measurement dataframe pickles from CAL dictionary pickles
    elif script_function == 3:
        make_measurements(args_dict)
    # 4: Run KPI sequences on measurement dataframe pickles
    elif script_function == 4:
        run_kpi_sequence(args_dict)
    else:
        logger.error('Wrong script function chosen')
        sys.exit()

    _script_time = time.time() - _start_time
    logger.info('Script end time: ' + str(datetime.datetime.now().time()))
    logger.info('Total script time: ' + str(datetime.timedelta(seconds=_script_time)))
    file_handler.close()


if __name__ == "__main__":
    main()
