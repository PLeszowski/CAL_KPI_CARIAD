"""
Module manages extraction of Core_Calibration_Dynamic from split pickles
Patryk Leszowski
APTIV
ADCAM MID
CALIBRATION
"""
import serializer
from file_list import FileList
import logging
import const as c


class PickleToDict:
    """
    Class handles the extraction of Core_Calibration_Dynamic from split pickles
    """
    def __init__(self, args_dict):
        self.log_pickle_folder = args_dict['log_pickle_folder']
        self.log_pickle_ext = args_dict['log_pickle_ext']
        self.pkl_dict_folder = args_dict['pkl_dict_folder']
        self.bad_file_list = []
        self.good_file_list = []
        self.logger = logging.getLogger(__name__)

        self.split_pickles = FileList(self.log_pickle_folder, self.log_pickle_ext)
        self.split_pickles.get_file_list()
        self.log_files = self.split_pickles.files

    def get_cal_dict_pickles(self):
        log_counter = 0
        logs_total = len(self.log_files)
        for log_file in self.log_files:
            try:
                plk = serializer.load_pkl(self.log_pickle_folder, log_file)
                log_counter += 1
            except Exception as e:
                self.logger.error(f'Cant open: {log_file}')
                self.logger.exception(e)
                self.bad_file_list.append(log_file)
                continue
            split_dict = {'cal_c2w_dict': {}, 'veh_speed_dict': {}, 'prelabel_dict': {}, 'common_counter_dict': {}}

            try:
                if 'EYEQ_TO_HOST' in plk['SPI'] and 'Core_Calibration_Output_protocol' in plk['SPI']['EYEQ_TO_HOST']:
                    split_dict['cal_c2w_dict'][c.DFROW_TIMESTAMP] = plk['SPI']['EYEQ_TO_HOST']['Core_Calibration_Output_protocol'][c.DFROW_TIMESTAMP]
                    split_dict['cal_c2w_dict'][c.SIG_CLB_C2W_STATE] = plk['SPI']['EYEQ_TO_HOST']['Core_Calibration_Output_protocol'][c.SIG_CLB_C2W_STATE]
                    split_dict['cal_c2w_dict'][c.SIG_CLB_C2W_YAW] = plk['SPI']['EYEQ_TO_HOST']['Core_Calibration_Output_protocol'][c.SIG_CLB_C2W_YAW]
                    split_dict['cal_c2w_dict'][c.SIG_CLB_C2W_PITCH] = plk['SPI']['EYEQ_TO_HOST']['Core_Calibration_Output_protocol'][c.SIG_CLB_C2W_PITCH]
                    split_dict['cal_c2w_dict'][c.SIG_CLB_C2W_CAM_HEIGHT] = plk['SPI']['EYEQ_TO_HOST']['Core_Calibration_Output_protocol'][c.SIG_CLB_C2W_CAM_HEIGHT]
                    split_dict['cal_c2w_dict'][c.SIG_CLB_C2W_ROLL] = plk['SPI']['EYEQ_TO_HOST']['Core_Calibration_Output_protocol'][c.SIG_CLB_C2W_ROLL]
                    split_dict['cal_c2w_dict'][c.SIG_CLB_C2W_STATE_DEGRADE_CAUSE] = plk['SPI']['EYEQ_TO_HOST']['Core_Calibration_Output_protocol'][c.SIG_CLB_C2W_STATE_DEGRADE_CAUSE]
                else:
                    split_dict['cal_c2w_dict'][c.DFROW_TIMESTAMP] = []
                    split_dict['cal_c2w_dict'][c.SIG_CLB_C2W_STATE] = []
                    split_dict['cal_c2w_dict'][c.SIG_CLB_C2W_YAW] = []
                    split_dict['cal_c2w_dict'][c.SIG_CLB_C2W_PITCH] = []
                    split_dict['cal_c2w_dict'][c.SIG_CLB_C2W_CAM_HEIGHT] = []
                    split_dict['cal_c2w_dict'][c.SIG_CLB_C2W_ROLL] = []
                    split_dict['cal_c2w_dict'][c.SIG_CLB_C2W_STATE_DEGRADE_CAUSE] = []
                    self.logger.warning(f'No Core_Calibration_Output_protocol: {log_file}')

            except KeyError as e:
                self.bad_file_list.append(log_file)
                self.logger.error(f'KeyError in Core_Calibration_Output_protocol: {log_file}')
                self.logger.exception(e)
                continue

            try:
                if 'EYEQ_TO_HOST' in plk['SPI'] and 'Core_Car_Output_protocol' in plk['SPI']['EYEQ_TO_HOST']:
                    split_dict['veh_speed_dict'][c.DFROW_TIMESTAMP] = plk['SPI']['EYEQ_TO_HOST']['Core_Car_Output_protocol'][c.DFROW_TIMESTAMP]
                    split_dict['veh_speed_dict'][c.DFROW_VEHICLE_SPEED] = plk['SPI']['EYEQ_TO_HOST']['Core_Car_Output_protocol'][c.SIG_VEHICLE_SPEED]
                else:
                    split_dict['veh_speed_dict'][c.DFROW_TIMESTAMP] = []
                    split_dict['veh_speed_dict'][c.DFROW_VEHICLE_SPEED] = []

            except KeyError as e:
                self.bad_file_list.append(log_file)
                self.logger.error(f'KeyError in Core_Car_Output_protocol: {log_file}')
                self.logger.exception(e)
                continue

            try:
                if 'EYEQ_TO_HOST' in plk['SPI'] and 'Core_Common_protocol' in plk['SPI']['EYEQ_TO_HOST']:
                    split_dict['common_counter_dict'][c.DFROW_TIMESTAMP] = plk['SPI']['EYEQ_TO_HOST']['Core_Common_protocol'][c.DFROW_TIMESTAMP]
                    split_dict['common_counter_dict'][c.SIG_CRC_COUNTER] = plk['SPI']['EYEQ_TO_HOST']['Core_Common_protocol'][c.SIG_CRC_COUNTER]
                else:
                    split_dict['common_counter_dict'][c.DFROW_TIMESTAMP] = []
                    split_dict['common_counter_dict'][c.SIG_CRC_COUNTER] = []

            except KeyError as e:
                self.bad_file_list.append(log_file)
                self.logger.error(f'KeyError in Core_Common_protocol: {log_file}')
                self.logger.exception(e)
                continue

            pkl_dict_name = log_file.split('.')[0]
            serializer.save_pkl(split_dict, self.pkl_dict_folder, pkl_dict_name)
            self.good_file_list.append(log_file)
            sts = log_counter/logs_total * 100
            # sts_str = f'{sts:.2f}'
            # self.logger.info(f'status: {sts_str}% saved {log_counter}/{logs_total}: {self.pkl_dict_folder + pkl_dict_name}')
            # print(f'saved {self.pkl_dict_folder + pkl_dict_name}')

        if self.bad_file_list:
            bad_file_num = self.bad_file_list
            self.logger.error(f'{bad_file_num} FILES NOT CONVERTED:')
            serializer.save_json(self.bad_file_list, self.pkl_dict_folder, 'bad_file_list.json')
            for bad_file in self.bad_file_list:
                self.logger.error(bad_file)

        if self.good_file_list:
            serializer.save_json(self.good_file_list, self.pkl_dict_folder, 'good_file_list.json')
