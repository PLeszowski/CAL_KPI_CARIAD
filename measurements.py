"""
Module manages tasks related to splits and measurements
Patryk Leszowski
APTIV
ADCAM MID
CALIBRATION
"""
import logging
import const
import sys
import pandas
import numpy as np
from file_list import FileList
import df_loader
from excel_printer import ExcelPrinter
import re
import serializer
import datetime
import math


class Measurements:
    """
    Class handles tasks related to splits and measurements
    Joins splits into measurements, puts measurement info dataframes, collects data about each measurement.
    """

    def __init__(self, args_dict):
        self.path_pickle_dict_folder = args_dict['pkl_dict_folder']
        self.ext = args_dict['pkl_dict_ext']
        self.prelabel_dict = args_dict['prelabel_dict']
        self.path_report = args_dict['path_report']
        self.report_file_name = args_dict['report_file_name']
        self.sbV_PackageNumber = args_dict['sw_package_num']
        self.measurement_dfs_folder = args_dict['measurement_folder']
        self.measurement_id_list = []
        self.num_of_measurements = 0
        self.dataframe_list = []
        self.nvm_check_failed_dict = {}
        self.crc_reset_counter = 0
        self.crc_reset_counter_at_switch_app = 0
        self.c2w_no_vision_counter = 0
        self.c2w_not_converged_counter = 0
        self.oor_info_dict = {}
        self.measurement_data_dict = {}
        self.invalid_measurement_list = []
        self.gt_data_df = df_loader.get_gt_data()
        self.file_list = FileList(self.path_pickle_dict_folder, self.ext)
        self.logger = logging.getLogger(__name__)
        # define lists to store info about each drive scenarios (ds)
        # number of elements correspond to the number of ds values in static method get_ds_number
        self.number_of_drives_per_ds = [0] * 31  # TODO range(x) depends on ds in get_ds_number method last elif
        self.number_of_clips_per_ds = [0] * 31  # TODO range(x) depends on ds in get_ds_number method last elif
        self.distance_driven_per_ds = [0] * 31  # TODO range(x) depends on ds in get_ds_number method last elif
        self.time_driven_per_ds = [0] * 31  # TODO range(x) depends on ds in get_ds_number method last elif

    def get_measurement_ids(self):
        """
        :return: None
        self.file_list.files contains a list of all splits ADCAM_VIN_DATE_TIME_XXXX
        Method gets unique measurement names ADCAM_VIN_DATE_TIME from all split files ADCAM_VIN_DATE_TIME_XXXX
        After method is executed, self.measurement_id_list contains a list of unique measurement names ADCAM_VIN_DATE_TIME
        """
        ids = []  # IDs list
        # get list of log files
        self.file_list.get_file_list()
        for file in self.file_list.files:
            # in file name find 8 digits followed by underscore followed by 6 digits, and group them
            if const.PROJECT_CONFIG == const.CARIAD:
                search_file_name = re.search(r'(CDMFK_.+_\d{8}_\d{6})_', file)
            else:  # const.PROJECT_CONFIG == const.ADCAM:
                search_file_name = re.search(r'(AD(CAM|MCP)_.+_\d{8}_\d{6})_', file)
            if search_file_name:
                # if matched append group to IDs list
                measurement_id = search_file_name.group(1)
                ids.append(measurement_id)
        # keep only unique IDs
        self.measurement_id_list = np.unique(np.array(ids))
        self.num_of_measurements = self.measurement_id_list.size
        # report to logger
        if self.num_of_measurements > 0:
            self.logger.info(f'Found {self.num_of_measurements} {self.ext} unique ADCAM_VIN_DATE_TIME measurements')
        else:
            self.logger.error(f'No {self.ext} files with ADCAM naming convention found')
            sys.exit(1)

    def get_measurement_data(self):
        """
        :return: None
        Method puts info (type=dictionary) about each measurement into self.measurement_data_dict
        """
        self.logger.info('getting measurement data')
        # self.measurement_id_list contains files, each containing data from one measurement (unique ADCAM_VIN_DATE_TIME)
        for file in self.measurement_id_list:
            # Change to ADCAM name convention
            # file = self.to_adcam_split_name(file)
            df = serializer.load_pkl(self.measurement_dfs_folder, file)
            # use static method get_ds_number to get ds number from df
            ds = self.get_ds_number(df)
            # increment number of drives (trials) for that ds
            self.number_of_drives_per_ds[ds] += 1
            # get number of splits in measurement
            self.number_of_clips_per_ds[ds] += len(set(df[const.DFROW_LOG_FILE]))
            # get difference between last and first distance of current ds and add to total of that ds
            self.distance_driven_per_ds[ds] += df[const.DFROW_DISTANCE].iloc[-1] - df[const.DFROW_DISTANCE].iloc[0]
            # get difference between last and first timestamp of current ds and add to total of that ds
            self.time_driven_per_ds[ds] += df[const.DFROW_TIMESTAMP].iloc[-1] - df[const.DFROW_TIMESTAMP].iloc[0]
            # put data into dictionary
            data_dict = {'LogNameStart': df[const.DFROW_LOG_FILE].iloc[0],  # name of first split in df
                         'LogNameEnd': df[const.DFROW_LOG_FILE].iloc[-1],  # name of last split in df
                         'sbV_PackageNumber': self.sbV_PackageNumber,  # HIL SW package number
                         'Functionality': const.FUNCTIONALITY,  # functionality name (constant)
                         'DriveScenario': ds,  # drive scenario number
                         'Trial': self.number_of_drives_per_ds[ds],  # drive scenario trial
                         const.DFROW_GT_ID: int(df.at[0, const.DFROW_GT_ID]),  # ground truth ID
                         const.DFROW_ROAD: df.at[0, const.DFROW_ROAD],  # road type
                         const.DFROW_WEATHER: df.at[0, const.DFROW_WEATHER],  # weather
                         const.DFROW_DAYTIME: df.at[0, const.DFROW_DAYTIME],  # time of day
                         'SUSPENSION': df.at[0, const.DFROW_SUSPENSION]}  # suspension

            # to self.measurement_data_dict add value data_dict with key
            self.measurement_data_dict[file] = data_dict
            if ds == 0:  # invalid drive scenarios
                self.invalid_measurement_list.append(file)

        # report invalid drive scenarios to logger
        if self.number_of_drives_per_ds[0] > 0:
            self.logger.warning(f'found {self.number_of_drives_per_ds[0]} invalid measurements')
            for measurement in self.invalid_measurement_list:
                self.logger.warning(f'Measurement {measurement} does not match any DS.')
        self.logger.info('got measurement data')

    def export_ds_collected_data_info(self):
        """
        :return:
        Method adds drive scenario data to the HEADER sheet of the kpi report
        """
        xls = ExcelPrinter(self.path_report, self.report_file_name, 'HEADER')
        data_dict = {}
        milliseconds = 0
        self.logger.info('\tDS\tNum of Drives\tNum of Clips\tDistance Driven [km]\tTime Driven [min]')
        current_row = 2
        for i in range(31):  # TODO range(x) depends on ds in get_ds_number method last elif
            key = 'DS-' + str(i)
            data_dict[key] = {}
            drives = self.number_of_drives_per_ds[i]
            clips = self.number_of_clips_per_ds[i]
            drive_distance = self.distance_driven_per_ds[i]
            drive_time = str(datetime.timedelta(seconds=self.time_driven_per_ds[i]))
            ms_match = re.match(r'.*\.(\d{3})\d{3}.*', drive_time)
            if ms_match:
                drive_time = drive_time[0:-3]
                milliseconds = milliseconds + int(ms_match.group(1))
                print(drive_time)
            else:
                drive_time = drive_time + '.000'
                print(drive_time)
            self.logger.info(f'\tDS-{i}\t{drives}\t{clips}\t{drive_distance}\t{drive_time}')
            data_dict[key] = {'Num of Drives': drives,
                              'Num of Clips': clips,
                              'Distance Driven [km]': drive_distance.__round__(2),
                              'Time Driven': drive_time}

        total_drives = self.num_of_measurements  # int(np.array(self.number_of_drives_per_ds).sum())
        total_clips = int(np.array(self.number_of_clips_per_ds).sum())
        total_distance = float(np.array(self.distance_driven_per_ds).sum()).__round__(2)
        total_time = datetime.timedelta(seconds=int(np.array(self.time_driven_per_ds).sum()))
        total_time_str = str(total_time)
        total_time_str = total_time_str + '.' + str(milliseconds)[-3:]
        crc_reset_ok = self.num_of_measurements - self.crc_reset_counter
        self.logger.info(f'Total number of drives: {total_drives}')
        self.logger.info(f'Total number of clips: {total_clips}')
        self.logger.info(f'Total distance driven: {total_distance}')
        self.logger.info(f'Total time driven: {total_time_str}')
        if self.invalid_measurement_list:
            invalid_measurement_num = len(self.invalid_measurement_list)
            self.logger.info(f'Total Invalid Measurements = {invalid_measurement_num}')
            for measurement in self.invalid_measurement_list:
                self.logger.info(f'{measurement}')
        totals_dict = {'DS All': {'Num of Drives': total_drives,
                                  'Num of Clips': total_clips,
                                  'Distance Driven [km]': total_distance,
                                  'Time Driven': total_time_str
                                  }
                       }
        nvm_check_dict = {'DS All': {'SPC values used in Vision': 'None',
                                     'SPC pitch values used in Vision': 'None',
                                     'SPC yaw values used in Vision': 'None',
                                     'SPC roll values used in Vision': 'None',
                                     'SPC height values used in Vision': 'None',
                                     'Reset Counter OK': crc_reset_ok,
                                     'Total number of resets': self.crc_reset_counter,
                                     'Number of resets during switch app': 'None'
                                     }
                          }

        converged_dict = {'DS All': {'Drives with no SPC': 'None',
                                     'Drives with SPC not converged': 'None',
                                     'Drives with SPC converges and switched to Vision': 'None',
                                     'Drives with no convergence in Vision': self.c2w_not_converged_counter
                                     }

                          }

        if self.oor_info_dict:
            oor_counter = self.oor_info_dict['oor_counter']
            oor_distance = self.oor_info_dict['oor_distance']
            oor_time = datetime.timedelta(seconds=int(np.array(self.oor_info_dict['oor_time']).sum()))
            oor_time_str = str(oor_time)
            oor_time_percent = oor_time/total_time
        else:
            oor_counter = 0
            oor_distance = 0
            oor_time_str = '00:00:00'
            oor_time_percent = 0
        c2w_state_oor_dict = {'DS All': {'Drives with CLB_C2W_State OOR': oor_counter,
                                         'Total OOR Distance': oor_distance,
                                         'Total OOR Time': oor_time_str,
                                         'Total OOR Time %': oor_time_percent
                                         }

                              }
        try:
            current_row = xls.export_to_excel(data_dict, current_row, None, 'Drive Scenario')
            current_row = xls.export_to_excel(totals_dict, current_row + 1, None, 'Total')
            current_row = xls.export_to_excel(nvm_check_dict, current_row + 1, None, 'NVM Check')
            current_row = xls.export_to_excel(converged_dict, current_row + 1, None, 'Convergence')
            xls.export_to_excel(c2w_state_oor_dict, current_row + 1, None, 'CLB_C2W_State OOR')
        except TypeError:
            self.logger.error('data_dict wrong type')
        self.logger.info('added collected data info')

    def make_measurement_df_pickles(self):
        """
        :return: None
        self.measurement_id_list contains a list of unique measurement names ADCAM_VIN_DATE_TIME
        self.file_list.files contains a list of all splits ADCAM_VIN_DATE_TIME_XXXX
        Method takes every split ADCAM_VIN_DATE_TIME_XXXX in self.file_list.files that has the name ADCAM_VIN_DATE_TIME
        and concatenates the splits to one dataframe (makes one measurement dataframe of splits from the same drive).
        Does it for each measurement names ADCAM_VIN_DATE_TIME in self.measurement_id_list
        Saves each dataframe to a pickle file
        After method is executed, every dataframe pickle is one measurement (unique ADCAM_VIN_DATE_TIME)
        """
        for measurement_id in self.measurement_id_list:
            df_list = []
            for file in self.file_list.files:
                if measurement_id in file:
                    # for each file unpack file to dictionary
                    if self.ext == 'json':
                        data_dict = serializer.load_json(self.path_pickle_dict_folder, file)
                    elif self.ext == 'pkl' or self.ext == 'pickle':
                        data_dict = serializer.load_pkl(self.path_pickle_dict_folder, file)
                    else:
                        self.logger.error(f'ERROR: wrong file extension {file}. Must be json or pkl')
                        raise FileNotFoundError(f'File extension must be json or pkl')
                    # Change to ADCAM name convention
                    measurement_id_temp = self.to_adcam_split_name(measurement_id)
                    # if data_dict['prelabel_dict']['LogName'] doesn't exist,
                    # add key ('LogName'), and value (filename without extension)
                    if measurement_id_temp in self.prelabel_dict.keys():
                        data_dict['prelabel_dict'][const.DFROW_LOG_FILE] = [file[0:48]]
                        data_dict['prelabel_dict'][const.DFROW_GT_ID] = [
                            int(self.prelabel_dict[measurement_id_temp][const.DFROW_GT_ID][0])]
                        data_dict['prelabel_dict'][const.DFROW_ROAD] = self.prelabel_dict[measurement_id_temp][
                            const.DFROW_ROAD]
                        data_dict['prelabel_dict'][const.DFROW_WEATHER] = self.prelabel_dict[measurement_id_temp][
                            const.DFROW_WEATHER]
                        data_dict['prelabel_dict'][const.DFROW_DAYTIME] = self.prelabel_dict[measurement_id_temp][
                            const.DFROW_DAYTIME]
                        self.logger.info('put prelabels into data_dict')
                    else:
                        self.logger.error(f'{measurement_id_temp} not in prelabel_dict')
                        break
                    # transform dictionary to dataframe
                    try:
                        if not self.check_data_dict(data_dict, file):
                            continue
                        data_frame = df_loader.load_df(data_dict, self.gt_data_df)
                        # self.logger.info(f'Put {self.ext} file {file} into dataframe')
                        del data_dict
                    except KeyError as e:
                        self.logger.error(f'KeyError in {file}')
                        self.logger.exception(e)
                    else:
                        df_list.append(data_frame)
                        del data_frame
            # concatenate the splits to one measurement dataframe
            try:
                measurement_df = pandas.concat(df_list, ignore_index=True, sort=True)
                measurement_df = self.get_driven_distance(measurement_df)
                df_size = sys.getsizeof(measurement_df)
                # save the dataframe to pickle
                serializer.save_pkl(measurement_df, self.measurement_dfs_folder, measurement_id)
                self.logger.info(f'saved measurement_df, size {df_size}, to file {measurement_id}.pkl')
            except ValueError as e:
                self.logger.exception(e)
                self.logger.error(f'one of the objects is empty')
                raise e

    def check_data_dict(self, data_dict, file):
        if 'veh_speed_dict' in data_dict:
            if len(data_dict['veh_speed_dict']['timestamp']) == 0:
                self.logger.warning(f'In veh_speed_dict timestamp is empty in {file}')
                return False
        else:
            self.logger.warning(f'No veh_speed_dict  in {file}')
            return False
        return True

    def to_adcam_split_name(self, split_name):
        if const.PROJECT_CONFIG == const.CARIAD:
            search = re.search(r'CDMFK_(\w{17}_\d{8}_\d{6})', split_name)
            if search:
                vin_date = search.group(1)
                split_name = f'ADCAM_{vin_date}'
            else:
                self.logger.warning(f'{split_name}: PROJECT_CONFIG = CARIAD, but CDMFK not found in measurement name')
        return split_name

    def print_ids(self):
        """
        :return:
        Print measurement_id_list content
        """
        for ids in self.measurement_id_list:
            print(ids)

    def print_file_list(self):
        """
        :return:
        Print file_list.files content
        """
        for file in self.file_list.files:
            print(file)

    @staticmethod
    def get_driven_distance(df):

        time_stamp = np.array(df[const.DFROW_TIMESTAMP])
        veh_speed = np.array(df['veh_speed'])
        distance = [0]
        last_valid_dist = 0
        for i in range(1, time_stamp.size):
            v_mean = (veh_speed[i] + veh_speed[i - 1]) / 2
            dist = ((time_stamp[i] - time_stamp[i - 1]) * (v_mean / 3600)) + distance[i - 1]
            if not math.isnan(dist):
                last_valid_dist = dist
            distance.append(last_valid_dist)
        df[const.DFROW_DISTANCE] = distance
        return df

    @staticmethod
    def get_ds_number(df):
        """
        :param df: dataframes containing data from one measurement
        :return: ds: (int) drive scenario number
        (defined in document BMW ADCAM Mid-ECU-10031411-UCC-015000-CAL-Use_Case_Catalog)
        """
        ds = 0
        if const.PROJECT_CONFIG == const.ADCAM:
            if df.at[0, const.DFROW_BR_YAW] == 0 and \
                    df.at[0, const.DFROW_BR_PITCH] == 0 and \
                    df.at[0, const.DFROW_BR_ROLL] == 0 and \
                    df.at[0, const.DFROW_SUSPENSION] == const.VAL_SUSP_DEFAULT and \
                    df.at[0, const.DFROW_ROAD] == const.VAL_ROAD_HIGHWAY and \
                    df.at[0, const.DFROW_WEATHER] == const.VAL_WEATHER_CLEAR and \
                    df.at[0, const.DFROW_DAYTIME] == const.VAL_DAYTIME_DAY:
                ds = 1
            elif df.at[0, const.DFROW_BR_YAW] == 0 and \
                    df.at[0, const.DFROW_BR_PITCH] == 0 and \
                    df.at[0, const.DFROW_BR_ROLL] == 0 and \
                    df.at[0, const.DFROW_SUSPENSION] == const.VAL_SUSP_DEFAULT and \
                    df.at[0, const.DFROW_ROAD] == const.VAL_ROAD_RURAL and \
                    df.at[0, const.DFROW_WEATHER] == const.VAL_WEATHER_CLEAR and \
                    df.at[0, const.DFROW_DAYTIME] == const.VAL_DAYTIME_DAY:
                ds = 2
            elif df.at[0, const.DFROW_BR_YAW] == 0 and \
                    df.at[0, const.DFROW_BR_PITCH] == 0 and \
                    df.at[0, const.DFROW_BR_ROLL] == 0 and \
                    df.at[0, const.DFROW_SUSPENSION] == const.VAL_SUSP_DEFAULT and \
                    df.at[0, const.DFROW_ROAD] == const.VAL_ROAD_CITY and \
                    df.at[0, const.DFROW_WEATHER] == const.VAL_WEATHER_CLEAR and \
                    df.at[0, const.DFROW_DAYTIME] == const.VAL_DAYTIME_DAY:
                ds = 3
            elif df.at[0, const.DFROW_BR_YAW] == 0 and \
                    df.at[0, const.DFROW_BR_PITCH] == 0 and \
                    df.at[0, const.DFROW_BR_ROLL] == 0 and \
                    df.at[0, const.DFROW_SUSPENSION] == const.VAL_SUSP_DEFAULT and \
                    df.at[0, const.DFROW_WEATHER] == const.VAL_WEATHER_CLEAR and \
                    df.at[0, const.DFROW_DAYTIME] == const.VAL_DAYTIME_NIGHT:
                ds = 4
            elif df.at[0, const.DFROW_BR_YAW] == 0 and \
                    df.at[0, const.DFROW_BR_PITCH] == 0 and \
                    df.at[0, const.DFROW_BR_ROLL] == 0 and \
                    df.at[0, const.DFROW_SUSPENSION] == const.VAL_SUSP_DEFAULT and \
                    df.at[0, const.DFROW_WEATHER] == const.VAL_WEATHER_RAIN and \
                    df.at[0, const.DFROW_DAYTIME] == const.VAL_DAYTIME_DAY:
                ds = 5
            elif df.at[0, const.DFROW_BR_YAW] == 0 and \
                    df.at[0, const.DFROW_BR_PITCH] == 0 and \
                    df.at[0, const.DFROW_BR_ROLL] == 0 and \
                    df.at[0, const.DFROW_SUSPENSION] == const.VAL_SUSP_DEFAULT and \
                    df.at[0, const.DFROW_WEATHER] == const.VAL_WEATHER_SNOW and \
                    df.at[0, const.DFROW_DAYTIME] == const.VAL_DAYTIME_DAY:
                ds = 6
            elif df.at[0, const.DFROW_BR_YAW] == 0 and \
                    df.at[0, const.DFROW_BR_PITCH] == 0 and \
                    df.at[0, const.DFROW_BR_ROLL] == 0 and \
                    df.at[0, const.DFROW_SUSPENSION] == const.VAL_SUSP_DEFAULT and \
                    df.at[0, const.DFROW_WEATHER] == const.VAL_WEATHER_FOG and \
                    df.at[0, const.DFROW_DAYTIME] == const.VAL_DAYTIME_DAY:
                ds = 7
            elif df.at[0, const.DFROW_BR_YAW] == 0 and \
                    df.at[0, const.DFROW_BR_PITCH] == 0 and \
                    df.at[0, const.DFROW_BR_ROLL] == 0 and \
                    df.at[0, const.DFROW_SUSPENSION] == const.VAL_SUSP_HIGH and \
                    df.at[0, const.DFROW_WEATHER] == const.VAL_WEATHER_CLEAR and \
                    df.at[0, const.DFROW_DAYTIME] == const.VAL_DAYTIME_DAY:
                ds = 8
            elif df.at[0, const.DFROW_BR_YAW] == 0 and \
                    df.at[0, const.DFROW_BR_PITCH] == 0 and \
                    df.at[0, const.DFROW_BR_ROLL] == 0 and \
                    df.at[0, const.DFROW_SUSPENSION] == const.VAL_SUSP_LOW and \
                    df.at[0, const.DFROW_WEATHER] == const.VAL_WEATHER_CLEAR and \
                    df.at[0, const.DFROW_DAYTIME] == const.VAL_DAYTIME_DAY:
                ds = 9
            elif df.at[0, const.DFROW_BR_YAW] == 0 and \
                    df.at[0, const.DFROW_BR_PITCH] == 0 and \
                    df.at[0, const.DFROW_BR_ROLL] == 0 and \
                    df.at[0, const.DFROW_SUSPENSION] == const.VAL_SUSP_VARYING and \
                    df.at[0, const.DFROW_WEATHER] == const.VAL_WEATHER_CLEAR and \
                    df.at[0, const.DFROW_DAYTIME] == const.VAL_DAYTIME_DAY:
                ds = 10
            elif df.at[0, const.DFROW_BR_YAW] == 0 and \
                    df.at[0, const.DFROW_BR_PITCH] == 3.5 and \
                    df.at[0, const.DFROW_BR_ROLL] == 0:
                ds = 11
            elif df.at[0, const.DFROW_BR_YAW] == 0 and \
                    df.at[0, const.DFROW_BR_PITCH] == 2.5 and \
                    df.at[0, const.DFROW_BR_ROLL] == 0:
                ds = 12
            elif df.at[0, const.DFROW_BR_YAW] == 0 and \
                    df.at[0, const.DFROW_BR_PITCH] == -2.5 and \
                    df.at[0, const.DFROW_BR_ROLL] == 0:
                ds = 13
            elif df.at[0, const.DFROW_BR_YAW] == 0 and \
                    df.at[0, const.DFROW_BR_PITCH] == -3.5 and \
                    df.at[0, const.DFROW_BR_ROLL] == 0:
                ds = 14
            elif df.at[0, const.DFROW_BR_YAW] == 3.5 and \
                    df.at[0, const.DFROW_BR_PITCH] == 0 and \
                    df.at[0, const.DFROW_BR_ROLL] == 0:
                ds = 15
            elif df.at[0, const.DFROW_BR_YAW] == 2.5 and \
                    df.at[0, const.DFROW_BR_PITCH] == 0 and \
                    df.at[0, const.DFROW_BR_ROLL] == 0:
                ds = 16
            elif df.at[0, const.DFROW_BR_YAW] == -2.5 and \
                    df.at[0, const.DFROW_BR_PITCH] == 0 and \
                    df.at[0, const.DFROW_BR_ROLL] == 0:
                ds = 17
            elif df.at[0, const.DFROW_BR_YAW] == -3.5 and \
                    df.at[0, const.DFROW_BR_PITCH] == 0 and \
                    df.at[0, const.DFROW_BR_ROLL] == 0:
                ds = 18
            elif df.at[0, const.DFROW_BR_YAW] == 0 and \
                    df.at[0, const.DFROW_BR_PITCH] == 0 and \
                    df.at[0, const.DFROW_BR_ROLL] == 3.5:
                ds = 19
            elif df.at[0, const.DFROW_BR_YAW] == 0 and \
                    df.at[0, const.DFROW_BR_PITCH] == 0 and \
                    df.at[0, const.DFROW_BR_ROLL] == 2.5:
                ds = 20
            elif df.at[0, const.DFROW_BR_YAW] == 0 and \
                    df.at[0, const.DFROW_BR_PITCH] == 0 and \
                    df.at[0, const.DFROW_BR_ROLL] == -2.5:
                ds = 21
            elif df.at[0, const.DFROW_BR_YAW] == 0 and \
                    df.at[0, const.DFROW_BR_PITCH] == 0 and \
                    df.at[0, const.DFROW_BR_ROLL] == -3.5:
                ds = 22
            elif df.at[0, const.DFROW_BR_YAW] == 3.5 and \
                    df.at[0, const.DFROW_BR_PITCH] == 3.5 and \
                    df.at[0, const.DFROW_BR_ROLL] == 3.5:
                ds = 23
            elif df.at[0, const.DFROW_BR_YAW] == -3.5 and \
                    df.at[0, const.DFROW_BR_PITCH] == -3.5 and \
                    df.at[0, const.DFROW_BR_ROLL] == -3.5:
                ds = 24
            elif df.at[0, const.DFROW_BR_YAW] == 0 and \
                    df.at[0, const.DFROW_BR_PITCH] == 4 and \
                    df.at[0, const.DFROW_BR_ROLL] == 0:
                ds = 25
            elif df.at[0, const.DFROW_BR_YAW] == 0 and \
                    df.at[0, const.DFROW_BR_PITCH] == -4 and \
                    df.at[0, const.DFROW_BR_ROLL] == 0:
                ds = 26
            elif df.at[0, const.DFROW_BR_YAW] == 4 and \
                    df.at[0, const.DFROW_BR_PITCH] == 0 and \
                    df.at[0, const.DFROW_BR_ROLL] == 0:
                ds = 27
            elif df.at[0, const.DFROW_BR_YAW] == -4 and \
                    df.at[0, const.DFROW_BR_PITCH] == 0 and \
                    df.at[0, const.DFROW_BR_ROLL] == 0:
                ds = 28
            elif df.at[0, const.DFROW_BR_YAW] == 0 and \
                    df.at[0, const.DFROW_BR_PITCH] == 5 and \
                    df.at[0, const.DFROW_BR_ROLL] == 0:
                ds = 29
            elif df.at[0, const.DFROW_BR_YAW] == 0 and \
                    df.at[0, const.DFROW_BR_PITCH] == -5 and \
                    df.at[0, const.DFROW_BR_ROLL] == 0:
                ds = 30
        if const.PROJECT_CONFIG == const.CARIAD:
            if df.at[0, const.DFROW_SUSPENSION] == const.VAL_SUSP_LOW and \
                    df.at[0, const.DFROW_ROAD] == const.VAL_ROAD_HIGHWAY and \
                    df.at[0, const.DFROW_WEATHER] == const.VAL_WEATHER_CLEAR and \
                    df.at[0, const.DFROW_DAYTIME] == const.VAL_DAYTIME_DAY:
                ds = 1
            elif df.at[0, const.DFROW_SUSPENSION] == const.VAL_SUSP_LOW and \
                    df.at[0, const.DFROW_ROAD] == const.VAL_ROAD_RURAL and \
                    df.at[0, const.DFROW_WEATHER] == const.VAL_WEATHER_CLEAR and \
                    df.at[0, const.DFROW_DAYTIME] == const.VAL_DAYTIME_DAY:
                ds = 2
            elif df.at[0, const.DFROW_SUSPENSION] == const.VAL_SUSP_LOW and \
                    df.at[0, const.DFROW_ROAD] == const.VAL_ROAD_CITY and \
                    df.at[0, const.DFROW_WEATHER] == const.VAL_WEATHER_CLEAR and \
                    df.at[0, const.DFROW_DAYTIME] == const.VAL_DAYTIME_DAY:
                ds = 3
            elif df.at[0, const.DFROW_SUSPENSION] == const.VAL_SUSP_LOW and \
                    df.at[0, const.DFROW_ROAD] == const.VAL_ROAD_HIGHWAY and \
                    df.at[0, const.DFROW_WEATHER] == const.VAL_WEATHER_CLEAR and \
                    df.at[0, const.DFROW_DAYTIME] == const.VAL_DAYTIME_NIGHT:
                ds = 4
            elif df.at[0, const.DFROW_SUSPENSION] == const.VAL_SUSP_LOW and \
                    df.at[0, const.DFROW_ROAD] == const.VAL_ROAD_RURAL and \
                    df.at[0, const.DFROW_WEATHER] == const.VAL_WEATHER_CLEAR and \
                    df.at[0, const.DFROW_DAYTIME] == const.VAL_DAYTIME_NIGHT:
                ds = 5
            elif df.at[0, const.DFROW_SUSPENSION] == const.VAL_SUSP_LOW and \
                    df.at[0, const.DFROW_ROAD] == const.VAL_ROAD_CITY and \
                    df.at[0, const.DFROW_WEATHER] == const.VAL_WEATHER_CLEAR and \
                    df.at[0, const.DFROW_DAYTIME] == const.VAL_DAYTIME_NIGHT:
                ds = 6
            elif df.at[0, const.DFROW_SUSPENSION] == const.VAL_SUSP_HIGH and \
                    df.at[0, const.DFROW_ROAD] == const.VAL_ROAD_HIGHWAY and \
                    df.at[0, const.DFROW_WEATHER] == const.VAL_WEATHER_CLEAR and \
                    df.at[0, const.DFROW_DAYTIME] == const.VAL_DAYTIME_DAY:
                ds = 7
            elif df.at[0, const.DFROW_SUSPENSION] == const.VAL_SUSP_HIGH and \
                    df.at[0, const.DFROW_ROAD] == const.VAL_ROAD_RURAL and \
                    df.at[0, const.DFROW_WEATHER] == const.VAL_WEATHER_CLEAR and \
                    df.at[0, const.DFROW_DAYTIME] == const.VAL_DAYTIME_DAY:
                ds = 8
            elif df.at[0, const.DFROW_SUSPENSION] == const.VAL_SUSP_HIGH and \
                    df.at[0, const.DFROW_ROAD] == const.VAL_ROAD_CITY and \
                    df.at[0, const.DFROW_WEATHER] == const.VAL_WEATHER_CLEAR and \
                    df.at[0, const.DFROW_DAYTIME] == const.VAL_DAYTIME_DAY:
                ds = 9
            elif df.at[0, const.DFROW_SUSPENSION] == const.VAL_SUSP_HIGH and \
                    df.at[0, const.DFROW_ROAD] == const.VAL_ROAD_HIGHWAY and \
                    df.at[0, const.DFROW_WEATHER] == const.VAL_WEATHER_CLEAR and \
                    df.at[0, const.DFROW_DAYTIME] == const.VAL_DAYTIME_NIGHT:
                ds = 10
            elif df.at[0, const.DFROW_SUSPENSION] == const.VAL_SUSP_HIGH and \
                    df.at[0, const.DFROW_ROAD] == const.VAL_ROAD_RURAL and \
                    df.at[0, const.DFROW_WEATHER] == const.VAL_WEATHER_CLEAR and \
                    df.at[0, const.DFROW_DAYTIME] == const.VAL_DAYTIME_NIGHT:
                ds = 11
            elif df.at[0, const.DFROW_SUSPENSION] == const.VAL_SUSP_HIGH and \
                    df.at[0, const.DFROW_ROAD] == const.VAL_ROAD_CITY and \
                    df.at[0, const.DFROW_WEATHER] == const.VAL_WEATHER_CLEAR and \
                    df.at[0, const.DFROW_DAYTIME] == const.VAL_DAYTIME_NIGHT:
                ds = 12
            elif df.at[0, const.DFROW_SUSPENSION] == const.VAL_SUSP_LOW and \
                    df.at[0, const.DFROW_ROAD] == const.VAL_ROAD_HIGHWAY and \
                    df.at[0, const.DFROW_WEATHER] == const.VAL_WEATHER_RAIN and \
                    df.at[0, const.DFROW_DAYTIME] == const.VAL_DAYTIME_DAY:
                ds = 13
            elif df.at[0, const.DFROW_SUSPENSION] == const.VAL_SUSP_LOW and \
                    df.at[0, const.DFROW_ROAD] == const.VAL_ROAD_RURAL and \
                    df.at[0, const.DFROW_WEATHER] == const.VAL_WEATHER_RAIN and \
                    df.at[0, const.DFROW_DAYTIME] == const.VAL_DAYTIME_DAY:
                ds = 14
            elif df.at[0, const.DFROW_SUSPENSION] == const.VAL_SUSP_LOW and \
                    df.at[0, const.DFROW_ROAD] == const.VAL_ROAD_CITY and \
                    df.at[0, const.DFROW_WEATHER] == const.VAL_WEATHER_RAIN and \
                    df.at[0, const.DFROW_DAYTIME] == const.VAL_DAYTIME_DAY:
                ds = 15
            elif df.at[0, const.DFROW_SUSPENSION] == const.VAL_SUSP_LOW and \
                    df.at[0, const.DFROW_ROAD] == const.VAL_ROAD_HIGHWAY and \
                    df.at[0, const.DFROW_WEATHER] == const.VAL_WEATHER_RAIN and \
                    df.at[0, const.DFROW_DAYTIME] == const.VAL_DAYTIME_NIGHT:
                ds = 16
            elif df.at[0, const.DFROW_SUSPENSION] == const.VAL_SUSP_LOW and \
                    df.at[0, const.DFROW_ROAD] == const.VAL_ROAD_RURAL and \
                    df.at[0, const.DFROW_WEATHER] == const.VAL_WEATHER_RAIN and \
                    df.at[0, const.DFROW_DAYTIME] == const.VAL_DAYTIME_NIGHT:
                ds = 17
            elif df.at[0, const.DFROW_SUSPENSION] == const.VAL_SUSP_LOW and \
                    df.at[0, const.DFROW_ROAD] == const.VAL_ROAD_CITY and \
                    df.at[0, const.DFROW_WEATHER] == const.VAL_WEATHER_RAIN and \
                    df.at[0, const.DFROW_DAYTIME] == const.VAL_DAYTIME_NIGHT:
                ds = 18
            elif df.at[0, const.DFROW_SUSPENSION] == const.VAL_SUSP_HIGH and \
                    df.at[0, const.DFROW_ROAD] == const.VAL_ROAD_HIGHWAY and \
                    df.at[0, const.DFROW_WEATHER] == const.VAL_WEATHER_RAIN and \
                    df.at[0, const.DFROW_DAYTIME] == const.VAL_DAYTIME_DAY:
                ds = 19
            elif df.at[0, const.DFROW_SUSPENSION] == const.VAL_SUSP_HIGH and \
                    df.at[0, const.DFROW_ROAD] == const.VAL_ROAD_RURAL and \
                    df.at[0, const.DFROW_WEATHER] == const.VAL_WEATHER_RAIN and \
                    df.at[0, const.DFROW_DAYTIME] == const.VAL_DAYTIME_DAY:
                ds = 20
            elif df.at[0, const.DFROW_SUSPENSION] == const.VAL_SUSP_HIGH and \
                    df.at[0, const.DFROW_ROAD] == const.VAL_ROAD_CITY and \
                    df.at[0, const.DFROW_WEATHER] == const.VAL_WEATHER_RAIN and \
                    df.at[0, const.DFROW_DAYTIME] == const.VAL_DAYTIME_DAY:
                ds = 21
            elif df.at[0, const.DFROW_SUSPENSION] == const.VAL_SUSP_HIGH and \
                    df.at[0, const.DFROW_ROAD] == const.VAL_ROAD_HIGHWAY and \
                    df.at[0, const.DFROW_WEATHER] == const.VAL_WEATHER_RAIN and \
                    df.at[0, const.DFROW_DAYTIME] == const.VAL_DAYTIME_NIGHT:
                ds = 22
            elif df.at[0, const.DFROW_SUSPENSION] == const.VAL_SUSP_HIGH and \
                    df.at[0, const.DFROW_ROAD] == const.VAL_ROAD_RURAL and \
                    df.at[0, const.DFROW_WEATHER] == const.VAL_WEATHER_RAIN and \
                    df.at[0, const.DFROW_DAYTIME] == const.VAL_DAYTIME_NIGHT:
                ds = 23
            elif df.at[0, const.DFROW_SUSPENSION] == const.VAL_SUSP_HIGH and \
                    df.at[0, const.DFROW_ROAD] == const.VAL_ROAD_CITY and \
                    df.at[0, const.DFROW_WEATHER] == const.VAL_WEATHER_RAIN and \
                    df.at[0, const.DFROW_DAYTIME] == const.VAL_DAYTIME_NIGHT:
                ds = 24
        return ds
