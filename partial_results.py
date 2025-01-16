"""
Module manages partial results and staged testcase data json file generation
Patryk Leszowski
APTIV
ADCAM MID
CALIBRATION
"""
import logging
import const
import re
import os
import serializer as s
import calc_functions as cf
import datetime


class PartialResults:
    """
    Class handles tasks related to partial results and staged testcase data json file generation
    """

    def __init__(self, sequence_df_dict, measurement_data_dict, path_json):

        self.partial_results_dict = {}
        self.measurement_data_dict = measurement_data_dict
        self.path_json = path_json
        # Dataframe dictionary
        self.sequence_df_dict = sequence_df_dict
        # logger instance
        self.logger = logging.getLogger(__name__)
        self.pattern = re.compile(r'(.+)_\w{3}_.+')
        self.init_partial_results_dict()

    def init_partial_results_dict(self):
        """
        :return: None
        Method sets keys for self.partial_results_dict from self.measurement_data_dict keys
        """
        # set keys: measurement IDs (ADCAM_VIN_DATE_TIME), values: empty dictionaries
        for k in self.measurement_data_dict.keys():
            self.partial_results_dict[k] = {}

    def partial_results_accuracy(self):
        """
        :return: None
        Method gets partial accuracy data from accuracy dataframe dictionary
        """
        accuracy_partial_results_dict = {}
        self.logger.info('getting accuracy partial results')
        # get accuracy dataframe dictionary
        try:
            accuracy_df_dict = self.sequence_df_dict['accuracy_af_df_dict']
            # key df_name is the name for each accuracy dataframe, value df is the dataframe
            for df_name, df in accuracy_df_dict.items():
                # Get LogName series from dataframe
                log_name_series = df[const.DFROW_LOG_FILE]
                # keep only ADCAM_VIN_DATE_TIME in split name
                df[const.DFROW_LOG_FILE] = log_name_series.str.replace(self.pattern, lambda m: m.group(1))
                # split df into multiple dfs based on split name ADCAM_VIN_DATE_TIME, and place dfs in dictionary
                accuracy_partial_results_dict[df_name] = {k: v for k, v in df.groupby(const.DFROW_LOG_FILE)}
            # partial_results_dict keys are the IDs for each measurement ADCAM_VIN_DATE_TIME,
            # values are empty dictionaries, see def init_partial_results_dict
            for measurement_id in self.partial_results_dict.keys():
                # add key 'Accuracy', value empty dictionary, to each empty dictionary
                self.partial_results_dict[measurement_id] = {'Accuracy': {}}
                # key df_dict_name is the name of the accuracy dictionary
                # df_dict contains keys: IDs for each measurement ADCAM_VIN_DATE_TIME, values: dataframes with measurement data
                for df_dict_name, df_dict in accuracy_partial_results_dict.items():
                    if measurement_id in df_dict.keys():
                        # get dataframe
                        df = df_dict[measurement_id]
                        # get a list of accuracy values
                        accuracy_values = df[df_dict_name].tolist()
                        # get a list of value weights
                        values_weight = df['weight'].tolist()
                        # put into partial_results_dict Accuracy dictionary
                        self.partial_results_dict[measurement_id]['Accuracy'][df_dict_name] = \
                            {df_dict_name: accuracy_values, 'weight': values_weight}
            self.logger.info('got accuracy partial results')
        except KeyError:
            self.logger.error('\'accuracy_df_dict\' not in sequence_df_dict')
            pass

    def partial_results_spc_time_dist(self):
        """
        :return: None
        Method gets partial spc time and distance data from spc time and distance dataframe
        """
        self.logger.info('getting spc time and distance partial results')
        try:
            df = self.sequence_df_dict['spc_time_dist_df']
            # keep only ADCAM_VIN_DATE_TIME in split name
            df[const.DFROW_LOG_FILE] = df[const.DFROW_LOG_FILE].str.replace(self.pattern, lambda m: m.group(1))
            for k in self.partial_results_dict.keys():
                # Add key 'Distance to SPC' with value 'Distance to Converge' from df, where key(k) = LogName
                self.logger.info(f'Log: {k}')
                try:
                    self.partial_results_dict[k]['Distance to SPC'] = \
                        df.loc[df[const.DFROW_LOG_FILE] == k, 'Distance to Converge'].iloc[0]
                    # Add key 'Time to SPC' with value 'Time to Converge' from df, where key(k) = LogName
                    self.partial_results_dict[k]['Time to SPC'] = \
                        df.loc[df[const.DFROW_LOG_FILE] == k, 'Time to Converge'].iloc[0]
                except IndexError:
                    self.logger.error(f'Log name {k} not in spc_time_dist_df')
                    continue
            self.logger.info('got spc time and distance partial results')
        except KeyError:
            self.logger.error('\'spc_time_dist_df\' not in sequence_df_dict')
            pass

    def partial_results_c2w_time_dist(self):
        """
        :return: None
        Method gets partial vision time and distance data from vision time and distance dataframe
        """
        self.logger.info('getting vision time and distance partial results')
        try:
            df = self.sequence_df_dict['c2w_time_dist_df']
            # keep only ADCAM_VIN_DATE_TIME in split name
            df[const.DFROW_LOG_FILE] = df[const.DFROW_LOG_FILE].str.replace(self.pattern, lambda m: m.group(1))
            for k in self.partial_results_dict.keys():
                try:
                    # Add key 'Distance to SPC' with value 'Distance to Converge' from df, where key(k) = LogName
                    self.partial_results_dict[k]['Distance to vision'] = \
                        df.loc[df[const.DFROW_LOG_FILE] == k, 'Distance to Converge'].iloc[0]
                    # Add key 'Time to SPC' with value 'Time to Converge' from df, where key(k) = LogName
                    self.partial_results_dict[k]['Time to vision'] = \
                        df.loc[df[const.DFROW_LOG_FILE] == k, 'Time to Converge'].iloc[0]
                except IndexError:
                    self.logger.error(f'Log name {k} not in c2w_time_dist_df')
                    continue
            self.logger.info('got vision time and distance partial results')
        except KeyError:
            self.logger.error('\'c2w_time_dist_df\' not in sequence_df_dict')
            pass

    def partial_results_calib_state_ratio(self):
        """
        :return: None
        Method gets partial calibration state ratio data from vision dataframe
        """
        self.logger.info('getting calibration state ratio partial results')
        try:
            df = self.sequence_df_dict['ratios_df']
            # Get LogName series
            log_name_series = df[const.DFROW_LOG_FILE]
            # keep only ADCAM_VIN_DATE_TIME in split name
            df[const.DFROW_LOG_FILE] = log_name_series.str.replace(self.pattern, lambda m: m.group(1))
            # split df into multiple dfs based on split name ADCAM_VIN_DATE_TIME, and place dfs in dictionary
            df_dict = {k: v for k, v in df.groupby(const.DFROW_LOG_FILE)}
            # del df
            for k in self.partial_results_dict.keys():
                if k in df_dict:
                    v = df_dict[k]
                    v.reset_index(inplace=True, drop=True)
                    self.partial_results_dict[k]['Calib State Ratio'] = \
                        cf.calc_calib_state_ratio_df(v, const.SIG_CLB_C2W_STATE)
            self.logger.info('got calibration state ratio partial results')
        except KeyError:
            self.logger.error('\'ratios_df\' not in sequence_df_dict')
            pass

    def put_measurement_data_to_partial_results(self):
        """
        :return: None
        Method updates partial results dictionaries with measurement data
        (obtained by measurements.py get_measurement_data method)
        """
        for k in self.partial_results_dict.keys():
            if k in self.measurement_data_dict:
                self.logger.info(f'putting measurement_data to partial_results for {k}')
                self.partial_results_dict[k].update(self.measurement_data_dict[k])
            else:
                self.logger.warning(f'key {k} not in measurement_data_dict')

    def make_staged_measurements_list_json(self):
        """
        :return:
        Method makes a json file with per-split (testcase) info
        """
        measurement_data_list = []
        # self.measurement_data_dict holds measurement data of each measurement
        # k is the dictionary key which is the measurement name
        # v is the measurement data for that measurement
        for k, v in self.measurement_data_dict.items():
            self.logger.info(f'adding {k} measurement data to staged_measurements_list')
            measurement_data_list.append(v)
        s.save_json(measurement_data_list, self.path_json, 'CAL_STAGED_TC_DATA_LIST')

    def save_partial_results(self):
        """
        :return: None
        Method saves self.partial_results_dict dictionaries to json files
        """
        self.logger.info('saving partial results to json')
        # self.partial_results_dict holds partial data dictionaries of each measurement
        # k is the dictionary key which is the measurement name
        # v is the partial data for that measurement
        for k, v in self.partial_results_dict.items():
            filename = k + '_CAL'
            s.save_json(v, self.path_json, filename)
            # fix float decimal places in json
            self.json_float_decimal_places(3, self.path_json, filename)

        self.logger.info('saved partial results to json')

    def json_float_decimal_places(self, decimal_places, path, file):
        """
        :param decimal_places: number of decimal places to leave in json floats
        :param path: path_log_file to json file
        :param file: filename
        :return: None
        Method opens json and converts floats which have more decimal places than decimal_places to floats that have
        decimal_places decimal places:)
        """
        # make sure the number of decimal places is a string
        ds = str(decimal_places)
        # add extension .json, if there isn't one
        if '.json' not in file:
            filename = file + '.json'
        else:
            filename = file
        # open json file
        with open(path + filename, 'r') as json:
            # put each line from file into lines list (each line is a list item)
            lines = json.readlines()
        # split filename by the dot(.)
        new_file_split = filename.split('.')
        # put file back together with _new. between the name and extension and assign to new_file
        new_file = new_file_split[0] + '_new.' + new_file_split[1]
        # open new_file
        with open(path + new_file, 'w') as json:
            for line in lines:
                # in line match a digit once or more, a dot, and ds number of digits, and remember that,
                # and also match a digit once or more after that. Replace the whole match with the remembered stuff
                new_line = re.sub(r'(\d+\.\d{' + ds + r'})\d+', lambda m: m.group(1), line)
                # and write it to json new_file
                json.write(new_line)
        #  delete original file
        os.remove(self.path_json + filename)
        #  rename new_file to original file
        try:
            os.rename(self.path_json + new_file, self.path_json + filename)
        except FileExistsError as e:
            self.logger.exception(e)
            os.rename(self.path_json + new_file,
                      self.path_json + datetime.datetime.now().strftime('_%Y%m%d_%H%M%S.') + filename)
            pass
