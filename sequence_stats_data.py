"""
Module manages the generation of the statistics sheets in kpi report
Patryk Leszowski
APTIV
ADCAM MID
CALIBRATION
"""
import logging
import const
import pandas
import df_filter
import calc_functions as cf
from excel_printer import ExcelPrinter


class StatsData:
    """
    Class handles the generation of the Full Data, Road, Time of Day, and Weather sheets in kpi report
    """

    def __init__(self, data_frame_dict, path_report, report_file_name, filter_param=None, param_val=None):
        self.data_frame_dict = data_frame_dict
        self.filter_param = filter_param
        self.param_val = param_val
        self.start_row = 0
        self.gt_id_list = []
        self.accuracy_af_df_dict = {}
        self.accuracy_spc_df_dict = {}
        self.spc_time_dist_df = pandas.DataFrame()
        self.spc_dist_vf_df = pandas.DataFrame()
        self.c2w_time_dist_df = pandas.DataFrame()
        self.ratios_df = pandas.DataFrame()
        self.xls = ExcelPrinter(path_report, report_file_name, '')
        self.logger = logging.getLogger(__name__)
        self.get_stats_dfs()

    def new_sheet(self, sheet_name):
        """
        :param sheet_name: name of new sheet
        :return: None
        Method adds new sheet in ExcelSheetGenerator
        """
        self.xls.sheet_name = sheet_name
        self.start_row = 0

    def set_filter_params(self, filter_param, param_val):
        """
        :param filter_param: dataframe column
        :param param_val: value in column
        :return: None
        Method to set the filter parameter and value
        """
        self.filter_param = filter_param
        self.param_val = param_val

    def reset_filter_params(self):  # currently not used
        """
        :return: None
        Method to reset the filter parameter and value
        """
        self.filter_param = None
        self.param_val = None

    def get_stats_dfs(self):
        """
        :return: None
        Method to get the desired dataframe from the dataframe dictionary
        """
        if 'accuracy_spc_df_dict' in self.data_frame_dict:
            self.accuracy_spc_df_dict = self.data_frame_dict['accuracy_spc_df_dict']
        else:
            self.logger.warning('accuracy_spc_df_dict not in self.data_frame_dict')

        if 'accuracy_af_df_dict' in self.data_frame_dict:
            self.accuracy_af_df_dict = self.data_frame_dict['accuracy_af_df_dict']
        else:
            self.logger.warning('accuracy_af_df_dict not in self.data_frame_dict')

        if 'spc_time_dist_df' in self.data_frame_dict:
            self.spc_time_dist_df = self.data_frame_dict['spc_time_dist_df']
        else:
            self.logger.warning('spc_time_dist_df not in self.data_frame_dict')

        if 'spc_dist_vf_df' in self.data_frame_dict:
            self.spc_dist_vf_df = self.data_frame_dict['spc_dist_vf_df']
        else:
            self.logger.warning('spc_dist_vf_df not in self.data_frame_dict')

        if 'c2w_time_dist_df' in self.data_frame_dict:
            self.c2w_time_dist_df = self.data_frame_dict['c2w_time_dist_df']
        else:
            self.logger.warning('c2w_time_dist_df not in self.data_frame_dict')

        if 'ratios_df' in self.data_frame_dict:
            self.ratios_df = self.data_frame_dict['ratios_df']
        else:
            self.logger.warning('ratios_df not in self.data_frame_dict')

    def export_stats_data(self, step_info, filter_adcam_default_config=True):
        """
        :return: None
        Sequence to generate data table in an Excel sheet
        """
        self.calib_mbly_state_ratio()
        self.start_row += 1

        self.vision_conv_dist()
        self.start_row += 1

        self.spc_conv_dist()
        self.start_row += 1

        self.spc_conv_valid_frame_dist()
        self.start_row += 1

        if self.accuracy_af_df_dict:
            self.accuracy_error(self.accuracy_af_df_dict, filter_adcam_default_config=filter_adcam_default_config)
        else:
            self.logger.warning(f'accuracy_af_df_dict is empty. Cannot generate \'Accuracy error\' on {step_info}')
        self.start_row += 2

        if self.accuracy_spc_df_dict:
            self.accuracy_error(self.accuracy_spc_df_dict, app_mode='SPC')
        else:
            self.logger.warning(f'accuracy_spc_df_dict is empty. Cannot generate \'Accuracy error\' on {step_info}')
        self.start_row += 2

    def calib_mbly_state_ratio(self):
        """
        :return: None
        Method to get Calibrated MBLY state rate ratio data
        """
        df = self.ratios_df.copy()
        # Calibrated MBLY state rate ratio
        if df.shape[0] > 0:
            # Filter param = param_val, if passed. ex. Daytime = Night
            if self.filter_param is not None:
                df = self.ratios_df[self.ratios_df[self.filter_param] == self.param_val]
        calib_state_ratio_dict = {
            'Calibrated MBLY state rate ratio': cf.calc_calib_mbly_state_ratio_df(df)}
        try:
            self.start_row = self.xls.export_to_excel(calib_state_ratio_dict, self.start_row, None, self.param_val)
        except TypeError:
            self.logger.error('data_dict wrong type')

    def vision_conv_dist(self):
        """
        :return: None
        Method to get Vision convergence distance data
        """
        df = self.c2w_time_dist_df.copy()
        # Vision convergence distance
        if df.shape[0] > 0:
            # Filter param = param_val, if passed. ex. Daytime = Night
            if self.filter_param is not None:
                df = self.c2w_time_dist_df[self.c2w_time_dist_df[self.filter_param] == self.param_val]
        dist_to_conv_dict = {'Vision convergence distance - all frames': cf.calc_min_med_ave_max_df(df, const.DFROW_DIST_TO_CONV)}
        try:
            self.start_row = self.xls.export_to_excel(dist_to_conv_dict, self.start_row, None, self.param_val)
        except TypeError:
            self.logger.error('data_dict wrong type')

    def spc_conv_dist(self):
        """
        :return: None
        Method to get SPC convergence distance data
        """
        df = self.spc_time_dist_df.copy()
        # SPC convergence distance
        if df.shape[0] > 0:
            # Filter param = param_val, if passed. ex. Daytime = Night
            if self.filter_param is not None:
                df = self.spc_time_dist_df[self.spc_time_dist_df[self.filter_param] == self.param_val]
        dist_to_conv_dict = {
            'SPC convergence distance - all frames':
                cf.calc_min_med_ave_max_df(df, const.DFROW_DIST_TO_CONV)}
        try:
            self.start_row = self.xls.export_to_excel(dist_to_conv_dict, self.start_row, None, self.param_val)
        except TypeError:
            self.logger.error('data_dict wrong type')

    def spc_conv_valid_frame_dist(self):
        """
        :return: None
        Method to get SPC convergence distance valid frames data
        """
        df = self.spc_dist_vf_df.copy()
        # SPC convergence distance valid frames
        if df.shape[0] > 0:
            # Filter param = param_val, if passed. ex. Daytime = Night
            if self.filter_param is not None:
                df = self.spc_dist_vf_df[self.spc_dist_vf_df[self.filter_param] == self.param_val]
        dist_to_conv_dict = {
            'SPC convergence distance - valid frames':
                cf.calc_min_med_ave_max_df(df, const.DFROW_DIST_TO_CONV)}
        try:
            self.start_row = self.xls.export_to_excel(dist_to_conv_dict, self.start_row, None, self.param_val)
        except TypeError:
            self.logger.error('data_dict wrong type')

    def accuracy_error(self, accuracy_df_dict, app_mode='Vision', filter_adcam_default_config=True):
        """
        :return: None
        Method to Accuracy error data
        """
        df_dict = accuracy_df_dict.copy()
        if const.PROJECT_CONFIG == const.ADCAM and filter_adcam_default_config:
            # filter only (0, 0, 0, default suspension) for ADCAM, else dont filter for CARIAD
            for k in accuracy_df_dict:
                df_dict[k] = df_filter.copy_rows__bracket_df(accuracy_df_dict[k], 0, 0, 0, const.VAL_SUSP_DEFAULT)
        # filter for param
        if self.filter_param is not None:
            for k in accuracy_df_dict:
                df_dict[k] = df_dict[k][df_dict[k][self.filter_param] == self.param_val]
        try:
            std_dev_dict = {app_mode + ' Accuracy - standard deviation': cf.calc_accuracy_std_dev(df_dict)}
            if app_mode == 'Vision':
                two_sigma_dict = {'Vision Accuracy - Percentile 95.5': cf.calc_2sigma_weighed(df_dict)}
                # two_sigma_dict = {'Vision Accuracy - Percentile 95.5': cf.calc_2sigma(df_dict)}
            else:
                two_sigma_dict = {'SPC Accuracy - Percentile 95.5': cf.calc_2sigma(df_dict)}
            abs_max_dict = {app_mode + 'Accuracy - absolute maximum': cf.calc_accuracy_max(df_dict)}
            abs_min_dict = {app_mode + 'Accuracy - absolute minimum': cf.calc_accuracy_min(df_dict)}
            median_dict = {app_mode + 'Accuracy - median': cf.calc_accuracy_med(df_dict)}
            # average_dict = {app_mode + 'Accuracy - average': cf.calc_accuracy_ave(df_dict)}
            average_dict = {app_mode + 'Accuracy - average': cf.calc_cam_pose_ave(df_dict)}
            self.start_row = self.xls.export_to_excel(std_dev_dict, self.start_row, None, self.param_val)
            self.start_row += 1
            self.start_row = self.xls.export_to_excel(two_sigma_dict, self.start_row, None, self.param_val)
            self.start_row += 1
            self.start_row = self.xls.export_to_excel(abs_max_dict, self.start_row, None, self.param_val)
            self.start_row += 1
            self.start_row = self.xls.export_to_excel(abs_min_dict, self.start_row, None, self.param_val)
            self.start_row += 1
            self.start_row = self.xls.export_to_excel(median_dict, self.start_row, None, self.param_val)
            self.start_row += 1
            self.start_row = self.xls.export_to_excel(average_dict, self.start_row, None, self.param_val)
        except TypeError:
            self.logger.error('data_dict wrong type')
        except KeyError as e:
            self.logger.error('Key Error')
            self.logger.exception(e)

    def export_stats_full_data(self):
        """
        :return: None
        Method adds Full Data sheet to the kpi report
        """
        # Full Data
        self.new_sheet('Stats Full Data')
        self.export_stats_data('Stats - Full Data')

    def export_stats_road_data(self):
        """
        :return: None
        Method adds Road sheet to the kpi report
        """
        # Road
        self.new_sheet('Stats Road')
        # Road = Highway
        self.logger.info('Stats Road = Highway start')
        self.set_filter_params(const.DFROW_ROAD, const.VAL_ROAD_HIGHWAY)
        self.export_stats_data('Stats - Road - Highway')
        # Road = Rural
        self.logger.info('Stats Road = Rural start')
        self.set_filter_params(const.DFROW_ROAD, const.VAL_ROAD_RURAL)
        self.export_stats_data('Stats - Road - Rural')
        # Road = City
        self.logger.info('Stats Road = City start')
        self.set_filter_params(const.DFROW_ROAD, const.VAL_ROAD_CITY)
        self.export_stats_data('Stats - Road - City')

    def export_stats_daytime_data(self):
        """
        :return: None
        Method adds Time of Day sheet to the kpi report
        """
        # Time of Day
        self.new_sheet('Stats Time of Day')
        # Time of Day = Day
        self.logger.info('Daytime = Day start')
        self.set_filter_params(const.DFROW_DAYTIME, const.VAL_DAYTIME_DAY)
        self.export_stats_data('Stats - Time of Day - Day')
        # Time of Day = Night
        self.logger.info('Daytime = Night start')
        self.set_filter_params(const.DFROW_DAYTIME, const.VAL_DAYTIME_NIGHT)
        self.export_stats_data('Stats - Time of Day - Night')

    def export_stats_weather_data(self):
        """
        :return: None
        Method adds Weather sheet to the kpi report
        """
        # Weather
        self.new_sheet('Stats Weather')
        # Weather = Clear
        self.logger.info('Stats Weather = Clear start')
        self.set_filter_params(const.DFROW_WEATHER, const.VAL_WEATHER_CLEAR)
        self.export_stats_data('Stats - Weather - Clear')
        # Weather = Rain
        self.logger.info('Stats Weather = Rain start')
        self.set_filter_params(const.DFROW_WEATHER, const.VAL_WEATHER_RAIN)
        self.export_stats_data('Stats - Weather - Rain')
        # Weather = Snow
        self.logger.info('Stats Weather = Snow start')
        self.set_filter_params(const.DFROW_WEATHER, const.VAL_WEATHER_SNOW)
        self.export_stats_data('Stats - Weather - Snow')
        # Weather = Fog
        self.logger.info('Stats Weather = Fog start')
        self.set_filter_params(const.DFROW_WEATHER, const.VAL_WEATHER_FOG)
        self.export_stats_data('Stats - Weather - Fog')

    def export_stats_gt_data(self):
        """
        :return: None
        Method adds gt data sheet to the kpi report
        """
        if self.ratios_df.shape[0] > 0:
            # GT
            self.new_sheet('Stats per GT')
            # Get unique GT_ID list
            gt_id_list = self.ratios_df[const.DFROW_GT_ID].unique().astype(int).tolist()
            gt_id_list.sort()
            self.gt_id_list = gt_id_list
            gt_id_count = len(gt_id_list)
            self.logger.info(f'export_stats_gt_data found {gt_id_count} GT_IDs')
            if gt_id_list:
                for gt_id in gt_id_list:
                    self.logger.info(f'GT ID = {gt_id}')
                    self.set_filter_params(const.DFROW_GT_ID, gt_id)
                    self.export_stats_data(f'GT ID {gt_id}', filter_adcam_default_config=False)
            else:
                self.logger.warning('No GT_ID in ratios_df')
        else:
            self.logger.warning('empty dataframe ratios_df - cant make sheet: Stats per GT')
