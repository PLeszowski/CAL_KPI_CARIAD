"""
Module manages the kpi report sheet generation sequences
Patryk Leszowski
APTIV
ADCAM MID
CALIBRATION
"""
import logging
from measurements import Measurements
from sequence_accuracy import AccuracySequence
from sequence_af_dist import AFDistanceSequence
from sequence_calib_state_ratio import CalibStateRatioSequence
from sequence_dergade_cause_ratio import DegradeCauseRatioSequence
from sequence_stats_data import StatsData
from partial_results import PartialResults
from sequence_oor_check import OORCheck
from sequence_dfs_to_excel import DFsToExcel
from sequence_not_converged import NotConvergedSequence
from sequence_velocity import VelocitySequence
from ave_hw_pose import HighwayPose
import serializer


class Sequencer:
    """
    Facade to manage the kpi report generation sequences
    """

    def __init__(self, args_dict):
        self.pkl_dict_folder = args_dict['pkl_dict_folder']
        self.pkl_dict_ext = args_dict['pkl_dict_ext']
        self.path_report = args_dict['path_report']
        self.report_file_name = args_dict['report_file_name']
        self.partial_results_folder = args_dict['partial_results_folder']
        self.sw_package_num = args_dict['sw_package_num']
        self.path_gt_data = args_dict['path_gt_data']
        self.gt_id_list = []
        self.sequence_dict = {}
        self.logger = logging.getLogger(__name__)
        self.__me = Measurements(args_dict)

    def run_kpi_sequence(self):
        """
        :return: None
        Method runs kpi report sheet generation sequences
        """
        if self.__me.measurement_id_list.size > 0:
            self.logger.info('running test sequences')
            self.get_measurement_data()
            self.run_accuracy_af_cal_sequence()
            self.run_accuracy_af_unv_sequence()
            self.run_accuracy_af_sus_sequence()
            self.run_time_and_dist_c2w_sequence()
            self.run_calib_state_ratio_c2w_sequence()
            self.run_degrade_cause_ratio_sequence()
            self.run_oor_check()
            self.run_partial_results()
            self.run_not_converged_sequence()
            self.run_velocity_sequence()
            self.run_stats_sequence()
            self.get_gt_hw_data()
            self.save_sequence_dict()
            # self.load_sequence_dict()
            self.add_ds_collected_data_info()
            self.dataframes_to_excel()
            self.end_script()
        else:
            self.logger.error('No measurement pickles in me.measurement_id_list')

    def get_measurement_ids(self):
        """
        :return: None
        Method calls Measurements class methods to change split pickles to measurement pickles
        """
        self.logger.info('getting dataframe list')
        self.__me.get_measurement_ids()

    def splits_to_measurements(self):
        """
        :return: None
        Method calls Measurements class methods to change split pickles to measurement pickles
        """
        self.logger.info('getting dataframe list')
        self.__me.make_measurement_df_pickles()

    def save_dataframes_list(self):
        """
        :return: None
        Method saves the list of dataframes from Measurements class to a pickle file
        """
        self.logger.info('saving dataframe list')
        serializer.save_pkl(self.__me.dataframe_list, self.pkl_dict_folder, 'dataframe_list')

    def load_dataframes_list(self):
        """
        :return: None
        Method loads the list of dataframes to Measurements class from a pickle file
        """
        self.logger.info('loading dataframe list')
        self.__me.dataframe_list = serializer.load_pkl(self.pkl_dict_folder, 'dataframe_list.pkl')

    def save_sequence_dict(self):
        """
        :return: None
        Method saves the sequence_dict to a pickle file
        """
        self.logger.info('saving sequence_dict')
        serializer.save_pkl(self.sequence_dict, self.pkl_dict_folder, 'sequence_dict')

    def load_sequence_dict(self):
        """
        :return: None
        Method loads sequence_dict from a pickle file
        """
        self.logger.info('loading sequence_dict')
        self.sequence_dict = serializer.load_pkl(self.pkl_dict_folder, 'sequence_dict.pickle')

    def get_measurement_data(self):
        """
        :return: None
        Method calls Measurements class method get_measurement_data()
        """
        self.__me.get_measurement_data()

    def add_ds_collected_data_info(self):
        """
        :return: None
        Method calls Measurements class method export_ds_collected_data_info()
        """
        self.__me.export_ds_collected_data_info()

    def run_accuracy_af_cal_sequence(self):
        """
        :return: None
        Method manages the generation of the Accuracy Vision Calibrated sheet in kpi report
        """
        # Accuracy Vision Calibrated
        if self.__me.measurement_id_list.size > 0:
            self.logger.info('Accuracy Vision Calibrated sequence start')
            acc = AccuracySequence(self.__me.measurement_id_list, self.path_report, self.report_file_name)
            acc.get_accuracy_df()
            acc.get_failed_frames()
            acc.export_kpi()
            self.sequence_dict['accuracy_af_df_dict'] = acc.df_obj
            self.sequence_dict['accuracy_af_ff_df'] = acc.df_failed_frames
            del acc
        else:
            self.logger.error('No measurement pickles in me.measurement_id_list')

    def run_accuracy_af_unv_sequence(self):
        """
        :return: None
        Method manages the generation of the Accuracy Vision Unvalidated sheet in kpi report
        """
        # Accuracy Vision Unvalidated
        if self.__me.measurement_id_list.size > 0:
            self.logger.info('Accuracy Vision Unvalidated sequence start')
            acc = AccuracySequence(self.__me.measurement_id_list, self.path_report, self.report_file_name, calib_sts='Unvalidated')
            acc.get_accuracy_df()
            acc.get_failed_frames()
            acc.export_kpi()
            self.sequence_dict['accuracy_af_uv_df_dict'] = acc.df_obj
            self.sequence_dict['accuracy_af_uv_ff_df'] = acc.df_failed_frames
            del acc
        else:
            self.logger.error('No measurement pickles in me.measurement_id_list')

    def run_accuracy_af_sus_sequence(self):
        """
        :return: None
        Method manages the generation of the Accuracy Vision Suspected sheet in kpi report
        """
        # Accuracy Vision Unvalidated
        if self.__me.measurement_id_list.size > 0:
            self.logger.info('Accuracy Vision Suspected sequence start')
            acc = AccuracySequence(self.__me.measurement_id_list, self.path_report, self.report_file_name, calib_sts='Suspected')
            acc.get_accuracy_df()
            acc.get_failed_frames()
            acc.export_kpi()
            self.sequence_dict['accuracy_af_sp_df_dict'] = acc.df_obj
            self.sequence_dict['accuracy_af_sp_ff_df'] = acc.df_failed_frames
            del acc
        else:
            self.logger.error('No measurement pickles in me.measurement_id_list')

    def run_time_and_dist_c2w_sequence(self):
        """
        :return: None
        Method manages the generation of the Time and Distance to vision sheets in kpi report
        """
        # Time and distance to C2W
        if self.__me.measurement_id_list.size > 0:
            self.logger.info('Time and distance to C2W sequence start')
            c2w = AFDistanceSequence(self.__me.measurement_id_list, self.path_report, self.report_file_name)
            c2w.get_c2w_time_and_dist_df()
            c2w.export_kpi()
            self.sequence_dict['c2w_time_dist_df'] = c2w.af_time_dist_df
            self.sequence_dict['c2w_not_conv_df'] = c2w.not_convereged_df
            self.sequence_dict['c2w_frame_drops_df'] = c2w.frame_drops_df
            self.__me.c2w_not_converged_counter = c2w.c2w_not_converged_counter
            self.__me.c2w_no_vision_counter = c2w.c2w_no_vision_counter
            del c2w
        else:
            self.logger.error('No measurement pickles in me.measurement_id_list')

    def run_calib_state_ratio_c2w_sequence(self):
        """
        :return: None
        Method manages the generation of the Calibration State Ratio sheet in kpi report
        """
        # Calibration State Ratio
        if self.__me.measurement_id_list.size > 0:
            self.logger.info('Calibration State Ratio sequence start')
            csr = CalibStateRatioSequence(self.__me.measurement_id_list, self.path_report, self.report_file_name)
            csr.get_vision_df()
            csr.export_kpi()
            self.sequence_dict['ratios_df'] = csr.df_obj
            del csr
        else:
            self.logger.error('No measurement pickles in me.measurement_id_list')

    def run_degrade_cause_ratio_sequence(self):
        """
        :return: None
        Method manages the generation of the Degrade Cause Ratio sheet in kpi report
        """
        # Degrade Cause Ratio
        if self.__me.measurement_id_list.size > 0:
            self.logger.info('Degrade Cause Ratio sequence start')
            dcr = DegradeCauseRatioSequence(self.__me.measurement_id_list, self.path_report, self.report_file_name)
            dcr.get_vision_df()
            dcr.export_kpi()
            self.sequence_dict['dc_ratios_df'] = dcr.df_obj
            serializer.save_pkl(self.sequence_dict, self.pkl_dict_folder, 'sequence_dict')  # temp, for tests only
            del dcr
        else:
            self.logger.error('No measurement pickles in me.measurement_id_list')

    def run_oor_check(self):
        """
        :return: None
        Method manages the OOR check
        """
        oor = OORCheck(self.__me.measurement_id_list, self.path_report, self.report_file_name)
        oor.get_oor_df()
        oor.export_kpi()
        self.sequence_dict['clb_c2w_state_oor_df'] = oor.clb_c2w_state_oor_df
        self.__me.oor_info_dict = oor.oor_info_dict

    def run_velocity_sequence(self):
        """
        :return: None
        Method manages the generation of the 'Velocity Distribution' sheet in kpi report
        """
        vel = VelocitySequence(self.__me.measurement_id_list, self.path_report, self.report_file_name)
        vel.get_velocity_weighed()
        vel.export_kpi()
        self.sequence_dict['velocity_df'] = vel.df_obj

    def run_stats_sequence(self):
        """
        :return: None
        Method manages the generation of the Full Data, Road, Time of Day, and Weather sheets in kpi report
        """
        # self.sequence_dict = serializer.load_pkl(self.path_log_file, 'sequence_dict.pkl')  # temp, for tests only
        if self.sequence_dict:
            self.logger.info('Stats sequence start')
            stats = StatsData(self.sequence_dict, self.path_report, self.report_file_name)
            stats.export_stats_full_data()
            stats.export_stats_road_data()
            stats.export_stats_daytime_data()
            stats.export_stats_weather_data()
            stats.export_stats_gt_data()
            self.gt_id_list = stats.gt_id_list
        else:
            self.logger.error('No dataframes in sequence_dict')

    def run_not_converged_sequence(self):
        """
        :return: None
        Method manages the generation of the 'Not Converged' sheet in kpi report
        """
        ncs = NotConvergedSequence(self.sequence_dict, self.path_report, self.report_file_name)
        ncs.get_dictionaries()
        ncs.export_not_converged()

    def run_partial_results(self):
        """
        :return: None
        Method manages the generation of Partial Data and Staged Test case data json files
        """
        # self.sequence_dict = serializer.load_pkl(self.pkl_dict_folder, 'sequence_dict.pkl')  # temp, for tests only
        if self.sequence_dict:
            if self.__me.measurement_data_dict:
                self.logger.info('Partial results start')
                partial_results = PartialResults(self.sequence_dict, self.__me.measurement_data_dict, self.partial_results_folder)
                partial_results.partial_results_accuracy()
                partial_results.partial_results_spc_time_dist()
                partial_results.partial_results_c2w_time_dist()
                partial_results.partial_results_calib_state_ratio()
                partial_results.put_measurement_data_to_partial_results()
                partial_results.make_staged_measurements_list_json()
                partial_results.save_partial_results()
            else:
                self.logger.error('No measurement data in me.measurement_data_dict, run get_measurement_data() first')
        else:
            self.logger.error('No dataframes in sequence_df_dict')

    def get_gt_hw_data(self):
        hw_data = HighwayPose(self.sequence_dict['accuracy_af_df_dict'], self.gt_id_list)
        hw_data.get_ave_hw_pose()
        self.sequence_dict['hw_ave_pose_df'] = hw_data.ave_hw_pose_df
        self.sequence_dict['top_weights_pose_df'] = hw_data.pose_top_weights_df

    def dataframes_to_excel(self):
        """
        :return: None
        Method manages the generation the 'DFS_CAL_KPI_OLD_ALGO_REPORT_xxxx' excel workbook
        """
        dfs = DFsToExcel(self.sequence_dict, self.path_report, 'DFS_' + self.report_file_name)
        dfs.export_dfs_to_excel()

    def end_script(self):
        gt_id_count = len(self.gt_id_list)
        self.logger.info(f'GT_IDs used: {self.gt_id_list}')
        self.logger.info(f'Input to VBA macro: gt_id_count = {gt_id_count}')
        self.logger.info(f'Input to VBA macro: tr_raw_name = {self.report_file_name}')
        self.logger.info(f'input to VBA macro: Path = {self.path_report}')
        self.logger.info('Script done')

