"""
Module manages the generation of the Dataframes workbook
Patryk Leszowski
APTIV
ADCAM MID
CALIBRATION
"""
from excel_printer import ExcelPrinter
import logging


class DFsToExcel:

    def __init__(self, sequence_dict, path_report, report_file_name):
        self.logger = logging.getLogger(__name__)
        self.sequence_dict = sequence_dict
        self.ex_print = ExcelPrinter(path_report, report_file_name, '')

    def print_df(self, dataframe, dataframe_name):
        if dataframe.shape[0] > 0:
            if dataframe.shape[0] > 65535:
                dataframe = dataframe[0:65535]
                self.logger.warning(f'DF {dataframe_name} is longer than 65535')
            self.ex_print.df_to_excel(dataframe)

    def export_dfs_to_excel(self):

        if 'accuracy_spc_df_dict' in self.sequence_dict:
            self.ex_print.sheet_name = 'spc_accuracy_yaw'
            df = self.sequence_dict['accuracy_spc_df_dict']['Delta Yaw']
            self.print_df(df, 'spc_accuracy_yaw delta yaw')
            self.ex_print.sheet_name = 'spc_accuracy_pitch'
            df = self.sequence_dict['accuracy_spc_df_dict']['Delta Pitch']
            self.print_df(df, 'spc_accuracy_pitch delta pitch')
            self.ex_print.sheet_name = 'spc_accuracy_roll'
            df = self.sequence_dict['accuracy_spc_df_dict']['Delta Roll']
            self.print_df(df, 'spc_accuracy_roll delta roll')
            self.ex_print.sheet_name = 'spc_accuracy_height'
            df = self.sequence_dict['accuracy_spc_df_dict']['Delta Height']
            self.print_df(df, 'spc_accuracy_height delta height')
        if 'accuracy_af_df_dict' in self.sequence_dict:
            self.ex_print.sheet_name = 'af_accuracy_yaw'
            df = self.sequence_dict['accuracy_af_df_dict']['Delta Yaw']
            self.print_df(df, 'af_accuracy_yaw delta yaw')
            self.ex_print.sheet_name = 'af_accuracy_pitch'
            df = self.sequence_dict['accuracy_af_df_dict']['Delta Pitch']
            self.print_df(df, 'af_accuracy_pitch delta pitch')
            self.ex_print.sheet_name = 'af_accuracy_roll'
            df = self.sequence_dict['accuracy_af_df_dict']['Delta Roll']
            self.print_df(df, 'af_accuracy_roll delta roll')
            self.ex_print.sheet_name = 'af_accuracy_height'
            df = self.sequence_dict['accuracy_af_df_dict']['Delta Height']
            self.print_df(df, 'af_accuracy_height delta height')
        if 'accuracy_af_uv_df_dict' in self.sequence_dict:
            self.ex_print.sheet_name = 'af_uv_accuracy_yaw'
            df = self.sequence_dict['accuracy_af_uv_df_dict']['Delta Yaw']
            self.print_df(df, 'af_uv_accuracy_yaw delta yaw')
            self.ex_print.sheet_name = 'af_uv_accuracy_pitch'
            df = self.sequence_dict['accuracy_af_uv_df_dict']['Delta Pitch']
            self.print_df(df, 'af_uv_accuracy_pitch delta pitch')
            self.ex_print.sheet_name = 'af_uv_accuracy_roll'
            df = self.sequence_dict['accuracy_af_uv_df_dict']['Delta Roll']
            self.print_df(df, 'af_uv_accuracy_roll delta roll')
            self.ex_print.sheet_name = 'af_uv_accuracy_height'
            df = self.sequence_dict['accuracy_af_uv_df_dict']['Delta Height']
            self.print_df(df, 'af_uv_accuracy_height delta height')
        if 'accuracy_af_sp_df_dict' in self.sequence_dict:
            self.ex_print.sheet_name = 'af_sp_accuracy_yaw'
            df = self.sequence_dict['accuracy_af_sp_df_dict']['Delta Yaw']
            self.print_df(df, 'af_sp_accuracy_yaw delta yaw')
            self.ex_print.sheet_name = 'af_sp_accuracy_pitch'
            df = self.sequence_dict['accuracy_af_sp_df_dict']['Delta Pitch']
            self.print_df(df, 'af_sp_accuracy_pitch delta pitch')
            self.ex_print.sheet_name = 'af_sp_accuracy_roll'
            df = self.sequence_dict['accuracy_af_sp_df_dict']['Delta Roll']
            self.print_df(df, 'af_sp_accuracy_roll delta roll')
            self.ex_print.sheet_name = 'af_sp_accuracy_height'
            df = self.sequence_dict['accuracy_af_sp_df_dict']['Delta Height']
            self.print_df(df, 'af_sp_accuracy_height delta height')
        if 'spc_time_dist_df' in self.sequence_dict:
            self.ex_print.sheet_name = 'spc_time_distance'
            df = self.sequence_dict['spc_time_dist_df']
            self.print_df(df, 'spc_time_dist_df')
        if 'spc_dist_vf_df' in self.sequence_dict:
            self.ex_print.sheet_name = 'spc_time_dist_valid_frames'
            df = self.sequence_dict['spc_dist_vf_df']
            self.print_df(df, 'spc_dist_vf_df')
        if 'spc_not_conv_df' in self.sequence_dict:
            self.ex_print.sheet_name = 'spc_not_converged'
            df = self.sequence_dict['spc_not_conv_df']
            self.print_df(df, 'spc_not_conv_df')
        if 'c2w_time_dist_df' in self.sequence_dict:
            self.ex_print.sheet_name = 'vision_time_distance'  # from 1st c2w frame to calibrated
            df = self.sequence_dict['c2w_time_dist_df']
            self.print_df(df, 'c2w_time_dist_df')
        if 'dist_to_af_df' in self.sequence_dict:
            self.ex_print.sheet_name = 'time_distance_to_vision'  # from 1st spc frame to 1st c2w frame
            df = self.sequence_dict['dist_to_af_df']
            self.print_df(df, 'dist_to_af_df')
        if 'c2w_not_conv_df' in self.sequence_dict:
            self.ex_print.sheet_name = 'c2w_not_converged'
            df = self.sequence_dict['c2w_not_conv_df']
            self.print_df(df, 'c2w_not_conv_df')
        if 'switch_app_time_df' in self.sequence_dict:
            self.ex_print.sheet_name = 'switch_app_time'
            df = self.sequence_dict['switch_app_time_df']
            self.print_df(df, 'switch_app_time_df')
        if 'c2w_frame_drops_df' in self.sequence_dict:
            self.ex_print.sheet_name = 'c2w_frame_drops'
            df = self.sequence_dict['c2w_frame_drops_df']
            self.print_df(df, 'c2w_frame_drops_df')
        if 'spc_frame_drops_df' in self.sequence_dict:
            self.ex_print.sheet_name = 'spc_frame_drops'
            df = self.sequence_dict['spc_frame_drops_df']
            self.print_df(df, 'spc_frame_drops_df')
        if 'ratios_df' in self.sequence_dict:
            self.ex_print.sheet_name = 'calibration_state_ratios'
            df = self.sequence_dict['ratios_df']
            self.print_df(df, 'ratios_df')
        if 'dc_ratios_df' in self.sequence_dict:
            self.ex_print.sheet_name = 'degrade_cause_ratios'
            df = self.sequence_dict['dc_ratios_df']
            self.print_df(df, 'dc_ratios_df')
        if 'spc_status_ratio_df' in self.sequence_dict:
            self.ex_print.sheet_name = 'spc_status_ratios'
            df = self.sequence_dict['spc_status_ratio_df']
            self.print_df(df, 'spc_status_ratio_df')
        if 'spc_error_ratio_df' in self.sequence_dict:
            self.ex_print.sheet_name = 'spc_error_ratios'
            df = self.sequence_dict['spc_error_ratio_df']
            self.print_df(df, 'spc_error_ratio_df')
        if 'nvm_persistence_df' in self.sequence_dict:
            self.ex_print.sheet_name = 'nvm_persistence_df'
            df = self.sequence_dict['nvm_persistence_df']
            self.print_df(df, 'nvm_persistence_df')
        if 'nvm_check_failed_df' in self.sequence_dict:
            self.ex_print.sheet_name = 'nvm_check_failed_df'
            df = self.sequence_dict['nvm_check_failed_df']
            self.print_df(df, 'nvm_check_failed_df')
        if 'crc_reset_counter_df' in self.sequence_dict:
            self.ex_print.sheet_name = 'crc_reset_counter_df'
            df = self.sequence_dict['crc_reset_counter_df']
            self.print_df(df, 'crc_reset_counter_df')
        if 'clb_c2w_state_oor_df' in self.sequence_dict:
            self.ex_print.sheet_name = 'clb_c2w_state_oor_df'
            df = self.sequence_dict['clb_c2w_state_oor_df']
            self.print_df(df, 'clb_c2w_state_oor_df')
        if 'velocity_df' in self.sequence_dict:
            self.ex_print.sheet_name = 'velocity_df'
            df = self.sequence_dict['velocity_df']
            self.print_df(df, 'velocity_df')
        self.ex_print.sheet_name = 'frame_counters'
        current_row = 0
        if 'accuracy_spc_ff_df' in self.sequence_dict:
            df = self.sequence_dict['accuracy_spc_ff_df']
            ff_dict = df.to_dict(orient='index')
            current_row = self.ex_print.export_to_excel(ff_dict, current_row, cell_0_0="SPC Frames")
            current_row += 2
        if 'accuracy_af_ff_df' in self.sequence_dict:
            df = self.sequence_dict['accuracy_af_ff_df']
            ff_dict = df.to_dict(orient='index')
            current_row = self.ex_print.export_to_excel(ff_dict, current_row, cell_0_0="Calibrated Frames")
            current_row += 2
        if 'accuracy_af_uv_ff_df' in self.sequence_dict:
            df = self.sequence_dict['accuracy_af_uv_ff_df']
            ff_dict = df.to_dict(orient='index')
            current_row = self.ex_print.export_to_excel(ff_dict, current_row, cell_0_0="Unvalidated Frames")
            current_row += 2
        if 'accuracy_af_sp_ff_df' in self.sequence_dict:
            df = self.sequence_dict['accuracy_af_sp_ff_df']
            ff_dict = df.to_dict(orient='index')
            self.ex_print.export_to_excel(ff_dict, current_row, cell_0_0="Suspected Frames")
