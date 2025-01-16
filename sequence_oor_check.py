"""
Module manages the OOR check for CLB_C2W_State
Patryk Leszowski
APTIV
ADCAM MID
CALIBRATION
"""
import logging
import pandas
import serializer
import const
import df_filter
import calc_functions as cf
from excel_printer import ExcelPrinter
from sequence_kpi import KpiSequence


class OORCheck(KpiSequence):
    """
    Class handles the out of range check for signal CLB_C2W_State
    Checks if signal CLB_C2W_State values are equal to OOR
    Counts distance driven in OOR state
    """

    def __init__(self, measurement_pickle_list, path_report, report_file_name):
        self.measurement_pickle_list = measurement_pickle_list
        self.drives_with_oor_counter = 0
        self.oor_info_dict = {'oor_counter': 0, 'oor_distance': 0, 'oor_time': 0}
        self.df_obj = pandas.DataFrame()
        self.clb_c2w_state_oor_df = pandas.DataFrame()
        self.xls = ExcelPrinter(path_report, report_file_name, 'OOR check')
        super().__init__(self.df_obj, const.S_PARAMS_OOR, [self.xls])
        self.logger = logging.getLogger(__name__)

    def get_oor_df(self):
        """
        :return: None
        Method gets dataframe with OOR data
        """
        self.logger.info(f'OOR Check')
        temp_df_list = []
        for file in self.measurement_pickle_list:
            # get measurement dataframe
            df = serializer.load_pkl(const.MEASUREMENT_DFS_FOLDER, file)
            # drop rows where CLB_C2W_STATE is none
            df = df[pandas.notnull(df[const.SIG_CLB_C2W_STATE])]
            # if dataframe is empty, go to next measurement
            if df.shape[0] == 0:
                continue
            # check if there is an occurrence of OOR in CLB_C2W_State column
            clb_c2w_status_values = set(df[const.SIG_CLB_C2W_STATE])
            # keep only columns relevant to OOR
            df = df[const.OOR_INFO].reset_index(drop=True)
            # get total distance of measurement
            total_distance = df[const.DFROW_DISTANCE].iloc[-1]
            # add column with total distance
            df['Total Distance'] = [total_distance] * df.shape[0]
            # if OOR occurred in measurement
            if const.VAL_C2W_STATE_OOR in clb_c2w_status_values:
                # get dataframe where CLB_C2W_State = OOR
                df_oor = df_filter.copy_rows__col_eq_val_df(df, const.SIG_CLB_C2W_STATE, const.VAL_C2W_STATE_OOR)
                # get dataframe where change to CLB_C2W_State = OOR occurs
                df_oor = df_filter.copy_rows__index_boundary_val_df(df_oor)
                # if number of indexes is not even, drop last row
                if df_oor.shape[0] % 2 != 0:
                    df_oor = df_oor[:-1]
                if df_oor.shape[0] > 0:
                    # get dataframe with delta time and delta distance
                    df_calc = cf.calc_delta_time_dist_df(df_oor, time_col='OOR Time', dist_col='OOR Distance')
                    # add to dataframe list
                    temp_df_list.append(df_calc)
                    self.drives_with_oor_counter += 1
                else:
                    self.logger.warning(f'OOR detected in {file}, but dataframe is empty after filtering')
            else:
                # get first row from df
                df_1st_row = df.iloc[[0]].reset_index(drop=True)
                df_1st_row[const.DFROW_OOR_TIME] = 0.0
                df_1st_row[const.DFROW_OOR_DISTANCE] = 0.0
                temp_df_list.append(df_1st_row)

        self.df_obj = pandas.concat(temp_df_list, ignore_index=True).drop(
            columns=[const.DFROW_TIMESTAMP, const.DFROW_DISTANCE])
        self.clb_c2w_state_oor_df = df_filter.copy_rows__col_eq_val_df(self.df_obj, const.SIG_CLB_C2W_STATE, const.VAL_C2W_STATE_OOR)
        self.oor_info_dict['oor_counter'] = self.drives_with_oor_counter
        self.oor_info_dict['oor_distance'] = self.clb_c2w_state_oor_df[const.DFROW_OOR_DISTANCE].sum()
        self.oor_info_dict['oor_time'] = self.clb_c2w_state_oor_df[const.DFROW_OOR_TIME].sum()

        if self.drives_with_oor_counter > 0:
            self.logger.info(f'There was {self.drives_with_oor_counter} drives with OOR occurrences')
        else:
            self.logger.info(f'There was no OOR occurrences in any drive')
