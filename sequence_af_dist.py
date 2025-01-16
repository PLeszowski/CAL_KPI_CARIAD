"""
Module manages the generation of the Time and Distance sheets in kpi report
Patryk Leszowski
APTIV
ADCAM MID
CALIBRATION
"""
import logging
from excel_printer import ExcelPrinter
import calc_functions as cf
import df_filter
import pandas
import const
import serializer
from sequence_kpi import KpiSequence


class AFDistanceSequence(KpiSequence):
    """
    Class handles the generation of the Time and Distance to Vision sheets in kpi report
    """

    def __init__(self, measurement_pickle_list, path_report, report_file_name):
        self.measurement_pickle_list = measurement_pickle_list
        self.df_obj = pandas.DataFrame()
        self.af_time_dist_df = pandas.DataFrame()
        self.not_convereged_df = pandas.DataFrame()
        self.frame_drops_df = pandas.DataFrame()  # not implemented
        self.count_all_convergences = True
        self.c2w_not_converged_counter = 0
        self.c2w_no_vision_counter = 0
        self.xls_time = ExcelPrinter(path_report, report_file_name, 'Time to Vision')
        self.xls_dist = ExcelPrinter(path_report, report_file_name, 'Distance to Vision')
        self.xls_list = [self.xls_time, self.xls_dist]
        super().__init__(self.df_obj, const.S_PARAMS_RUN_MODE_DIST_AF, self.xls_list, const.P_PARAMS_TD)
        self.logger = logging.getLogger(__name__)

    def get_c2w_time_and_dist_df(self):
        """
        :return: None
        Method gets dataframe with time and distance for CLB_C2W_STATE convergence
        """
        self.logger.info('Get vision time and distance dataframe')
        temp_df_list = []
        for file in self.measurement_pickle_list:
            df = serializer.load_pkl(const.MEASUREMENT_DFS_FOLDER, file)
            # get first and last row from df
            df_1st_row = df.iloc[[0]].reset_index(drop=True)
            df_last_row = df.iloc[[-1]].reset_index(drop=True)
            self.logger.info(f'Get vision time and distance dataframe from {file}')
            # drop rows where CLB_C2W_STATE is none
            df = df[pandas.notnull(df[const.SIG_CLB_C2W_STATE])]
            if df.shape[0] > 0:
                # Check if C2W Converged
                clb_c2w_status_values = set(df[const.SIG_CLB_C2W_STATE])
                if const.VAL_C2W_STATE_CALIBRATED in clb_c2w_status_values:
                    temp_df = df.copy()
                    # add a row with CLB_C2W_STATE difference
                    temp_df['Diff'] = df[const.SIG_CLB_C2W_STATE].diff()
                    del df
                    # keep only rows with CLB_C2W_STATE difference
                    temp_df = temp_df[temp_df['Diff'] != 0]
                    # iterate through remaining dataframe and keep only rows where CLB_C2W_STATE changes around CALIBRATED
                    temp_df = df_filter.copy_rows__col_changed_val_df(temp_df, const.SIG_CLB_C2W_STATE,
                                                                      const.VAL_C2W_STATE_CALIBRATED, get_first_index=True)
                    # if number of indexes is more than 1 (at least once change to converged)
                    if temp_df.shape[0] > 1:
                        # if all convergence events should be counted
                        if self.count_all_convergences:
                            # if number of indexes is not even, drop last row
                            if temp_df.shape[0] % 2 != 0:
                                temp_df = temp_df[:-1]
                        # else if only first convergence event should be counted
                        else:
                            # keep only first two rows
                            temp_df = temp_df[:2]
                        # append dataframe to list, dropping 'Diff' column
                        temp_df_list.append(temp_df[const.TIME_DIST_INFO].reset_index(drop=True))
                    else:
                        # populate first row to second row (time and dist difference will be 0)
                        temp_df.loc[1] = list(temp_df.loc[0])
                        temp_df_list.append(temp_df[const.TIME_DIST_INFO].reset_index(drop=True))
                else:
                    # C2W did not converge
                    self.logger.warning(f'C2W did not converge: {file}')
                    self.c2w_not_converged_counter += 1
                    # Set timestamp to None
                    df_last_row.at[0, const.DFROW_TIMESTAMP] = None
                    # populate first row to second row (time and dist difference will be None)
                    df_last_row.loc[1] = list(df_last_row.loc[0])
                    temp_df_list.append(df_last_row[const.TIME_DIST_INFO].reset_index(drop=True))
            else:
                # No Vision
                self.logger.warning(f'No Vision: {file}')
                self.c2w_no_vision_counter += 1
                # populate first row to second row
                df_1st_row.at[0, const.DFROW_TIMESTAMP] = None
                df_1st_row.loc[1] = list(df_1st_row.loc[0])
                temp_df_list.append(df_1st_row[const.TIME_DIST_INFO].reset_index(drop=True))
        # merge all dataframes from list, and calculate time and distance to converged
        try:
            # rows 'Time to Converge' and 'Distance to Converge' are added
            self.df_obj = cf.calc_delta_time_dist_df(pandas.concat(temp_df_list, ignore_index=True)).drop(
                columns=[const.DFROW_TIMESTAMP, const.DFROW_DISTANCE])
            self.af_time_dist_df = self.df_obj.copy()
            self.not_convereged_df = self.df_obj[pandas.isnull(self.df_obj[const.DFROW_TIME_TO_CONV])]
            self.df_obj = self.df_obj[pandas.notnull(self.df_obj[const.DFROW_TIME_TO_CONV])]
        except ValueError as e:
            self.logger.exception(e)
        if self.c2w_not_converged_counter > 0:
            self.logger.warning(f'Vision Clips not converge: {self.c2w_not_converged_counter}')
