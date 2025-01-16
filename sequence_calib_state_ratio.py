"""
Module manages the generation of the Calibration State Ratio sheet in kpi report
Patryk Leszowski
APTIV
ADCAM MID
CALIBRATION
"""
import logging
from excel_printer import ExcelPrinter
import calc_functions as cf
import pandas
import const
import serializer
from sequence_kpi import KpiSequence


class CalibStateRatioSequence(KpiSequence):
    """
    Class handles the generation of the Calibration State Ratio sheet in kpi report
    """

    def __init__(self, measurement_pickle_list, path_report, report_file_name):
        self.measurement_pickle_list = measurement_pickle_list
        # del measurement_pickle_list
        self.df_obj = pandas.DataFrame()
        self.current_row = 0
        self.xls = ExcelPrinter(path_report, report_file_name, 'Calibrated state ratio')
        super().__init__(self.df_obj, const.S_PARAMS_PR, [self.xls], function=cf.calc_calib_state_ratio_df)
        self.logger = logging.getLogger(__name__)

    def get_vision_df(self):
        """
        :return: None
        Method gets dataframe where with SIG_CLB_C2W_STATE value counts
        """
        temp_df_list = []
        for file in self.measurement_pickle_list:
            self.logger.info(f'Get ratios dataframe from {file}')
            # unpickle dictionary to dataframe
            df = serializer.load_pkl(const.MEASUREMENT_DFS_FOLDER, file)
            # drop rows where CLB_C2W_STATE is none
            df = df[pandas.notnull(df[const.SIG_CLB_C2W_STATE])]
            if df.shape[0] > 0:
                # get number of occurrences for each value in CLB_C2W_STATE
                counts = df[const.SIG_CLB_C2W_STATE].value_counts()
                # if one value didn't occur add it with zero count
                if const.VAL_C2W_STATE_CALIBRATED not in counts:
                    counts.at[const.VAL_C2W_STATE_CALIBRATED] = 0
                if const.VAL_C2W_STATE_UNVALIDATED not in counts:
                    counts.at[const.VAL_C2W_STATE_UNVALIDATED] = 0
                if const.VAL_C2W_STATE_SUSPECTED not in counts:
                    counts.at[const.VAL_C2W_STATE_SUSPECTED] = 0
                if const.VAL_C2W_STATE_OOR not in counts:
                    counts.at[const.VAL_C2W_STATE_OOR] = 0
                # keep only ratios info from first row (df becomes series)
                df = df.loc[df.index[0]][const.CALIB_STATE_RATIO_INFO]
                # substitute file name with shorter form
                df[const.DFROW_LOG_FILE] = file
                # append counts to df
                df = df.append(counts)
                # df back to dataframe, transpose
                df = df.to_frame().T
                # append to dataframe list
                temp_df_list.append(df)
        # concatenate all dataframes from list to one dataframe
        self.df_obj = pandas.concat(temp_df_list, ignore_index=True, sort=True)
