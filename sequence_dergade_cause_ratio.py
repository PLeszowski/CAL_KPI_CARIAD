"""
Module manages the generation of the Degrade Cause Ratio sheet in kpi report
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


class DegradeCauseRatioSequence(KpiSequence):
    """
    Class handles the generation of the Degrade State Cause sheet in kpi report
    """

    def __init__(self, measurement_pickle_list, path_report, report_file_name):
        self.measurement_pickle_list = measurement_pickle_list
        # del measurement_pickle_list
        self.df_obj = pandas.DataFrame()
        self.current_row = 0
        self.xls = ExcelPrinter(path_report, report_file_name, 'Degrade cause ratio')
        super().__init__(self.df_obj, const.S_PARAMS_DC, [self.xls], function=cf.calc_degrade_cause_ratio_df)
        self.logger = logging.getLogger(__name__)

    def get_vision_df(self):
        """
        :return: None
        Method gets dataframe where with Degrade Cause value counts
        """
        temp_df_list = []
        degrade_cause_count_dict = {'No degradation': 0,
                                    'Height': 0,
                                    'Yaw': 0,
                                    'Pitch': 0,
                                    'Roll': 0}

        for file in self.measurement_pickle_list:
            self.logger.info(f'Get ratios dataframe from {file}')
            # unpickle dictionary to dataframe
            df = serializer.load_pkl(const.MEASUREMENT_DFS_FOLDER, file)
            # drop rows where Degrade Cause is none
            df = df[pandas.notnull(df[const.SIG_CLB_C2W_STATE_DEGRADE_CAUSE])]
            if df.shape[0] > 0:
                # get number of occurrences for each value in CLB_C2W_STATE
                counts = df[const.SIG_CLB_C2W_STATE_DEGRADE_CAUSE].value_counts()
                # zero all dictionary values
                for key in degrade_cause_count_dict.keys():
                    degrade_cause_count_dict[key] = 0
                # Degradation Cause is bitwise
                for idx in counts.index.values:
                    if idx == 0:
                        degrade_cause_count_dict['No degradation'] = counts[idx]
                    if idx & 0x01:
                        degrade_cause_count_dict['Height'] += counts[idx]
                    if idx & 0x02:
                        degrade_cause_count_dict['Yaw'] += counts[idx]
                    if idx & 0x04:
                        degrade_cause_count_dict['Pitch'] += counts[idx]
                    if idx & 0x08:
                        degrade_cause_count_dict['Roll'] += counts[idx]

                degrade_cause_count = pandas.Series(degrade_cause_count_dict)
                # keep only ratios info from first row (df becomes series)
                df = df.loc[df.index[0]][const.DEGRADE_CAUSE_RATIO_INFO]
                # substitute file name with shorter form
                df[const.DFROW_LOG_FILE] = file
                # append degrade_cause_count to df
                df = df.append(degrade_cause_count)
                # df back to dataframe, transpose
                df = df.to_frame().T
                # append to dataframe list
                temp_df_list.append(df)
        # concatenate all dataframes from list to one dataframe
        self.df_obj = pandas.concat(temp_df_list, ignore_index=True, sort=True)
