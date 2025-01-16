"""
Module manages the generation of the Accuracy sheet in kpi report
Patryk Leszowski
APTIV
ADCAM MID
CALIBRATION
"""
import logging
from excel_printer import ExcelPrinter
import df_filter
import pandas
import const
import serializer
from sequence_kpi import KpiSequence
import statistics as stat
import calc_functions as cf


class AccuracySequence(KpiSequence):
    """
    Class handles the generation of the Accuracy sheet in kpi report
    """

    def __init__(self, measurement_pickle_list, path_report, report_file_name, app_mode='Vision', roll_std=False, calib_sts='Calibrated'):
        self.measurement_pickle_list = measurement_pickle_list
        self.df_obj = {}
        self.df_failed_frames = pandas.DataFrame(index=[const.DFROW_D_PITCH, const.DFROW_D_YAW, const.DFROW_D_ROLL, const.DFROW_D_HEIGHT], columns=["TOTAL", "PASS", "FAIL"]).fillna(0)
        self.app_mode = app_mode
        self.roll_std = roll_std
        self.calib_sts = calib_sts
        if self.calib_sts == 'Suspected':
            self.app_mode = 'Vision Suspected'
            self.calib_value = const.VAL_C2W_STATE_SUSPECTED
        elif self.calib_sts == 'Unvalidated':
            self.app_mode = 'Vision Unvalidated'
            self.calib_value = const.VAL_C2W_STATE_UNVALIDATED
        else:
            self.app_mode = 'Vision Calibrated'
            self.calib_value = const.VAL_C2W_STATE_CALIBRATED
        self.calib_sig = const.SIG_CLB_C2W_STATE
        self.height_sig = const.SIG_CLB_C2W_CAM_HEIGHT
        self.accuracy_rows = const.ACCURACY_INFO
        calc_func = cf.calc_weighed_percentile_df
        # calc_func = cf.calc_percentile_df
        self.xls = [ExcelPrinter(path_report, report_file_name, 'Accuracy ' + self.app_mode)]
        super().__init__(self.df_obj, const.S_PARAMS_ACC, self.xls, function=calc_func)
        self.logger = logging.getLogger(__name__)

    def get_accuracy_df(self):
        """
        :return: None
        Method gets dataframe where CLB_SPC_STATUS = Calibrated, Unvalidated, and Suspected
        """
        self.logger.info(f'Get accuracy dataframe')
        accuracy_df_list_dict = {const.DFROW_D_PITCH: [],
                                 const.DFROW_D_YAW: [],
                                 const.DFROW_D_ROLL: [],
                                 const.DFROW_D_HEIGHT: []
                                 }

        for file in self.measurement_pickle_list:
            df = serializer.load_pkl(const.MEASUREMENT_DFS_FOLDER, file)
            self.logger.info(f'Getting {self.app_mode} accuracy dataframe from {file}')
            # keep only columns relevant to accuracy
            df = df[self.accuracy_rows].reset_index(drop=True)
            # get first row from df
            df_1st_row = df.iloc[[0]].reset_index(drop=True)
            # keep only rows where calibrated
            df_temp = df_filter.copy_rows__col_eq_val_df(df, self.calib_sig, self.calib_value)
            if df_temp.shape[0] > 0:
                # substitute file name with shorter form
                # df[const.DFROW_LOG_FILE] = file
                # get weighed values of each parameter
                for idx, param in enumerate(accuracy_df_list_dict.keys()):
                    # get unique delta values and their weights
                    df_weight = df_temp[param].value_counts().to_frame(name='weight').reset_index().rename(columns={'index': param})
                    # if delta roll std, get standard deviation of unique delta rolls
                    if param == const.DFROW_D_ROLL and self.roll_std is True:
                        # get first row from df_param
                        df_acc = df_temp.iloc[[0]].reset_index(drop=True)
                        df_acc.at[0, param] = stat.pstdev(df_weight[param])
                        # put param column at end of dataframe
                        cols = list(df_acc.columns.values)
                        cols.pop(cols.index(param))
                        df_acc = df_acc[cols + [param]]
                        accuracy_df_list_dict[param].append(df_acc)
                    # else get unique deltas with weights (weights used if weighed percentile function passed to parent)
                    else:
                        # get indexes of delta weights
                        weight_indexes = df_filter.get_indexes_of_first_occurrence(df_temp[param], df_weight[param])
                        # get only rows with weight_indexes, all columns
                        df_cut = df_temp.iloc[weight_indexes, :].reset_index(drop=True)
                        # drop column param (its duplicated in df_weight)
                        df_cut.drop([param], axis=1, inplace=True)
                        df_joined = df_cut.join(df_weight)
                        accuracy_df_list_dict[param].append(df_joined)
            else:
                self.logger.warning(f'No {self.app_mode} convergence in {file} ')
                for idx, param in enumerate(accuracy_df_list_dict.keys()):
                    df_1st_row[param] = None  # np.nan
                    # add weight column
                    df_1st_row['weight'] = 0
                    accuracy_df_list_dict[param].append(df_1st_row)

        for param, df_list in accuracy_df_list_dict.items():
            try:
                self.df_obj[param] = pandas.concat(df_list, ignore_index=True)
            except ValueError as e:
                self.logger.exception(e)
            # get rid of drop frames (HEIGHT=0 when frame is bad)
            try:
                self.df_obj[param] = self.df_obj[param][self.df_obj[param][self.height_sig] != 0].reset_index(drop=True)
            except KeyError as e:
                self.logger.exception(e)

        self.logger.info('get_accuracy_df done')

    def get_failed_frames(self):
        param_pass_dict = {const.DFROW_D_PITCH: 0.3, const.DFROW_D_YAW: 0.3, const.DFROW_D_ROLL: 0.5, const.DFROW_D_HEIGHT: 0.06}
        for param in param_pass_dict.keys():
            df_param = self.df_obj[param]
            total_frames = df_param["weight"].sum()
            # failed frames
            df_failed = df_param[df_param[param] > param_pass_dict[param]]
            failed_frames = df_failed["weight"].sum()
            passed_frames = total_frames - failed_frames
            self.df_failed_frames.loc[param, "TOTAL"] = total_frames
            self.df_failed_frames.loc[param, "PASS"] = passed_frames
            self.df_failed_frames.loc[param, "FAIL"] = failed_frames
            failed_frames_dict = self.df_failed_frames.to_dict(orient='index')


