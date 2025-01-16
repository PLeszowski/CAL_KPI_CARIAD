"""
Module manages the generation of the Velocity Distribution sheet in kpi report
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


class VelocitySequence(KpiSequence):
    """
    Class handles the generation of the Velocity Distribution sheet in kpi report
    """

    def __init__(self, measurement_pickle_list, path_report, report_file_name):
        self.measurement_pickle_list = measurement_pickle_list
        self.df_obj = pandas.DataFrame()
        self.xls = ExcelPrinter(path_report, report_file_name, 'Velocity Distribution')
        super().__init__(self.df_obj, const.S_PARAMS_VELOCITY, [self.xls], function=cf.calc_weighed_percentile_df)
        self.logger = logging.getLogger(__name__)

    def get_velocity_weighed(self):
        """
        :return: None
        Method gets dataframe with time and distance for CLB_C2W_STATE convergence
        """
        self.logger.info('Get vision time and distance dataframe')
        temp_df_list = []
        for file in self.measurement_pickle_list:
            df = serializer.load_pkl(const.MEASUREMENT_DFS_FOLDER, file)
            try:
                self.logger.info(f'Getting velocity distribution {file}')
                # keep only columns relevant to velocity distribution
                df = df[const.VELOCITY_INFO].reset_index(drop=True)
                # keep only rows with velocity values
                df = df[df[const.DFROW_VEHICLE_SPEED].notnull()]
                # # keep only rows with valid velocity
                # df = df[df[const.DFROW_VEHICLE_SPEED_V] == const.VAL_VALID]
                # # drop column veh_speed_valid
                # df.drop(columns=[const.DFROW_VEHICLE_SPEED_V], inplace=True)
                # get unique delta values and their weights
                df_weight = df[const.DFROW_VEHICLE_SPEED].value_counts().to_frame(name='weight').reset_index().rename(columns={'index': const.DFROW_VEHICLE_SPEED})
                # get dataframe with info columns, same length as df_weight
                df_info = df.loc[0:len(df_weight)-1, :]
                # drop column veh_speed
                df_info.drop(columns=[const.DFROW_VEHICLE_SPEED], inplace=True)
                # merge info and velocity/weights
                df_merged = df_info.merge(df_weight, left_index=True, right_index=True)
                temp_df_list.append(df_merged)
            except Exception as e:
                self.logger.error(f'Error while processing {file}')
                self.logger.exception(e)
                continue
        # merge all dataframes from list, and calculate weighed velocities
        try:
            self.df_obj = pandas.concat(temp_df_list, ignore_index=True)
        except ValueError as e:
            self.logger.exception(e)



