"""
Module gets average pitch, yaw, roll, and height for each GT_ID from highway data
Used only to compare to actual ground truth data
Patryk Leszowski
APTIV
ADCAM MID
CALIBRATION
"""
import logging
import pandas as pd
import const
import math

class HighwayGT:

    def __init__(self, accuracy_af_df_dict, gt_id_list):
        self.accuracy_af_df_dict = accuracy_af_df_dict
        self.gt_id_list = gt_id_list
        self.gt_df = pd.DataFrame(columns=const.GT_HW_POSE_INFO)
        self.gt_df[const.DFROW_GT_ID] = self.gt_id_list
        self.delta_to_sig = const.GT_HW_DPARAM_TO_SIG
        self.delta_to_param = const.GT_HW_DPARAM_TO_PARAM
        self.logger = logging.getLogger(__name__)

    def get_hw_gt_values(self):
        self.logger.info('start get_hw_gt_values')
        for d_param, af_df in self.accuracy_af_df_dict.items():
            for gt_id in self.gt_id_list:
                self.logger.info(f'getting average gt for GT ID {gt_id:04d}')
                # get dataframe where GT_ID = gt_id
                af_gt_df = af_df[af_df[const.DFROW_GT_ID] == gt_id]
                # get dataframe where ROAD_TYPE = Highway
                af_hw_gt_df = af_gt_df[af_gt_df[const.DFROW_ROAD] == const.VAL_ROAD_HIGHWAY]
                # get average for parameter
                gt_ave = af_hw_gt_df[self.delta_to_sig[d_param]].mean()
                # insert info into gt_df
                # get index of gt_id
                gt_id_index = self.gt_df.index[self.gt_df[const.DFROW_GT_ID] == gt_id]
                # insert gt_ave to parameter at index
                self.gt_df.loc[gt_id_index, self.delta_to_param[d_param]] = gt_ave
        # fill degrees
        self.gt_df[const.DFROW_GT_PITCH_DEG] = self.gt_df[const.DFROW_GT_PITCH] / (math.pi / 180)
        self.gt_df[const.DFROW_GT_YAW_DEG] = self.gt_df[const.DFROW_GT_YAW] / (math.pi / 180)
        self.gt_df[const.DFROW_GT_ROLL_DEG] = self.gt_df[const.DFROW_GT_ROLL] / (math.pi / 180)
        self.logger.info('end get_hw_gt_values')








