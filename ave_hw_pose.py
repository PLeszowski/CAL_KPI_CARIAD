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

class HighwayPose:

    def __init__(self, accuracy_af_df_dict, gt_id_list):
        self.accuracy_af_df_dict = accuracy_af_df_dict
        self.gt_id_list = gt_id_list
        self.ave_hw_pose_df = pd.DataFrame(columns=const.GT_HW_POSE_INFO)
        self.ave_hw_pose_df[const.DFROW_GT_ID] = self.gt_id_list
        self.pose_top_weights_df = self.ave_hw_pose_df.copy()
        self.delta_to_sig = const.GT_HW_DPARAM_TO_SIG
        self.delta_to_param = const.GT_HW_DPARAM_TO_PARAM
        self.logger = logging.getLogger(__name__)

    def get_ave_hw_pose(self):
        self.logger.info('start get_ave_hw_pose')
        for d_param, af_df in self.accuracy_af_df_dict.items():
            for gt_id in self.gt_id_list:
                self.logger.info(f'getting average highway pose for GT ID {gt_id:04d}')
                # get dataframe where GT_ID = gt_id
                af_gt_df = af_df[af_df[const.DFROW_GT_ID] == gt_id]
                # get dataframe where ROAD_TYPE = Highway
                af_hw_gt_df = af_gt_df[af_gt_df[const.DFROW_ROAD] == const.VAL_ROAD_HIGHWAY]
                # get average for parameter (data for ave_hw_pose_df)
                hw_pose_ave = af_hw_gt_df[self.delta_to_sig[d_param]].mean()
                # get top 90% weights value
                top_weights_value = af_hw_gt_df["weight"].quantile(0.90)
                # get dataframe with top weights
                top_weights_df = af_hw_gt_df[af_hw_gt_df["weight"] > top_weights_value]
                # get average for parameter (data for pose_top_weights_df)
                pose_top_ave = top_weights_df[self.delta_to_sig[d_param]].mean()
                # insert info into gt_df
                # get index of gt_id
                gt_id_index = self.ave_hw_pose_df.index[self.ave_hw_pose_df[const.DFROW_GT_ID] == gt_id]
                # insert hw_pose_ave to parameter at index (ave_hw_pose_df)
                self.ave_hw_pose_df.loc[gt_id_index, self.delta_to_param[d_param]] = hw_pose_ave
                # insert pose_top_ave to parameter at index (pose_top_weights_df)
                self.pose_top_weights_df.loc[gt_id_index, self.delta_to_param[d_param]] = pose_top_ave
        # fill degrees
        self.ave_hw_pose_df[const.DFROW_GT_PITCH_DEG] = self.ave_hw_pose_df[const.DFROW_GT_PITCH] / (math.pi / 180)
        self.ave_hw_pose_df[const.DFROW_GT_YAW_DEG] = self.ave_hw_pose_df[const.DFROW_GT_YAW] / (math.pi / 180)
        self.ave_hw_pose_df[const.DFROW_GT_ROLL_DEG] = self.ave_hw_pose_df[const.DFROW_GT_ROLL] / (math.pi / 180)
        self.pose_top_weights_df[const.DFROW_GT_PITCH_DEG] = self.pose_top_weights_df[const.DFROW_GT_PITCH] / (math.pi / 180)
        self.pose_top_weights_df[const.DFROW_GT_YAW_DEG] = self.pose_top_weights_df[const.DFROW_GT_YAW] / (math.pi / 180)
        self.pose_top_weights_df[const.DFROW_GT_ROLL_DEG] = self.pose_top_weights_df[const.DFROW_GT_ROLL] / (math.pi / 180)
        self.logger.info('end get_ave_hw_pose')








