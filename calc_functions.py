"""
Module contains functions that do calculations on dataframe parameters
Patryk Leszowski
APTIV
ADCAM MID
CALIBRATION
"""
import const
import logging
import numpy as np
import math
import pandas

logger = logging.getLogger(__name__)


def calc_weighed_percentile_df(dataframe, param):
    """
    Function reads data_frame calib_param, and calculates percentiles.
    Returns dictionary
    :param dataframe: dataframe
    :param param: Calibration parameter used for filter
    :return: dictionary
    """
    logger.debug(f'param: {param}')
    try:
        df = dataframe[[param, 'weight']].reset_index(drop=True)
        df = df[pandas.notnull(df['weight'])]
        df = df[df['weight'] != 0]
        if df.shape[0] == 0:
            percentile_dict = {'Percentile 0': np.nan,
                               'Percentile 25': np.nan,
                               'Percentile 50': np.nan,
                               'Percentile 75': np.nan,
                               'Percentile 90': np.nan,
                               'Percentile 95': np.nan,
                               'Percentile 98': np.nan,
                               'Percentile 99': np.nan,
                               'Percentile 100': np.nan}
        else:
            percentile_dict = {'Percentile 0': dataframe[param].min(),
                               'Percentile 25': weighed_percentile(df, 0.25),
                               'Percentile 50': weighed_percentile(df, 0.5),
                               'Percentile 75': weighed_percentile(df, 0.75),
                               'Percentile 90': weighed_percentile(df, 0.90),
                               'Percentile 95': weighed_percentile(df, 0.95),
                               'Percentile 98': weighed_percentile(df, 0.98),
                               'Percentile 99': weighed_percentile(df, 0.99),
                               'Percentile 100': dataframe[param].max()}
    except KeyError as e:
        logger.exception(e)
        raise e
    else:
        return percentile_dict


def weighed_percentile(df, nth_q, interpolation='linear'):
    """
    Function calculates weighted percentile
    see https://en.wikipedia.org/wiki/Percentile linear interpolation between closest ranks method, where C=1 (default)
    :param df: 2 row dataframe, first row: values, second row weights
    :param nth_q: nth quantile (n is any number between and including 0 and 1)
    :param interpolation: Optional parameter specifies the interpolation method
    (same as numpy.percentile interpolation parameter)
    :return: percentile value

    """
    # drop rows where param is none
    df = df[pandas.notnull(df[df.columns[0]])]
    # sort by values row (smallest to biggest)
    df = df.sort_values(df.columns[0]).reset_index(drop=True)

    # if z weight is zero, drop that row
    if 0 in df[df.columns[1]]:
        df = df[df[df.columns[1]] != 0]

    # get data from first row, and weights from second row
    data = np.array(df[df.columns[0]])
    weights = np.array(df[df.columns[1]])

    # min
    if nth_q <= 0:
        return data[0]
    # max
    if nth_q >= 1:
        return data[-1]

    # get rank: position of nth percentile (as it would be in a not weighed data set)
    # get sum of weights
    sum_of_weights = weights.sum()
    # get rank
    rank = (nth_q * (sum_of_weights - 1)) + 1

    rank_int_part = int(rank)
    rank_frac_part = rank % rank_int_part

    data_iterator = 0  # keeps track of when to jump to the next value
    step = 0  # keeps track of steps (total number of steps are defined by rank)
    while step < rank_int_part:
        # get weight of current value
        weight = int(weights[data_iterator])
        for weight_iterator in range(weight):
            step += 1
            if step >= rank_int_part:
                # if the next value is in the next value/weight set and is not the last value/weight set
                if weight_iterator == weight - 1 and data_iterator < weights.size - 1:
                    if interpolation == 'linear':
                        return data[data_iterator] + (rank_frac_part * (data[data_iterator + 1] - data[data_iterator]))
                    elif interpolation == 'higher':
                        return data[data_iterator + 1]
                    elif interpolation == 'midpoint':
                        return (data[data_iterator] + data[data_iterator + 1]) / 2
                    elif interpolation == 'nearest':
                        return data[data_iterator] if rank_frac_part < 0.5 else data[data_iterator + 1]
                    elif interpolation == 'lower':
                        return data[data_iterator]
                    else:
                        raise ValueError("interpolation can only be 'linear','higher','midpoint','nearest', or 'lower'")
                else:
                    return data[data_iterator]
        data_iterator += 1


def calc_percentile_df(dataframe, param):
    """
    Function reads data_frame calib_param, and calculates percentiles.
    Returns dictionary
    :param dataframe: dataframe
    :param param: Calibration parameter used for filter
    :return: dictionary
    """
    logger.debug(f'param: {param}')
    try:
        percentile_dict = {'Percentile 0': dataframe[param].min(),
                           'Percentile 25': dataframe[param].quantile(0.25),
                           'Percentile 50': dataframe[param].quantile(0.5),
                           'Percentile 75': dataframe[param].quantile(0.75),
                           'Percentile 90': dataframe[param].quantile(0.90),
                           'Percentile 95': dataframe[param].quantile(0.95),
                           'Percentile 98': dataframe[param].quantile(0.98),
                           'Percentile 99': dataframe[param].quantile(0.99),
                           'Percentile 100': dataframe[param].max()}
    except KeyError as e:
        logger.exception(e)
        raise e
    else:
        return percentile_dict


def calc_2sigma_weighed(df_dict):
    logger.debug('calc 2 sigma weighed')
    two_sigma_dict = {}
    for param, dataframe in df_dict.items():
        df = dataframe[[param, 'weight']]
        if df.shape[0] == 0:
            two_sigma_dict[param] = np.nan
        else:
            two_sigma_dict[param] = weighed_percentile(df, 0.95)

    return two_sigma_dict


def calc_2sigma(df_dict):
    logger.debug('calc 2 sigma')
    two_sigma_dict = {}
    for param, dataframe in df_dict.items():
        two_sigma_dict[param] = dataframe[param].quantile(0.95)

    return two_sigma_dict


def calc_accuracy_max(df_dict):
    """
    Function reads accuracy dataframe, and calculates absolute maximum for pitch, yaw, roll, height.
    Returns dictionary.
    :param: df_dict: dictionary of dataframes
    :return: dictionary

    """
    logger.debug('calc absolute maximum deviation')
    try:
        abs_max_dict = {'Pitch': df_dict[const.DFROW_D_PITCH][const.DFROW_D_PITCH].max(),
                        'Yaw': df_dict[const.DFROW_D_YAW][const.DFROW_D_YAW].max(),
                        'Roll': df_dict[const.DFROW_D_ROLL][const.DFROW_D_ROLL].max(),
                        'Height': df_dict[const.DFROW_D_HEIGHT][const.DFROW_D_HEIGHT].max()}
    except KeyError as e:
        logger.exception(e)
        raise e
    else:
        return abs_max_dict


def calc_accuracy_min(df_dict):
    """
    Function reads accuracy dataframe, and calculates absolute minimum for pitch, yaw, roll, height.
    Returns dictionary.
    :param: df_dict: dictionary of dataframes
    :return: dictionary
    """
    logger.debug('calc absolute maximum deviation')
    try:
        abs_min_dict = {'Pitch': df_dict[const.DFROW_D_PITCH][const.DFROW_D_PITCH].min(),
                        'Yaw': df_dict[const.DFROW_D_YAW][const.DFROW_D_YAW].min(),
                        'Roll': df_dict[const.DFROW_D_ROLL][const.DFROW_D_ROLL].min(),
                        'Height': df_dict[const.DFROW_D_HEIGHT][const.DFROW_D_HEIGHT].min()}
    except KeyError as e:
        logger.exception(e)
        raise e
    else:
        return abs_min_dict


def calc_accuracy_med(df_dict):
    """
    Function reads accuracy dataframe, and calculates median for pitch, yaw, roll, height.
    Returns dictionary.
    param df_dict: dictionary of dataframes
    return: dictionary
    """
    logger.debug('calc absolute maximum deviation')
    try:
        med_dict = {'Pitch': df_dict[const.DFROW_D_PITCH][const.DFROW_D_PITCH].median(),
                    'Yaw': df_dict[const.DFROW_D_YAW][const.DFROW_D_YAW].median(),
                    'Roll': df_dict[const.DFROW_D_ROLL][const.DFROW_D_ROLL].median(),
                    'Height': df_dict[const.DFROW_D_HEIGHT][const.DFROW_D_HEIGHT].median()}
    except KeyError as e:
        logger.exception(e)
        raise e
    else:
        return med_dict


def calc_accuracy_ave(df_dict):
    """
    Function reads accuracy dataframe, and calculates median for pitch, yaw, roll, height.
    Returns dictionary.
    :param: df_dict: dictionary of dataframes
    :return: dictionary

    """
    logger.debug('calc absolute maximum deviation')
    try:
        ave_dict = {'Pitch': df_dict[const.DFROW_D_PITCH][const.DFROW_D_PITCH].mean(),
                    'Yaw': df_dict[const.DFROW_D_YAW][const.DFROW_D_YAW].mean(),
                    'Roll': df_dict[const.DFROW_D_ROLL][const.DFROW_D_ROLL].mean(),
                    'Height': df_dict[const.DFROW_D_HEIGHT][const.DFROW_D_HEIGHT].mean()}
    except KeyError as e:
        logger.exception(e)
        raise e
    else:
        return ave_dict


def calc_cam_pose_ave(df_dict):
    """
    Function reads accuracy dataframe, and calculates median for pitch, yaw, roll, height.
    Returns dictionary.
    :param: df_dict: dictionary of dataframes
    :return: dictionary

    """
    logger.debug('calc absolute maximum deviation')
    try:
        ave_dict = {'Pitch': df_dict[const.DFROW_D_PITCH][const.DFROW_PITCH_DEG].mean(),
                    'Yaw': df_dict[const.DFROW_D_YAW][const.DFROW_YAW_DEG].mean(),
                    'Roll': df_dict[const.DFROW_D_ROLL][const.DFROW_ROLL_DEG].mean(),
                    'Height': df_dict[const.DFROW_D_HEIGHT][const.SIG_CLB_C2W_CAM_HEIGHT].mean()}
    except KeyError as e:
        logger.exception(e)
        raise e
    else:
        return ave_dict


def calc_accuracy_std_dev(df_dict):
    """
    Function reads data_frame calib_param, and calculates standard deviation for pitch, yaw, roll, height.
    Returns dictionary.
    :param: df_dict: dictionary of dataframes
    :return: dictionary

    """
    logger.debug('calc standard deviation')
    try:
        std_dev_dict = {'Pitch': df_dict[const.DFROW_D_PITCH][const.DFROW_D_PITCH].std(),
                        'Yaw': df_dict[const.DFROW_D_YAW][const.DFROW_D_YAW].std(),
                        'Roll': df_dict[const.DFROW_D_ROLL][const.DFROW_D_ROLL].std(),
                        'Height': df_dict[const.DFROW_D_HEIGHT][const.DFROW_D_HEIGHT].std()}
    except KeyError as e:
        logger.exception(e)
        raise e
    else:
        return std_dev_dict


def calc_calib_state_ratio_df(dataframe, param):
    """
    Function reads data_frame calib_param column, and calculates ratio of values 'Suspected'
    'Un-validated' and 'Calibrated'.
    Returns dictionary.
    :param dataframe: dataframe, filtered for autofix
    :param param: dataframe column to count ratio
    :return: dictionary
    """
    logger.debug(f'calc calib state ratio: param: {param}')
    try:
        ratios_dict = {'Suspected': 0,
                       'Un-validated': 0,
                       'Calibrated': 0,
                       'Out of Range': 0}

        if dataframe.shape[0] > 0:
            suspected_sum = dataframe[const.VAL_C2W_STATE_SUSPECTED].sum()
            unvalidated_sum = dataframe[const.VAL_C2W_STATE_UNVALIDATED].sum()
            calibrated_sum = dataframe[const.VAL_C2W_STATE_CALIBRATED].sum()
            oor_sum = dataframe[const.VAL_C2W_STATE_OOR].sum()
            total = suspected_sum + unvalidated_sum + calibrated_sum + oor_sum
            if total > 0:
                ratios_dict = {'Suspected': (suspected_sum / total) * 100,
                               'Un-validated': (unvalidated_sum / total) * 100,
                               'Calibrated': (calibrated_sum / total) * 100,
                               'Out of Range': (oor_sum / total) * 100}
    except KeyError as e:
        logger.exception(e)
        raise e
    else:
        return ratios_dict


def calc_degrade_cause_ratio_df(dataframe, param):
    """
    Function reads data_frame calib_param column, and calculates ratio of values 'HEIGHT'
    'YAW', 'PITCH', and 'ROLL'.
    Returns dictionary.
    :param dataframe: dataframe, filtered for autofix
    :param param: dataframe column to count ratio
    :return: dictionary

    """
    logger.debug(f'calc degrade cause ratio: param: {param}')
    try:
        ratios_dict = {'NO DEGRADE': 0,
                       'HEIGHT': 0,
                       'YAW': 0,
                       'PITCH': 0,
                       'ROLL': 0}

        if dataframe.shape[0] > 0:
            no_degrade_sum = dataframe['No degradation'].sum()
            height_sum = dataframe['Height'].sum()
            yaw_sum = dataframe['Yaw'].sum()
            pitch_sum = dataframe['Pitch'].sum()
            roll_sum = dataframe['Roll'].sum()
            total = no_degrade_sum + height_sum + yaw_sum + pitch_sum + roll_sum
            if total > 0:
                ratios_dict = {'NO DEGRADE': (no_degrade_sum / total) * 100,
                               'HEIGHT': (height_sum / total) * 100,
                               'YAW': (yaw_sum / total) * 100,
                               'PITCH': (pitch_sum / total) * 100,
                               'ROLL': (roll_sum / total) * 100}
    except KeyError as e:
        logger.exception(e)
        raise e
    else:
        return ratios_dict


def calc_delta_time_dist_df(dataframe, time_col=const.DFROW_TIME_TO_CONV, dist_col=const.DFROW_DIST_TO_CONV):
    """
    Function first gets two dataframes from data_frame, one with odd rows, one with even rows,
    then calculates the difference of columns TIMESTAMP and MILEAGE_KM
    Returns dataframe with two additional rows
    :param time_col: name of delta time column
    :param dist_col: name of delta distance column
    :param dataframe: dataframe with sorted frames where param changes value
    :return: dataframe with additional rows delta time and delta distance
    """
    logger.debug('calc delta time and dist')
    try:
        df_odd = dataframe.iloc[1::2]
        df_even = dataframe.iloc[0::2]
        df_odd = df_odd.reset_index(drop=True)
        df_even = df_even.reset_index(drop=True)
        df = df_odd.copy()
        df[time_col] = df_odd[const.DFROW_TIMESTAMP] - df_even[const.DFROW_TIMESTAMP]
        df[dist_col] = df_odd[const.DFROW_DISTANCE] - df_even[const.DFROW_DISTANCE]
    except KeyError as e:
        logger.exception(e)
        raise e
    except ValueError as e:
        logger.exception(e)
        raise e
    else:
        return df


def calc_calib_mbly_state_ratio_df(dataframe):
    """
    Function reads data_frame calib_param column, and calculates ratio of values ('Suspected' +
    'Un-validated') and 'Calibrated'.
    Returns dictionary.
    :param dataframe: dataframe, filtered for autofix
    :return: dictionary[description] = value(%)
    """
    logger.debug('calc calib mbly state ratio')
    try:
        ratios_dict = {'clips not converged': 0,
                       'clips converged': 0}
        if dataframe.shape[0] > 0:
            suspected_sum = dataframe[const.VAL_C2W_STATE_SUSPECTED].sum()
            unvalidated_sum = dataframe[const.VAL_C2W_STATE_UNVALIDATED].sum()
            calibrated_sum = dataframe[const.VAL_C2W_STATE_CALIBRATED].sum()
            total = suspected_sum + unvalidated_sum + calibrated_sum
            if total > 0:
                ratios_dict = {'clips not converged': ((suspected_sum + unvalidated_sum) / total) * 100,
                               'clips converged': (calibrated_sum / total) * 100}
    except KeyError as e:
        logger.exception(e)
        raise e
    else:
        return ratios_dict


def calc_min_med_ave_max_df(dataframe, param):
    """
    :param dataframe: dataframe
    :param param: dataframe column (series) to calculate on
    :return: dictionary[stats] = stat value
    Function reads dataframe calib_param, and calculates minimum, median, average, maximum.
    Returns dictionary.
    """
    logger.debug(f'param: {param}')
    try:
        if dataframe.shape[0] > 0:
            percentile_dict = {'minimum': dataframe[param].min(),
                               'median': dataframe[param].median(),
                               'average': dataframe[param].mean(),
                               'maximum': dataframe[param].max()
                               }
        else:
            percentile_dict = {'minimum': 'No Data',
                               'median': 'No Data',
                               'average': 'No Data',
                               'maximum': 'No Data'
                               }
    except KeyError as e:
        logger.exception(e)
        raise e
    else:
        return percentile_dict


def rad_to_deg(rad):
    return math.degrees(rad)


def px_to_deg_level2(px):
    return px / 20.75
