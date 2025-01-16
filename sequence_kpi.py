"""
Module manages the generation of kpi sequence sheets in kpi report
Patryk Leszowski
APTIV
ADCAM MID
CALIBRATION
"""
import sys
import const
import logging
import df_filter
import calc_functions as cf


class KpiSequence:
    """
    Parent class handles the generation of kpi sequence sheets in kpi report
    """

    def __init__(self, df_obj, s_params, excel_pr_list, p_params=None, function=cf.calc_percentile_df):
        self.logger = logging.getLogger(__name__)
        self.df_obj = df_obj
        self.s_params = s_params
        self.excel_pr_list = excel_pr_list
        self.p_params = p_params
        self.calc_function = function
        self.current_row = 0

    def export_kpi(self):
        """
        :return: None
        Sequence to generate drive scenario excel sheets
        """
        if const.PROJECT_CONFIG == const.CARIAD:
            self.logger.info(f'Using config CARIAD')
            for config in const.CARIAD_CONFIG:
                self.calc_kpi_cariad(config)
                self.current_row += 2
        else:
            self.logger.info(f'Using config ADCAM')
            for config in const.CAM_POS_CONFIG:
                self.calc_kpi(config)
                self.current_row += 2

    def calc_kpi(self, config):
        """
        :param config: bracket and suspension (scenario) configuration tuple (pitch, yaw, roll, suspension)
        :return: none
        Get scenario dictionaries and generate excel sheet
        """

        pitch, yaw, roll, suspension = config
        param_str = f'({pitch}, {yaw}, {roll}, {suspension})'
        # Parameters for convereged criteria
        params = self.s_params.copy()
        for k, v in params.items():
            params[k] = v + param_str

        # param is the key of the params dictionary (name of dataframe row)
        for param, value in params.items():
            if type(self.df_obj) is dict:
                bracket_df = df_filter.copy_rows__bracket_df(self.df_obj[param], pitch, yaw, roll, suspension)
            else:
                bracket_df = df_filter.copy_rows__bracket_df(self.df_obj, pitch, yaw, roll, suspension)
            self.logger.info(f'{param} = {value}')
            if config == (0, 0, 0, const.VAL_SUSP_DEFAULT):
                if self.p_params:
                    percentile_dict_list = self.calc_all_cond(self.calc_function, bracket_df, self.p_params)
                else:
                    percentile_dict_list = self.calc_all_cond(self.calc_function, bracket_df, [param])
            else:
                if self.p_params:
                    percentile_dict_list = self.calc_road_cond(self.calc_function, bracket_df, self.p_params)
                else:
                    percentile_dict_list = self.calc_road_cond(self.calc_function, bracket_df, [param])
            try:
                for idx, percentile_dict in enumerate(percentile_dict_list):
                    self.excel_pr_list[idx].export_to_excel(percentile_dict, self.current_row, None, value)
                self.current_row = self.excel_pr_list[-1].current_row
            except TypeError:
                self.logger.error('data_dict wrong type')
            else:
                self.current_row += 1
            for percentile_dict in percentile_dict_list:
                percentile_dict.clear()

    def calc_kpi_cariad(self, config):
        """
        :param config: road, weather, daytime and suspension (scenario) configuration tuple
        :return: none
        Get scenario dictionaries and generate excel sheet
        """

        road, weather, daytime, suspension = config
        param_str = f'({road}, {weather}, {daytime}, {suspension})'
        # Parameters for convereged criteria
        params = self.s_params.copy()
        for k, v in params.items():
            params[k] = v + param_str

        # param is the key of the params dictionary (name of dataframe row)
        for param, value in params.items():
            if type(self.df_obj) is dict:
                cariad_ds_df = df_filter.copy_rows__cariad_ds_df(self.df_obj[param], road, weather, daytime, suspension)
            else:
                cariad_ds_df = df_filter.copy_rows__cariad_ds_df(self.df_obj, road, weather, daytime, suspension)
            self.logger.info(f'{param} = {value}')
            if config == (const.VAL_ROAD_ANY, const.VAL_WEATHER_ANY, const.VAL_DAYTIME_ANY, const.VAL_SUSP_ANY):
                if self.p_params:
                    percentile_dict_list = self.calc_all_cond(self.calc_function, cariad_ds_df, self.p_params)
                else:
                    percentile_dict_list = self.calc_all_cond(self.calc_function, cariad_ds_df, [param])
            else:
                if self.p_params:
                    percentile_dict_list = self.calc_road_cond(self.calc_function, cariad_ds_df, self.p_params)
                else:
                    percentile_dict_list = self.calc_road_cond(self.calc_function, cariad_ds_df, [param])
            try:
                for idx, percentile_dict in enumerate(percentile_dict_list):
                    self.excel_pr_list[idx].export_to_excel(percentile_dict, self.current_row, None, value)
                self.current_row = self.excel_pr_list[-1].current_row
            except TypeError:
                self.logger.error('data_dict wrong type')
            else:
                self.current_row += 1
            for percentile_dict in percentile_dict_list:
                percentile_dict.clear()

    def calc_all_cond(self, func, dataframe, params):
        """
        :param func: function used for calculations
        :param dataframe: dataframe with data to perform calculations on
        :param params: dataframe row to perform calculations on
        :return: list of dictionaries with calculated data
        Function calls the passed calc function for each passed parameter
        Used to generate DS 1-7 of the CAL KPI report
        """
        # declare list of length equal to the number of parameters passed
        dict_list = list(range(params.__len__()))
        for param in params:
            self.logger.info(f'param: {param}')
        try:
            # Full data
            for num, param in enumerate(params):
                self.logger.info(f'Full data: param: {param}')
                dict_list[num] = {'Full data': func(dataframe, param)}
            # Day
            env_condition_filtered_df = dataframe[dataframe[const.DFROW_DAYTIME] == const.VAL_DAYTIME_DAY]
            for num, param in enumerate(params):
                self.logger.info(f'Day: param: {param}')
                dict_list[num]['Day'] = func(env_condition_filtered_df, param)
            # Night
            env_condition_filtered_df = dataframe[dataframe[const.DFROW_DAYTIME] == const.VAL_DAYTIME_NIGHT]
            for num, param in enumerate(params):
                self.logger.info(f'Night: param: {param}')
                dict_list[num]['Night'] = func(env_condition_filtered_df, param)
            # # Dusk
            # env_condition_filtered_df = dataframe[dataframe[const.DFROW_DAYTIME] == const.VAL_DAYTIME_DUSK]
            # for num, param in enumerate(params):
            #   self.logger.info(f'Dusk: param: {param}')
            # 	dict_list[num]['Dusk'] = func(env_condition_filtered_df, param)
            # City
            env_condition_filtered_df = dataframe[dataframe[const.DFROW_ROAD] == const.VAL_ROAD_CITY]
            for num, param in enumerate(params):
                self.logger.info(f'City: param: {param}')
                dict_list[num]['City'] = func(env_condition_filtered_df, param)
            # Highway
            env_condition_filtered_df = dataframe[dataframe[const.DFROW_ROAD] == const.VAL_ROAD_HIGHWAY]
            for num, param in enumerate(params):
                self.logger.info(f'Highway: param: {param}')
                dict_list[num]['Highway'] = func(env_condition_filtered_df, param)
            # Rural
            env_condition_filtered_df = dataframe[dataframe[const.DFROW_ROAD] == const.VAL_ROAD_RURAL]
            for num, param in enumerate(params):
                self.logger.info(f'Rural: param: {param}')
                dict_list[num]['Rural'] = func(env_condition_filtered_df, param)
            # Clear
            env_condition_filtered_df = dataframe[dataframe[const.DFROW_WEATHER] == const.VAL_WEATHER_CLEAR]
            for num, param in enumerate(params):
                self.logger.info(f'Clear: param: {param}')
                dict_list[num]['Clear'] = func(env_condition_filtered_df, param)
            # # Overcast
            # env_condition_filtered_df = dataframe[dataframe[const.DFROW_WEATHER] == const.VAL_WEATHER_OVERCAST]
            # for num, param in enumerate(params):
            #     self.logger.info(f'Overcast: param: {param}')
            #     dict_list[num]['Overcast'] = func(env_condition_filtered_df, param)
            # Rain
            env_condition_filtered_df = dataframe[dataframe[const.DFROW_WEATHER] == const.VAL_WEATHER_RAIN]
            for num, param in enumerate(params):
                self.logger.info(f'Rain: param: {param}')
                dict_list[num]['Rain'] = func(env_condition_filtered_df, param)
            # Snow
            env_condition_filtered_df = dataframe[dataframe[const.DFROW_WEATHER] == const.VAL_WEATHER_SNOW]
            for num, param in enumerate(params):
                self.logger.info(f'Snow: param: {param}')
                dict_list[num]['Snow'] = func(env_condition_filtered_df, param)
            # Fog
            env_condition_filtered_df = dataframe[dataframe[const.DFROW_WEATHER] == const.VAL_WEATHER_FOG]
            for num, param in enumerate(params):
                self.logger.info(f'Fog: param: {param}')
                dict_list[num]['Fog'] = func(env_condition_filtered_df, param)

            # City, Day, Clear
            env_condition_filtered_df = dataframe[(dataframe[const.DFROW_ROAD] == const.VAL_ROAD_CITY) &
                                                  (dataframe[const.DFROW_DAYTIME] == const.VAL_DAYTIME_DAY) &
                                                  (dataframe[const.DFROW_WEATHER] == const.VAL_WEATHER_CLEAR)]
            for num, param in enumerate(params):
                self.logger.info(f'CityDayClear: param: {param}')
                dict_list[num]['CityDayClear'] = func(env_condition_filtered_df, param)
            # Highway, Day, Clear
            env_condition_filtered_df = dataframe[(dataframe[const.DFROW_ROAD] == const.VAL_ROAD_HIGHWAY) &
                                                  (dataframe[const.DFROW_DAYTIME] == const.VAL_DAYTIME_DAY) &
                                                  (dataframe[const.DFROW_WEATHER] == const.VAL_WEATHER_CLEAR)]
            for num, param in enumerate(params):
                self.logger.info(f'HighwayDayClear: param: {param}')
                dict_list[num]['HighwayDayClear'] = func(env_condition_filtered_df, param)
            # Rural, Day, Clear
            env_condition_filtered_df = dataframe[(dataframe[const.DFROW_ROAD] == const.VAL_ROAD_RURAL) &
                                                  (dataframe[const.DFROW_DAYTIME] == const.VAL_DAYTIME_DAY) &
                                                  (dataframe[const.DFROW_WEATHER] == const.VAL_WEATHER_CLEAR)]
            for num, param in enumerate(params):
                self.logger.info(f'RuralDayClear: param: {param}')
                dict_list[num]['RuralDayClear'] = func(env_condition_filtered_df, param)
            # City, Night, Clear
            env_condition_filtered_df = dataframe[(dataframe[const.DFROW_ROAD] == const.VAL_ROAD_CITY) &
                                                  (dataframe[const.DFROW_DAYTIME] == const.VAL_DAYTIME_NIGHT) &
                                                  (dataframe[const.DFROW_WEATHER] == const.VAL_WEATHER_CLEAR)]
            for num, param in enumerate(params):
                self.logger.info(f'CityNightClear: param: {param}')
                dict_list[num]['CityNightClear'] = func(env_condition_filtered_df, param)
            # Highway, Night, Clear
            env_condition_filtered_df = dataframe[(dataframe[const.DFROW_ROAD] == const.VAL_ROAD_HIGHWAY) &
                                                  (dataframe[const.DFROW_DAYTIME] == const.VAL_DAYTIME_NIGHT) &
                                                  (dataframe[const.DFROW_WEATHER] == const.VAL_WEATHER_CLEAR)]
            for num, param in enumerate(params):
                self.logger.info(f'HighwayNightClear: param: {param}')
                dict_list[num]['HighwayNightClear'] = func(env_condition_filtered_df, param)
            # Rural, Night, Clear
            env_condition_filtered_df = dataframe[(dataframe[const.DFROW_ROAD] == const.VAL_ROAD_RURAL) &
                                                  (dataframe[const.DFROW_DAYTIME] == const.VAL_DAYTIME_NIGHT) &
                                                  (dataframe[const.DFROW_WEATHER] == const.VAL_WEATHER_CLEAR)]
            for num, param in enumerate(params):
                self.logger.info(f'RuralNightClear: param: {param}')
                dict_list[num]['RuralNightClear'] = func(env_condition_filtered_df, param)

            # # City, Day, Overcast
            # env_condition_filtered_df = dataframe[(dataframe[const.DFROW_ROAD] == const.VAL_ROAD_CITY) &
            #                                       (dataframe[const.DFROW_DAYTIME] == const.VAL_DAYTIME_DAY) &
            #                                       (dataframe[const.DFROW_WEATHER] == const.VAL_WEATHER_OVERCAST)]
            # for num, param in enumerate(params):
            #     self.logger.info(f'CityDayOvercast: param: {param}')
            #     dict_list[num]['CityDayOvercast'] = func(env_condition_filtered_df, param)
            # # Highway, Day, Overcast
            # env_condition_filtered_df = dataframe[(dataframe[const.DFROW_ROAD] == const.VAL_ROAD_HIGHWAY) &
            #                                       (dataframe[const.DFROW_DAYTIME] == const.VAL_DAYTIME_DAY) &
            #                                       (dataframe[const.DFROW_WEATHER] == const.VAL_WEATHER_OVERCAST)]
            # for num, param in enumerate(params):
            #     self.logger.info(f'HighwayDayOvercast: param: {param}')
            #     dict_list[num]['HighwayDayOvercast'] = func(env_condition_filtered_df, param)
            # Rural, Day, Overcast
            # env_condition_filtered_df = dataframe[(dataframe[const.DFROW_ROAD] == const.VAL_ROAD_RURAL) &
            #                                       (dataframe[const.DFROW_DAYTIME] == const.VAL_DAYTIME_DAY) &
            #                                       (dataframe[const.DFROW_WEATHER] == const.VAL_WEATHER_OVERCAST)]
            # for num, param in enumerate(params):
            #     self.logger.info(f'RuralDayOvercast: param: {param}')
            #     dict_list[num]['RuralDayOvercast'] = func(env_condition_filtered_df, param)

            # City, Day, Rain
            if const.PROJECT_CONFIG == const.CARIAD:
                daytime = const.VAL_DAYTIME_NIGHT
            else:
                daytime = const.VAL_DAYTIME_DAY
            env_condition_filtered_df = dataframe[(dataframe[const.DFROW_ROAD] == const.VAL_ROAD_CITY) &
                                                  (dataframe[const.DFROW_DAYTIME] == daytime) &
                                                  (dataframe[const.DFROW_WEATHER] == const.VAL_WEATHER_RAIN)]
            for num, param in enumerate(params):
                self.logger.info(f'CityDayRain: param: {param}')
                dict_list[num]['CityDayRain'] = func(env_condition_filtered_df, param)
            # Highway, Day, Rain
            env_condition_filtered_df = dataframe[(dataframe[const.DFROW_ROAD] == const.VAL_ROAD_HIGHWAY) &
                                                  (dataframe[const.DFROW_DAYTIME] == daytime) &
                                                  (dataframe[const.DFROW_WEATHER] == const.VAL_WEATHER_RAIN)]
            for num, param in enumerate(params):
                self.logger.info(f'HighwayDayRain: param: {param}')
                dict_list[num]['HighwayDayRain'] = func(env_condition_filtered_df, param)
            # Rural, Day, Rain
            env_condition_filtered_df = dataframe[(dataframe[const.DFROW_ROAD] == const.VAL_ROAD_RURAL) &
                                                  (dataframe[const.DFROW_DAYTIME] == daytime) &
                                                  (dataframe[const.DFROW_WEATHER] == const.VAL_WEATHER_RAIN)]
            for num, param in enumerate(params):
                self.logger.info(f'RuralDayRain: param: {param}')
                dict_list[num]['RuralDayRain'] = func(env_condition_filtered_df, param)

            # City, Day, Snow
            env_condition_filtered_df = dataframe[(dataframe[const.DFROW_ROAD] == const.VAL_ROAD_CITY) &
                                                  (dataframe[const.DFROW_DAYTIME] == const.VAL_DAYTIME_DAY) &
                                                  (dataframe[const.DFROW_WEATHER] == const.VAL_WEATHER_SNOW)]
            for num, param in enumerate(params):
                self.logger.info(f'CityDaySnow: param: {param}')
                dict_list[num]['CityDaySnow'] = func(env_condition_filtered_df, param)
            # Highway, Day, Snow
            env_condition_filtered_df = dataframe[(dataframe[const.DFROW_ROAD] == const.VAL_ROAD_HIGHWAY) &
                                                  (dataframe[const.DFROW_DAYTIME] == const.VAL_DAYTIME_DAY) &
                                                  (dataframe[const.DFROW_WEATHER] == const.VAL_WEATHER_SNOW)]
            for num, param in enumerate(params):
                self.logger.info(f'HighwayDaySnow: param: {param}')
                dict_list[num]['HighwayDaySnow'] = func(env_condition_filtered_df, param)
            # Rural, Day, Snow
            env_condition_filtered_df = dataframe[(dataframe[const.DFROW_ROAD] == const.VAL_ROAD_RURAL) &
                                                  (dataframe[const.DFROW_DAYTIME] == const.VAL_DAYTIME_DAY) &
                                                  (dataframe[const.DFROW_WEATHER] == const.VAL_WEATHER_SNOW)]
            for num, param in enumerate(params):
                self.logger.info(f'RuralDaySnow: param: {param}')
                dict_list[num]['RuralDaySnow'] = func(env_condition_filtered_df, param)
            # City, Day, Fog
            env_condition_filtered_df = dataframe[(dataframe[const.DFROW_ROAD] == const.VAL_ROAD_CITY) &
                                                  (dataframe[const.DFROW_DAYTIME] == const.VAL_DAYTIME_DAY) &
                                                  (dataframe[const.DFROW_WEATHER] == const.VAL_WEATHER_FOG)]
            for num, param in enumerate(params):
                self.logger.info(f'CityDayFog: param: {param}')
                dict_list[num]['CityDayFog'] = func(env_condition_filtered_df, param)
            # Highway, Day, Fog
            env_condition_filtered_df = dataframe[(dataframe[const.DFROW_ROAD] == const.VAL_ROAD_HIGHWAY) &
                                                  (dataframe[const.DFROW_DAYTIME] == const.VAL_DAYTIME_DAY) &
                                                  (dataframe[const.DFROW_WEATHER] == const.VAL_WEATHER_FOG)]
            for num, param in enumerate(params):
                self.logger.info(f'HighwayDayFog: param: {param}')
                dict_list[num]['HighwayDayFog'] = func(env_condition_filtered_df, param)
            # Rural, Day, Fog
            env_condition_filtered_df = dataframe[(dataframe[const.DFROW_ROAD] == const.VAL_ROAD_RURAL) &
                                                  (dataframe[const.DFROW_DAYTIME] == const.VAL_DAYTIME_DAY) &
                                                  (dataframe[const.DFROW_WEATHER] == const.VAL_WEATHER_FOG)]
            for num, param in enumerate(params):
                self.logger.info(f'RuralDayFog: param: {param}')
                dict_list[num]['RuralDayFog'] = func(env_condition_filtered_df, param)

        except KeyError as e:
            self.logger.exception(e)
            raise e
        else:
            return dict_list

    def calc_road_cond(self, func, dataframe, params):
        """
        :param func: function used for calculations
        :param dataframe: dataframe with data to perform calculations on
        :param params: dataframe row to perform calculations on
        :return: list of dictionaries with calculated data
        Function calls the passed calc function for each passed parameter
        Used to generate DS 8-last of the CAL KPI report
        """
        dict_list = list(range(params.__len__()))
        try:
            # Full data
            for num, param in enumerate(params):
                self.logger.info(f'Full data: param: {param}')
                dict_list[num] = {'Full data': func(dataframe, param)}
            # City
            env_condition_filtered_df = dataframe[dataframe[const.DFROW_ROAD] == const.VAL_ROAD_CITY]
            for num, param in enumerate(params):
                self.logger.info(f'City: param: {param}')
                dict_list[num]['City'] = func(env_condition_filtered_df, param)
            # Highway
            env_condition_filtered_df = dataframe[dataframe[const.DFROW_ROAD] == const.VAL_ROAD_HIGHWAY]
            for num, param in enumerate(params):
                self.logger.info(f'Highway: param: {param}')
                dict_list[num]['Highway'] = func(env_condition_filtered_df, param)
            # Rural
            env_condition_filtered_df = dataframe[dataframe[const.DFROW_ROAD] == const.VAL_ROAD_RURAL]
            for num, param in enumerate(params):
                self.logger.info(f'Rural: param: {param}')
                dict_list[num]['Rural'] = func(env_condition_filtered_df, param)

        except KeyError as e:
            self.logger.exception(e)
            raise e
        else:
            return dict_list
