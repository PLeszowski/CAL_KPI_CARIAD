"""
Module exports a dictionary of dictionaries to an excel sheet
Patryk Leszowski
APTIV
ADCAM MID
CALIBRATION
"""
import logging
import xlrd
import xlwt
from xlutils.copy import copy
from file_list import FileList
import math


class ExcelPrinter:
    """
    Class exports a dictionary of dictionaries, and dataframes to an Excel sheet.
    Note that the oldest instance should call export_to_excel first, otherwise any other instance's workbooks will be
    overwritten by the oldest instance's export_to_excel call.
    """

    def __init__(self, path, file_name, sheet_name):
        self.path = path
        self.file = file_name
        self.sheet_name = sheet_name
        self.ext = 'xls'
        self.current_row = 0
        self.logger = logging.getLogger(__name__)
        fl = FileList(self.path, self.ext)
        try:
            fl.get_file_list()
        except FileNotFoundError:
            self.logger.warning(f'file with extension {self.ext} not found')
            print(f'file with extension {self.ext} not found')
            pass
        finally:
            self.file_list = fl.files

    def export_to_excel(self, data_dict, start_row, col_title_list=None, cell_0_0=None, nested=True):
        """
        :param data_dict: dictionary (nested, dictionary of dictionaries)
        :param start_row: row at which start
        :param col_title_list: list of titles for columns. If None, keys of nested 1st dictionary are used
        :param cell_0_0:
        :param nested:
        :return: last row
        Function prints data from data_dict to xls file defined by col_title_list
        """
        if nested:
            try:
                self.check_dictionary(data_dict)
            except TypeError as e:
                self.logger.exception(e)
                raise e

            if col_title_list is None:
                keys = list(data_dict.keys())
                col_title_list = data_dict[keys[0]].keys()

        row, col = 0, 0

        if self.file not in self.file_list:
            wb = xlwt.Workbook()
            ws = wb.add_sheet('HEADER')
            ws.write(row, col, 'CAL KPI Report data')
            wb.save(self.path + self.file)
            self.file_list.append(self.file)

        rb = xlrd.open_workbook(self.path + self.file)
        sheets = rb.sheet_names()
        workbook = copy(rb)
        if self.sheet_name in sheets:
            worksheet = workbook.get_sheet(self.sheet_name)  # overwrite??!!
        else:
            worksheet = workbook.add_sheet(self.sheet_name)
        self.logger.info('export_to_excel: Add to Excel sheet: ' + self.sheet_name)

        row, col = start_row, 1

        if cell_0_0 is not None:
            worksheet.write(row, 0, cell_0_0)

        if nested:
            for col_title in col_title_list:
                worksheet.write(row, col, col_title)
                col += 1

        row += 1

        if nested:
            for key, dictionary in data_dict.items():
                col = 0
                worksheet.write(row, col, key)
                col += 1
                for col_title in col_title_list:
                    if col_title in dictionary:
                        if type(dictionary[col_title]) is not str:
                            if math.isnan(dictionary[col_title]):
                                worksheet.write(row, col, 'None')
                            else:
                                try:
                                    worksheet.write(row, col, dictionary[col_title])
                                except:
                                    pass

                        else:
                            worksheet.write(row, col, dictionary[col_title])
                        col += 1
                row += 1
        else:
            for key, value in data_dict.items():
                col = 0
                worksheet.write(row, col, key)
                col += 1
                worksheet.write(row, col, value)
                row += 1

        try:
            workbook.save(self.path + self.file)
        except PermissionError as e:
            self.logger.error(f'Permission denied: {self.path + self.file} not saved!!')
            self.logger.exception(e)
            raise e
        else:
            self.logger.info('Saved workbook ' + self.file)

        self.current_row = row
        return row

    @staticmethod
    def check_dictionary(data_dict):
        """
        :param data_dict: dictionary
        :return: Boolean
        Returns true if dictionary values are dictionaries
        """
        if type(data_dict) is dict:
            for key in data_dict:
                if type(data_dict[key]) is not dict:
                    raise TypeError('The values of data_dict are not of type dict')
        else:
            raise TypeError('data_dict is not of type dict')

    def df_to_excel(self, df):

        d = df.to_dict()
        row, col = 0, 0

        if self.file not in self.file_list:
            wb = xlwt.Workbook()
            ws = wb.add_sheet('HEADER')
            ws.write(row, col, 'Sequence Dataframes')
            wb.save(self.path + self.file)
            self.file_list.append(self.file)

        rb = xlrd.open_workbook(self.path + self.file)
        sheets = rb.sheet_names()
        workbook = copy(rb)
        if self.sheet_name in sheets:
            worksheet = workbook.get_sheet(self.sheet_name)  # overwrite??!!
        else:
            worksheet = workbook.add_sheet(self.sheet_name)
        self.logger.info('df_to_excel: Add to Excel sheet: ' + self.sheet_name)
        for key, dictionary in d.items():
            worksheet.write(row, col, key)
            row += 1
            for value in dictionary.values():
                worksheet.write(row, col, value)
                row += 1
            col += 1
            row = 0

        try:
            workbook.save(self.path + self.file)
        except PermissionError as e:
            self.logger.error(f'Permission denied: {self.path + self.file} not saved!!')
            self.logger.exception(e)
            raise e
        else:
            self.logger.info('Saved workbook ' + self.file)
