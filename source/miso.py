"""MISO Real-Time Final Market LMPs"""

import sys, os
import datetime as dt
import requests
import io
import pandas as pd
import xlrd as xl

class MisoNodeNotFound(Exception):
    pass

class MisoTypeNotFound(Exception):
    pass

class MisoValueNotFound(Exception):
    pass

class MisoInvalidDataFormat(Exception):
    pass

def convert_df_al2csv(xls):
    """Convert Forecast and Actual Load Report from XLS to CSV"""
    sheet = xls.sheet_by_index(0)
    result = []
    for row in range(4):
        result.append(",".join([str(x.value) for x in sheet.row(row)]))
    header = ["Zone","Type","Value"]
    header.extend(list(map(lambda x:str(x),range(24))))
    result.append(",".join(header))
    for zone in range(2,len(sheet.row(4)),2):
        data = [sheet.row(4)[zone].value.split()[0],"Forecast","LOAD"]
        for row in range(6,30):
            data.append(str(round(sheet.row(row)[zone].value,2)))
        result.append(",".join(data))
    for zone in range(2,len(sheet.row(4)),2):
        data = [sheet.row(4)[zone].value.split()[0],"Actual","LOAD"]
        for row in range(6,30):
            data.append(str(round(sheet.row(row)[zone+1].value,2)))
        result.append(",".join(data))
    return "\n".join(result)

class Data:
    """MISO LMP Data Downloader"""
    BASEURL = "https://docs.misoenergy.org/marketreports"
    CACHEDIR = ".cache"
    DATAFORMATS = {
        'da_exante_lmp' : 'csv',
        'da_expost_lmp' : 'csv',
        'df_al' : 'xls',
        'rt_lmp_final' : 'csv',
        'rt_lmp_prelim' : 'csv',
        }

    def __init__(self,dataset,day):
        """LMP data downloader constructor

        ARGUMENTS

        dataset - download dataset (see DATAFORMATS)
        day - day to download (as datetime)
        """
        if not os.path.exists(self.CACHEDIR):
            os.makedirs(self.CACHEDIR,exist_ok=True)
        filename = f"{self.CACHEDIR}/{dataset}_{day.strftime('%Y%m%d')}.{self.DATAFORMATS[dataset]}"
        if not os.path.exists(filename):
            url = f"{self.BASEURL}/{day.strftime('%Y%m%d')}_{dataset}.{self.DATAFORMATS[dataset]}"
            content = requests.get(url).content
            try:
                with open(filename,"wb") as fh:
                    fh.write(content)
            except:
                os.remove(filename)
                raise
        else:
            with open(filename,"rb") as fh:
                content = fh.read()
        if self.DATAFORMATS[dataset] in ["xls"]:
            xls = xl.open_workbook(filename)
            converter = f"convert_{dataset}2csv"
            if converter not in globals():
                raise MisoInvalidDataFormat(filename)
            self.data = globals()[converter](xls)

        elif self.DATAFORMATS[dataset] in ["csv"]:
            self.data = content.decode('utf-8')

    def string(self):
        """Returns data as a string in CSV format"""
        return self.data

    def stream(self):
        """Returns data as a stream in CSV format"""
        return io.StringIO(self.data)

class Node:
    """MISO Zone Data Class"""
    SHOWPROGRESS = False
    DATEFORMAT = "%Y-%m-%d"
    VALIDTYPES = {'Interface','Loadzone','Hub','Gennode'}
    VALIDVALUES = {'LMP','MCC','MLC'}

    def __init__(self,
        starttime, 
        stoptime,
        dataset,
        stack = True,
        types = '*',
        values = '*',
        nodes = '*',
        dropna = True,
        ):
        """LMP data constructor

        ARGUMENTS

        starttime - timestamp of first day (required)
        stoptime - timestamp of last day (required)
        dataset - dataset to use (required)
        stack - stack records with hours in rows (default False)
        types - zone type to include (default is '*')
        values - value type to use (default '*')
        nodes - node to use (default '*')
        dropna - drop NA values (default True)
        """

        if types != '*' and types not in self.VALIDTYPES:
            raise MisoTypeNotFound(types)

        if values != '*' and values not in self.VALIDVALUES:
            raise MiseValueNotFound(values)

        result = []
        for day in pd.date_range(dt.datetime.strptime(starttime,self.DATEFORMAT),dt.datetime.strptime(stoptime,self.DATEFORMAT),freq='D'):
            if self.SHOWPROGRESS:
                print(f"Processing {dataset} {day}",flush=True,file=sys.stderr,end='... ')
            content = Data(dataset,day)
            data = pd.read_csv(content.stream(),skiprows=4)
            data.insert(0,"Datetime",day)
            if self.SHOWPROGRESS:
                print(f"{len(data)} records found",flush=True,file=sys.stderr)

            index = ["Datetime"]
            if nodes != '*':
                data = data[data["Node"] == nodes]
                data.drop("Node",axis=1,inplace=True)
            else:
                index.append("Node")

            if types != '*':
                data = data[data["Type"] == types]
                data.drop("Type",axis=1,inplace=True)
            else:
                index.append("Type")

            if values != '*':
                data = data[data["Value"] == values]
                data.drop("Value",axis=1,inplace=True)
            else:
                index.append("Value")

            if stack:
                data.set_index(index,inplace=True)
                data.columns = list(range(24))
                data = data.stack(dropna=dropna).reset_index()
                columns = list(data.columns)
                columns[len(index)] = "hour"
                data.columns = columns
                data["Datetime"] = data["Datetime"] + pd.to_timedelta(data["hour"]*3600e9)
                data.drop("hour",axis=1,inplace=True)
                data.set_index(index,inplace=True)
                data.columns = ["Value"]

            if len(data) > 0:
                result.append(data)

        self.data = pd.concat(result)

    def dataframe(self):
        """Return the LMP data as a pandas dataframe"""
        return self.data

class Zone:
    """MISO Zone Data Class"""
    SHOWPROGRESS = False
    DATEFORMAT = "%Y-%m-%d"
    VALIDTYPES = {'Forecast','Actual'}
    VALIDVALUES = {'LOAD'}

    def __init__(self,
        starttime, 
        stoptime,
        dataset,
        stack = True,
        types = '*',
        values = '*',
        zones = '*',
        dropna = True,
        ):
        """DFAL data constructor

        ARGUMENTS

        starttime - timestamp of first day (required)
        stoptime - timestamp of last day (required)
        dataset - dataset to use (required)
        stack - stack records with hours in rows (default False)
        types - zone type to include (default is '*')
        values - value type to use (default '*')
        zones - zone to use (default '*')
        dropna - drop NA values (default True)
        """

        if types != '*' and types not in self.VALIDTYPES:
            raise MisoTypeNotFound(types)

        if values != '*' and values not in self.VALIDVALUES:
            raise MiseValueNotFound(values)

        result = []
        for day in pd.date_range(dt.datetime.strptime(starttime,self.DATEFORMAT),dt.datetime.strptime(stoptime,self.DATEFORMAT),freq='D'):
            if self.SHOWPROGRESS:
                print(f"Processing {dataset} {day}",flush=True,file=sys.stderr,end='... ')
            content = Data(dataset,day)
            data = pd.read_csv(content.stream(),skiprows=4)
            data.insert(0,"Datetime",day)
            if self.SHOWPROGRESS:
                print(f"{len(data)} records found",flush=True,file=sys.stderr)

            index = ["Datetime"]
            if zones != '*':
                data = data[data["Zone"] == zones]
                data.drop("Zone",axis=1,inplace=True)
            else:
                index.append("Zone")

            if types != '*':
                data = data[data["Type"] == types]
                data.drop("Type",axis=1,inplace=True)
            else:
                index.append("Type")

            if values != '*':
                data = data[data["Value"] == values]
                data.drop("Value",axis=1,inplace=True)
            else:
                index.append("Value")

            if stack:
                data.set_index(index,inplace=True)
                data.columns = list(range(24))
                data = data.stack(dropna=dropna).reset_index()
                columns = list(data.columns)
                columns[len(index)] = "hour"
                data.columns = columns
                data["Datetime"] = data["Datetime"] + pd.to_timedelta(data["hour"]*3600e9)
                data.drop("hour",axis=1,inplace=True)
                data.set_index(index,inplace=True)
                data.columns = ["Value"]

            if len(data) > 0:
                result.append(data)

        self.data = pd.concat(result)

    def dataframe(self):
        """Return the DFAL data as a pandas dataframe"""
        return self.data

if __name__ == "__main__":
    import unittest

    class TestLMP(unittest.TestCase):
    
        def test_dataframe(self):
            lmp = Node("2021-01-01","2021-01-01","rt_lmp_final",stack=False)
            self.assertEqual(len(lmp.dataframe()),6837)

        def test_week(self):
            lmp = Node("2021-01-01","2021-01-07","rt_lmp_final",stack=False)
            self.assertEqual(len(lmp.dataframe()),6837*7)

        def test_node(self):
            lmp = Node("2021-01-01","2021-01-07","rt_lmp_final",stack=False,nodes='WPS.OCONTO.MP')
            self.assertEqual(len(lmp.dataframe()),21)

        def test_type(self):
            lmp = Node("2021-01-01","2021-01-07","rt_lmp_final",stack=False,types='Loadzone')
            self.assertEqual(len(lmp.dataframe()),8505)

        def test_value(self):
            lmp = Node("2021-01-01","2021-01-07","rt_lmp_final",stack=False,values='LMP')
            self.assertEqual(len(lmp.dataframe()),15953)

        def test_stack_all(self):
            lmp = Node("2021-01-01","2021-01-07","rt_lmp_final")
            self.assertEqual(len(lmp.dataframe()),1148616)

        def test_stack_type(self):
            lmp = Node("2021-01-01","2021-01-07","rt_lmp_final",types='Loadzone')
            self.assertEqual(len(lmp.dataframe()),204120)

        def test_stack_type_node(self):
            lmp = Node("2021-01-01","2021-01-07","rt_lmp_final",types='Loadzone',nodes='AECI.ALTW')
            self.assertEqual(len(lmp.dataframe()),504)

        def test_stack_type_node_value(self):
            lmp = Node("2021-01-01","2021-01-07","rt_lmp_final",types='Loadzone',nodes='AECI.ALTW',values='LMP')
            self.assertEqual(len(lmp.dataframe()),168)

        def test_stack_node(self):
            lmp = Node("2021-01-01","2021-01-07","rt_lmp_final",nodes='AECI.ALTW')
            self.assertEqual(len(lmp.dataframe()),504)

        def test_stack_node_value(self):
            lmp = Node("2021-01-01","2021-01-07","rt_lmp_final",nodes='AECI.ALTW',values='LMP')
            self.assertEqual(len(lmp.dataframe()),168)

        def test_stack_value(self):
            lmp = Node("2021-01-01","2021-01-07","rt_lmp_final",values='LMP')
            self.assertEqual(len(lmp.dataframe()),382872)

        def test_xls(self):
            dfal = Zone("2022-01-31","2022-01-31","df_al")
            self.assertEqual(len(dfal.dataframe()),336)

    unittest.main()
