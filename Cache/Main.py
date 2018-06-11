import Model.Pyspark_code as psk ,Model.livy as livy
import textwrap,pandas as pd

crm_pdf = pd.DataFrame()
loc_pdf = pd.DataFrame()
def load_crm():
    print('Cache/Main.load_crm            # ask query from livy')
    code_data_crm = {'code': textwrap.dedent(psk.codes['load_crm'])}
    crm = livy.main('read_crm',code_data_crm,'')
    print('Cache/Main.load_crm            # crm is in the memmory')
    crm_pdf = crm

def load_loc():
    print('Cache/Main.load_loc            # location is in the memmory')
    x1 = pd.ExcelFile("../data/iran provineces lon lat.xlsx")
    df_city = x1.parse("Sheet1")
    loc_pdf = df_city['city code','city name farsi','lon','lat']