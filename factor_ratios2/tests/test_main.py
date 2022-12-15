from factor_ratios.main import app
import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from factor_ratios.functions import GetWeeks, GetLastSaturday, KeyVaultConnect, GetNextSaturday, GetMaxWeeks, countryName_to_iso, new_reg_custom, new_reg_global, score, doable_tabel, opslag_func_tabel, opslag_func_single, opslag_all_tabel, opslag_name, fill_na_Q3, subfactor_scores_single, rename_factors, override_iso, override_cap, clean_dict, filterData, getData

#Global fixtures
@pytest.fixture
def lRequestedCols_input():
    '''Returns the requested columns for the data structure'''
    lRequestedCols = ["week", "companyName", "countryIso","regionName","sectorName","industryName" ,"marketCap","jyskeQuantQuint","valueQuint","qualityQuint","momentumQuint", "isin"]
    return lRequestedCols

@pytest.fixture
def score_inputs():
    '''Provides a demo dataSeries like the one, that will be input in the function, when scoring the papers'''
    ind = ["Q1", "Q2", "Q3", "Q4", "Q5"]
    ds = pd.Series([2, 3, 6, 2 , 0], index = ind)
    return ds

@pytest.fixture
def data_inputs():
    data = pd.read_csv("/home/anme/Development/poetry-demo/factor_ratios/tests/tests_data/test_data_week.csv", index_col=0)
    return data

@pytest.fixture
def data_multi_inputs():
    data = pd.read_csv("/home/anme/Development/poetry-demo/factor_ratios/tests/tests_data/test_data_multiweek.csv", index_col=0)
    return data

@pytest.fixture
def dict_inputs():
    data = pd.read_csv("/home/anme/Development/poetry-demo/factor_ratios/tests/tests_data/dict.csv", index_col=0)
    return data

#Overall functions and connections
def test_KeyVaultConnect():
    output = KeyVaultConnect("kv-dad-d")
    output._client
    '''Verifies that keyVaultConnect connects to the correct vault and that the output is the correct type/class'''
    assert str(output.__class__) == "<class 'azure.keyvault.secrets._client.SecretClient'>"
    assert output.vault_url == 'https://kv-dad-d.vault.azure.net'

def test_GetLastSaturday_type():
    '''Verifies that the function returns a string'''
    assert isinstance(GetLastSaturday(), str)

def test_GetLastSaturday_isSaturday():
    '''Verifies that the function returns a week_ID which is a saturday'''
    assert datetime.strptime(GetLastSaturday(), "%Y%m%d").weekday() == 5

def test_GetNextSaturday_type():
    '''Verifies that the function returns a string'''
    assert isinstance(GetNextSaturday(), str)

def test_GetNextSaturday_isSaturday():
    '''Verifies that the function returns a week_ID which is a saturday'''
    assert datetime.strptime(GetNextSaturday(), "%Y%m%d").weekday() == 5

def test_GetNextSaturday_weekAfterLast():
    '''Verifies that the function returns a saturday there is a week after GetLastSaturday'''
    assert (datetime.strptime(GetNextSaturday(), "%Y%m%d") - datetime.strptime(GetLastSaturday(), "%Y%m%d")).days == 7

def test_GetWeek():
    '''Verifies that the GetWeek functions returns the correct week, here 10 weeks in the past or 70 days'''
    assert (datetime.strptime(GetLastSaturday(), "%Y%m%d") - datetime.strptime(str(GetWeeks(10)), "%Y%m%d")).days == 10*7

def test_GetMaxWeeks_type():
    '''Verifies that the GetMaxWeeks function returns a int'''
    assert isinstance(GetMaxWeeks(), int)

def test_GetMaxWeeks_correctMax():
    '''Verifies that the GetMaxWeeks function returns the maximum number of weeks where there is data from Jyske Quant'''
    iQuantStartWeek=20170102
    assert int((datetime.strptime(GetLastSaturday(), "%Y%m%d") - datetime.strptime(str(iQuantStartWeek), "%Y%m%d")).days / 7) == GetMaxWeeks()

def test_countryName_to_iso_type(data_inputs):
    '''Verifies that the transformed input is as a string'''
    assert isinstance(countryName_to_iso(data_inputs, "Denmark"), str)

def test_countryName_to_iso_nameToIso(data_inputs):
    '''Verifies that the function turns a "countryName" to a "countryIso"'''
    assert countryName_to_iso(data_inputs, "Denmark") == "DNK"

def test_new_reg_custom(data_inputs):
    '''Verifies that the function changes the "regionName" column to the desired array'''
    assert (np.unique(new_reg_custom(data_inputs).regionName) == np.array(["Canada", "EM ex. China", "North America"])).all()

def test_new_reg_global(data_inputs):
    '''Verifies that the function changes the "regionName" column to the desired array'''
    assert (np.unique(new_reg_global(data_inputs).regionName) == np.array(["Asia", "China", "Emerging Markets", "Europe", "Japan", "US"])).all()

def test_score_type(score_inputs):
    '''Verifies that the returned data is a float'''
    assert isinstance(score(score_inputs), float)

def test_score_calc(score_inputs):
    '''Verifies that the returned data is correctly calculated'''
    calc = (score_inputs.loc["Q1"] + score_inputs.loc["Q2"]) / (score_inputs.loc["Q4"] + score_inputs.loc["Q5"])
    assert score(score_inputs) == calc

def test_doable_tabel_countryIso(data_inputs):
    '''Verifies that the countryIso column is filtered so that countries which has fewer than 15 papers are removed'''
    opslag = "countryIso"
    data = doable_tabel(data_inputs, opslag)
    assert len(data.groupby(opslag).filter(lambda x: x[opslag].count() < 15)) == 0

def test_doable_tabel_other(data_inputs):
    '''Verifies that the filtering of the rest of the columns to filter upon eg. industryName removes rows which less than 20 papers'''
    opslag = "industryName"
    data = doable_tabel(data_inputs, opslag)
    assert len(data.groupby(opslag).filter(lambda x: x[opslag].count() < 20)) == 0

def test_opslag_func_tabel_cols(data_inputs):
    '''Verifies that the length of the recieved dataframe is the correct one of 9'''
    data = opslag_func_tabel(data_inputs, "regionName", "Global", cap = False, week_ID = 20221126)
    assert len(data.columns) == 9

def test_opslag_func_tabel_sorting(data_inputs):
    '''Verifies that the data is sorted correctly by Jyske Quant factor ratio'''
    data = opslag_func_tabel(data_inputs, "regionName", "Global", cap = False, week_ID = 20221126)
    assert data.iloc[0,3] == data.iloc[:,3].max()

def test_opslag_func_tabel_cap(data_inputs):
    '''Verifies that weighting the data by marketcap yield different results than by equal weight.'''
    assert opslag_func_tabel(data_inputs, "regionName", cap = False, week_ID = 20221126)["jyskeQuantFactorRatio"].equals(opslag_func_tabel(data_inputs, "regionName", cap = True, week_ID = 20221126)["jyskeQuantFactorRatio"]) == False

def test_opslag_func_single_cols(data_inputs):
    '''Verifies that the length of the recieved dataframe is the correct one of either 3, 4, 5 or 6 depended on the numbers of factors chosen'''
    data = opslag_func_single(data_inputs, "regionName", cap = False, week_ID = 20221126, faktor_list=["Jyske Quant"])
    assert len(data.columns) in [3, 4, 5, 6]

def test_opslag_func_single_sorting(data_inputs):
    '''Verifies that the data is sorted correctly.'''
    data = opslag_func_single(data_inputs, "regionName", cap = False, week_ID = 20221126, faktor_list=["Jyske Quant"])
    assert data.iloc[0,1] == data.iloc[:,1].max()

def test_opslag_func_single_cap(data_inputs):
    '''Verifies that weighting the data by marketcap yield different results than by equal weight.'''
    assert opslag_func_single(data_inputs, "regionName", cap = False, week_ID = 20221126, faktor_list=["Jyske Quant"])["jyskeQuantFactorRatio"].equals(opslag_func_single(data_inputs, "regionName", True, 20221126, ["Jyske Quant"])["jyskeQuantFactorRatio"]) == False

def test_opslag_all_tabel_cols(data_multi_inputs):
    '''Verifies that the length of the recieved dataframe is the correct one of 9'''
    data = opslag_all_tabel(data_multi_inputs, "regionName", "Global", cap = False)
    assert len(data.columns) == 9

def test_opslag_all_tabel(data_multi_inputs):
    '''Verifies that the length of the dataframe is divisible with the number of weeks in the dataFrame, so no rows is getting dropped.'''
    data = opslag_all_tabel(data_multi_inputs, "regionName", "Global", cap = False)
    assert len(data) % len(np.unique(data["week"])) == 0

def test_opslag_all_tabel_cap(data_multi_inputs):
    '''Verifies that weighting the data by marketcap yield different results than by equal weight.'''
    assert opslag_all_tabel(data_multi_inputs, "regionName", "Global", cap = False)["shareQ1"].equals(opslag_all_tabel(data_multi_inputs, "regionName", "Global", cap = True)) == False

def test_opslag_name_region():
    '''Verifies that all input combinations changed the opslag variable to the correct column name'''
    assert opslag_name("Region", "Global") == "regionName"

def test_opslag_name_country():
    '''Verifies that all input combinations changed the opslag variable to the correct column name'''
    assert opslag_name("Region", "Europe") == "countryIso"

def test_opslag_name_sector():
    '''Verifies that all input combinations changed the opslag variable to the correct column name'''
    assert opslag_name("Sector", "Global") == "sectorName"

def test_opslag_name_subsector():
    '''Verifies that all input combinations changed the opslag variable to the correct column name'''
    assert opslag_name("Subsector", "Global") == "GIC_GROUP_NM"

def test_opslag_name_industry():
    '''Verifies that all input combinations changed the opslag variable to the correct column name'''
    assert opslag_name("Industry", "Global") == "industryName"

def test_fill_na_Q3(data_inputs):
    '''Verifies that the picked column no longer has any NAN's after applying the function'''
    data = fill_na_Q3(data_inputs, "priceQuint")
    assert data["priceQuint"].isnull().values.any() == False

def test_subfactor_scores_single_col(data_inputs):
    '''Verifies that the returned dataFrame is of the correct length '''
    data = subfactor_scores_single(data_inputs, "Q2", "Jyske Quant", "Price", cap = False)
    assert len(data.columns) == 2

def test_subfactor_scores_single_colName(data_inputs):
    '''Verifies that the Factor ratio column name is correctly constructed using the input subfactor'''
    data = subfactor_scores_single(data_inputs, "Q2", "Jyske Quant", "Price", cap = False)
    assert data.columns[-1] == "Price".lower() + "FactorRatio"

def test_rename_factors():
    '''Verifies that the factors that are inputed is changed to the correct column name used in the created dataFrame'''
    assert rename_factors(["Jyske Quant", "Momentum"]) == ["jyskeQuantFactorRatio", "momentumFactorRatio"]

def test_override_iso_country(data_inputs):
    '''Verifies that the original data is overwriten with the data provided by the dictionary'''
    dict1 = pd.Series(data_inputs["regionName"].values, index = data_inputs["countryIso"]).to_dict()
    dict2 = {"DK0010272202" : "CHN", "DK0060252690" : "CAN"}
    override_iso(dict2, data_inputs, dict1) 
    assert data_inputs[(data_inputs["isin"] == "DK0060252690") & (data_inputs["countryIso"] == "CAN")].iloc[0,3] == "CAN"

def test_override_iso_region(data_inputs):
    '''Verifies that the original data is overwriten with the data provided by the dictionary'''
    dict1 = pd.Series(data_inputs["regionName"].values, index = data_inputs["countryIso"]).to_dict()
    dict2 = {"DK0010272202" : "CHN", "DK0060252690" : "CAN"}
    override_iso(dict2, data_inputs, dict1) 
    assert data_inputs[(data_inputs["isin"] == "DK0060252690") & (data_inputs["regionName"] == "North America")].iloc[0,4] == "North America"

def test_override_cap(data_inputs):
    '''Verifies that the original data is overwriten with the data provided by the dictionary'''
    dict1 = {"SA14TG012N13" : 0.05}
    override_cap(dict1, data_inputs)
    assert data_inputs[data_inputs["isin"] == "SA14TG012N13"].iloc[0,8] == 1942.578857*0.05

def test_clean_dict_type(dict_inputs):
    '''Verifies that the function returns a dictionary'''
    assert isinstance(clean_dict(dict_inputs), dict)

def test_clean_dict_isoNan(dict_inputs):
    '''Verifies that the function cleans the dataFrame from NAN's'''
    assert "Nan" not in clean_dict(dict_inputs)["iso_change"].values()

def test_clean_dict_capNan(dict_inputs):
    '''Verifies that the function cleans the dataFrame from NAN's'''
    assert float("NaN") not in clean_dict(dict_inputs)["cap_factors"].values()

def test_clean_dict_isoNone(dict_inputs):
    '''Verifies that the function cleans the dataFrame from None values'''
    assert "None" not in clean_dict(dict_inputs)["iso_change"].values()

def test_clean_dict_capNone(dict_inputs):
    '''Verifies that the function cleans the dataFrame from None values'''
    assert "None" not in clean_dict(dict_inputs)["cap_factors"].values()

def test_filterData_US(data_inputs):
    '''Verify the the function returns the correct subset of the original dataset'''
    data = new_reg_global(data_inputs)
    assert data[data["regionName"] == "US"].equals(filterData(data_inputs, "regionName", "Global", "US"))

def test_filterData_sector(data_inputs):
    '''Verify the the function returns the correct subset of the original dataset'''
    assert data_inputs[(data_inputs["sectorName"] == "Health Care") & (data_inputs["regionName"] == "Europe")].equals(filterData(data_inputs, "sectorName", "Europe", "Health Care"))

def test_getData_cols(data_inputs):
    '''Verifies that the function returns a dataFrame with the correct columns'''
    lRequestedCols = ["week", "isin", "companyName", "countryIso", "regionName", "sectorName", "GIC_GROUP_NM", "industryName", "marketCap", "jyskeQuantQuint", "valueQuint", "qualityQuint", "momentumQuint", "absValueQuint", "relValueQuint", "profitabilityQuint", "growthQuint", "safetyQuint", "earningsStabilityQuint", "sentimentQuint", "priceQuint", "countryName"]
    assert data_inputs.columns.tolist() == lRequestedCols

def test_getData_duplicates(data_inputs):
    '''Verifies that the function returns a dataFrame with no duplicate rows'''
    assert data_inputs.loc[data_inputs.duplicated(keep=False)].empty