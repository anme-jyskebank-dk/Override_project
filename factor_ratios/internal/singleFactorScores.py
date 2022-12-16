from factor_ratios.functions import BlobConnect, clean_dict, override_iso, override_cap, opslag_name, countryName_to_iso, rename_factors, GetWeeks, opslag_func_single, filterData, getData
import io
import pandas as pd
import numpy as np
from typing import Union

def singleFactorScoreOverTime(weeks: Union[int, None] = None, type: Union[str, None] = None, region: Union[str, None] = None, marketcap: Union[bool, None] = None, filter_str: str = None, faktor_list: Union[list, None] = None):    
    """
    Calculates a single factor ratio or multiple factor ratios for the desired region, sector, industry group or industry for multiple weeks.

    Args:
        weeks: The desired number of weeks to get data for looking back in time. That means if 10 is provided, it will show data from the last ten weeks.
        type: The desired type the factor ratios are calculated on either Region, Sector, Group or Industry.
        region: The region to filter the data on, can be global for no filtering else Asia, Custom, Emerging Markets, Europe, Japan, North America, China or United States.
        marketcap: A boolean which decides whether the factor ratios are marketCap weighted or not.
        filter_str: A country, region, sector, indsutry group or industry to further filter the data on.
        faktor_list: A selection of the desired factors from this list (Jyske Quant, Value, Quality, Momentum).

    Returns:
        (dict): Returns a dictionary with the desired factor ratio(s), which then are used in the endpoint.
    """   
    keyvault = "kv-dad-d"
    lRequestedCols = ["week", "isin", "companyName", "countryIso","regionName","sectorName", "GIC_GROUP_NM", "industryName" ,"marketCap","jyskeQuantQuint","valueQuint","qualityQuint","momentumQuint", "countryName"]
    blob_service = BlobConnect(keyvault)
    blob_service_client_research_overrides = blob_service.get_container_client(container='research-overrides')
    blob_service_client_jyske_quant = blob_service.get_container_client(container='jyske-quant')
    data = getData(blob_service_client_research_overrides, blob_service_client_jyske_quant)[lRequestedCols]
    opslag = opslag_name(type, region)

    file_list = []
    my_blobs2 = blob_service_client_research_overrides.list_blobs()
    for blob2 in my_blobs2:
        file_list.append(blob2.name)
    file = [i for i in file_list if i.startswith('override_')]
    file = io.BytesIO(blob_service_client_research_overrides.download_blob(file[-1]).readall())
    df = pd.read_parquet(file, engine='pyarrow')
    df = clean_dict(df.iloc[:,-2:])

    my_blobs = blob_service_client_jyske_quant.list_blobs()
    for blob in my_blobs:
        blob.name
    file_temp = io.BytesIO(blob_service_client_jyske_quant.download_blob(blob.name).readall())
    df_temp = pd.read_parquet(file_temp, engine='pyarrow', columns= lRequestedCols)
    override_dict = pd.Series(df_temp["regionName"].values, index = df_temp["countryIso"]).to_dict()

    try:
        filter_str = countryName_to_iso(df_temp, filter_str)
    except:
        filter_str = filter_str

    try:
        filter_str = filter_str.replace("and", "&")
    except:
        filter_str = filter_str
    
    override_iso(df["iso_change"], data, override_dict)
    override_cap(df["cap_factors"], data)
    
    data = filterData(data, opslag, region, filter_str)
    data = data[data["week"] >= int(GetWeeks(weeks))]
    data = [opslag_func_single(data, opslag = opslag, cap = marketcap, week_ID = x, faktor_list = faktor_list) for x in np.unique(data["week"])]
    data = pd.concat(data, ignore_index= True)
    data = data.fillna(method='ffill')
    data.iloc[0,:] = data.iloc[0,:].fillna(0)

    data["week"] = pd.to_datetime(data["week"], format = "%Y%m%d")
    data["week"] = data["week"].astype(str)
    
    filter_cols = [data.columns[0], "week"] + rename_factors(faktor_list)
    return data.loc[:, filter_cols].to_dict("records")

def singleFactorScoreOverTime2(weeks: int, type: list = None, region: list = None, marketcap: Union[bool, None] = None, filter_str: list = None, faktor_list: Union[list, None] = None):    
    """
    Calculates a single factor ratio or multiple factor ratios for a list of desired region, sector, industry group or industry for multiple weeks.

    Args:
        weeks: The desired number of weeks to get data for looking back in time. That means if 10 is provided, it will show data from the last ten weeks.
        type: The desired type the factor ratios are calculated on either Region, Sector, Group or Industry.
        region: The region to filter the data on, can be global for no filtering else Asia, Custom, Emerging Markets, Europe, Japan, North America, China or United States.
        marketcap: A boolean which decides whether the factor ratios are marketCap weighted or not.
        filter_str: A country, region, sector, indsutry group or industry to further filter the data on.
        faktor_list: A selection of the desired factors from this list (Jyske Quant, Value, Quality, Momentum).

    Returns:
        (dict): Returns a dictionary with the desired factor ratio(s), which then are used in the endpoint.
    """   
    keyvault = "kv-dad-d"
    lRequestedCols = ["week", "isin", "companyName", "countryIso","regionName","sectorName", "GIC_GROUP_NM", "industryName" ,"marketCap","jyskeQuantQuint","valueQuint","qualityQuint","momentumQuint", "countryName"]
    blob_service = BlobConnect(keyvault)
    blob_service_client_research_overrides = blob_service.get_container_client(container='research-overrides')
    blob_service_client_jyske_quant = blob_service.get_container_client(container='jyske-quant')
    data = getData(blob_service_client_research_overrides, blob_service_client_jyske_quant)[lRequestedCols]

    opslag =[]
    for i in range(len(filter_str)): 
        op = opslag_name(type[i], region[i])
        opslag.append(op)

    my_blobs2 = blob_service_client_research_overrides.list_blobs()
    for blob2 in my_blobs2:
        blob2.name

    file = io.BytesIO(blob_service_client_research_overrides.download_blob(blob2.name).readall())
    df = pd.read_parquet(file, engine='pyarrow')

    df = clean_dict(df.iloc[:,-2:])

    my_blobs = blob_service_client_jyske_quant.list_blobs()
    test = []
    for blob in my_blobs:
        test.append(blob.name)
    test = [i for i in test if i.startswith('weekly-scores_') and i >= "weekly-scores_" + GetWeeks(weeks)]


    file_temp = io.BytesIO(blob_service_client_jyske_quant.download_blob(test[-1]).readall())
    df_temp = pd.read_parquet(file_temp, engine='pyarrow', columns= lRequestedCols)
    override_dict = pd.Series(df_temp["regionName"].values, index = df_temp["countryIso"]).to_dict()

    for i in range(len(filter_str)):
        try:
            filter_str[i] = countryName_to_iso(df_temp, filter_str[i])
        except:
            filter_str[i] = filter_str[i]

    for i in range(len(filter_str)):
        try:
            filter_str[i] = filter_str[i].replace("and", "&")
        except:
            filter_str[i] = filter_str[i]
    
    override_iso(df["iso_change"], data, override_dict)
    override_cap(df["cap_factors"], data)

    data = data[data["week"] >= int(GetWeeks(weeks))]
    data_list = []
    for i in range(len(opslag)):
        data2 = filterData(data, opslag[i], region[i], filter_str[i])
        data2 = [opslag_func_single(data2, opslag = opslag[i], cap = marketcap, week_ID = x, faktor_list = faktor_list) for x in np.unique(data2["week"])]
        data2 = pd.concat(data2, ignore_index= True)
        data2["week"] = pd.to_datetime(data2["week"], format = "%Y%m%d")
        data2["week"] = data2["week"].astype(str)
        data_list.append(data2)

    
    data = pd.concat(data_list, ignore_index=True)
    data = data.fillna(method='ffill')
    data.iloc[0,:] = data.iloc[0,:].fillna(0)
    data.iloc[int(len(data)/2),:] = data.iloc[int(len(data)/2),:].fillna(0) 
    
    return data.to_dict("records")