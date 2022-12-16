'''
from factor_ratios.functions import BlobConnect, clean_dict, override_iso, override_cap, opslag_name, countryName_to_iso, filterData, GetLastSaturday
import io
import pandas as pd
import numpy as np
from typing import Union, List

def sizeFactor(type: Union[str, None] = None, region: Union[str, None] = None, filter_str: str = None):
    """
    Calculates the number of papers for the specific filtering.

    Args:
        type: The desired type the factor ratios are calculated on either Region, Sector, Group or Industry.
        region: The region to filter the data on, can be global for no filtering else Asia, Custom, Emerging Markets, Europe, Japan, North America, China or United States.
        filter_str: A country, region, sector, indsutry group or industry to further filter the data on.

    Returns:
        (dict): Returns a dictionary with the number of papers for the specific filtering.
    """       
    week_ID = GetLastSaturday()
    lRequestedCols = ["week", "isin", "companyName", "countryIso","regionName","sectorName", "GIC_GROUP_NM", "industryName" ,"marketCap","jyskeQuantQuint","valueQuint","qualityQuint","momentumQuint", "countryName"]
    keyvault = "kv-dad-d"
    blob_service = BlobConnect(keyvault)
    blob_service_client_jyske_quant = blob_service.get_container_client(container='jyske-quant')
    blob_service_client_research_overrides = blob_service.get_container_client(container='research-overrides')

    opslag = opslag_name(type, region)
    

    my_blobs2 = blob_service_client_jyske_quant.list_blobs()
    test = []
    for blob2 in my_blobs2:
        blob2.name
        test.append(blob2.name)

    test = [i for i in test if i.startswith('weekly-scores_' + str(week_ID))]

    file2 = io.BytesIO(blob_service_client_jyske_quant.download_blob(test[-1]).readall())
    data = pd.read_parquet(file2, engine='pyarrow', columns= lRequestedCols)

    override_dict = pd.Series(data["regionName"].values, index = data["countryIso"]).to_dict()

    try:
        filter_str = countryName_to_iso(data, filter_str)
    except:
        filter_str = filter_str

    try:
        filter_str = filter_str.replace("and", "&")
    except:
        filter_str = filter_str
    
    data = data.loc[:, data.columns != "countryName"]

    data = filterData(data, opslag, region, filter_str)

    file_list = []
    my_blobs2 = blob_service_client_research_overrides.list_blobs()
    for blob2 in my_blobs2:
        file_list.append(blob2.name)
    file = [i for i in file_list if i.startswith('override')]
    file = io.BytesIO(blob_service_client_research_overrides.download_blob(file[-1]).readall())
    df = pd.read_parquet(file, engine='pyarrow')
    df = clean_dict(df.iloc[:,-2:])

    override_iso(df["iso_change"], data, override_dict)
    override_cap(df["cap_factors"], data)


    response = len(data[data[opslag] == filter_str])
    response = {"Value" : response}
    return response
'''