from factor_ratios.functions import  BlobConnect, opslag_name, GetLastSaturday, clean_dict, override_iso, override_cap, opslag_func_tabel
import io
import pandas as pd
from typing import Union

def factorScores(week_ID: int, type: Union[str, None] = None, region: Union[str, None] = None, marketcap: Union[bool, None] = None):
    """
    Calculates all the factor ratios for the desired region, sector, industry group or industry for a specific week.

    Args:
        week_ID: The desired week on which to do the calculations as an integer of the format [YYYYMMDD].
        type: The desired type the factor ratios are calculated on either Region, Sector, Group or Industry.
        region: The region to filter the data on, can be global for no filtering else Asia, Custom, Emerging Markets, Europe, Japan, North America, China or United States.
        marketcap: A boolean which decides whether the factor ratios are marketCap weighted or not.
        
    Returns:
        (dict): Returns a dictionary with all the factor ratios, which then are used in the endpoint.
    """   
    lRequestedCols = ["week", "isin", "companyName", "countryIso","regionName","sectorName", "GIC_GROUP_NM", "industryName" ,"marketCap","jyskeQuantQuint","valueQuint","qualityQuint","momentumQuint"]
    keyvault = "kv-dad-d"
    blob_service = BlobConnect(keyvault)
    blob_service_client_jyske_quant = blob_service.get_container_client(container='jyske-quant')
    blob_service_client_research_overrides = blob_service.get_container_client(container='research-overrides')

    opslag = opslag_name(type, region)

    if week_ID == None:
        week_ID = int(GetLastSaturday())
    else:
        week_ID = week_ID
    
    my_blobs = blob_service_client_jyske_quant.list_blobs()
    test = []
    for blob in my_blobs:
        blob.name
        test.append(blob.name)
    test = [i for i in test if i.startswith('weekly-scores_' + str(week_ID))]

    file = io.BytesIO(blob_service_client_jyske_quant.download_blob(test[-1]).readall())
    data = pd.read_parquet(file, engine='pyarrow', columns= lRequestedCols)
    override_dict = pd.Series(data["regionName"].values, index = data["countryIso"]).to_dict()

    file_list2 = []
    my_blobs2 = blob_service_client_research_overrides.list_blobs()
    for blob2 in my_blobs2:
        file_list2.append(blob2.name)
    file2 = [i for i in file_list2 if i.startswith('override_')]
    file2 = io.BytesIO(blob_service_client_research_overrides.download_blob(file2[-1]).readall())
    df = pd.read_parquet(file2, engine='pyarrow')
    df = clean_dict(df.iloc[:,-2:])

    override_iso(df["iso_change"], data, override_dict)
    override_cap(df["cap_factors"], data)
    
    if marketcap == True:
        return opslag_func_tabel(data, opslag, reg = region, cap = True, week_ID = week_ID).to_dict("records")
    else:
        return opslag_func_tabel(data, opslag, reg = region, week_ID = week_ID).to_dict("records")