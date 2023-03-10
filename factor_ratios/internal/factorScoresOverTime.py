from factor_ratios.functions import  BlobConnect, opslag_name, clean_dict, override_iso, override_cap, getData, opslag_all_tabel, GetWeeks, newest_parquet
import pandas as pd
from typing import Union

def factorScoresOverTime(weeks: int, type: Union[str, None] = None, region: Union[str, None] = None, marketcap: Union[bool, None] = None):
    """
    Calculates all the factor ratios for the desired region, sector, industry group or industry for multiple weeks. #Bliver ikke brugt til noget på nuværende tidspunkt.

    Args:
        weeks: The desired number of weeks to get data for looking back in time. That means if 10 is provided, it will show data from the last ten weeks.
        type: The desired type the factor ratios are calculated on either Region, Sector, Group or Industry.
        region: The region to filter the data on, can be global for no filtering else Asia, Custom, Emerging Markets, Europe, Japan, North America, China or United States.
        marketcap: A boolean which decides whether the factor ratios are marketCap weighted or not.
        
    Returns:
        (dict): Returns a dictionary with all the factor ratios, which then are used in the endpoint.
    """   
    keyvault = "kv-dad-d"
    lRequestedCols = ["week", "isin", "companyName", "countryIso","regionName","sectorName", "GIC_GROUP_NM", "industryName" ,"marketCap","jyskeQuantQuint","valueQuint","qualityQuint","momentumQuint", "countryName"]
    blob_service = BlobConnect(keyvault)
    blob_service_client_research_overrides = blob_service.get_container_client(container='research-overrides')
    blob_service_client_jyske_quant = blob_service.get_container_client(container='jyske-quant')
    data = getData(blob_service_client_research_overrides, blob_service_client_jyske_quant)[lRequestedCols]
    opslag = opslag_name(type, region)

    df = newest_parquet("research_overrides")
    df = clean_dict(df.iloc[:,-2:])

    df_temp = newest_parquet("jyske_quant", lRequestedCols)
    override_dict = pd.Series(df_temp["regionName"].values, index = df_temp["countryIso"]).to_dict()

    override_iso(df["iso_change"], data, override_dict)
    override_cap(df["cap_factors"], data)

    data = data[data["week"] >= int(GetWeeks(weeks))]

    if marketcap == True:
        data = opslag_all_tabel(data, opslag, reg = region, cap = True)
        return data.to_dict("records")
    else:
        data = opslag_all_tabel(data, opslag, reg = region)
        return data.to_dict("records")