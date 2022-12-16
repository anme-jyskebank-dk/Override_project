from factor_ratios.functions import opslag_name, GetLastSaturday, clean_dict, override_iso, override_cap, opslag_func_tabel, newest_parquet
import pandas as pd
from typing import Union

def factorScores(week_ID: Union[int, None] = None, type: Union[str, None] = None, region: Union[str, None] = None, marketcap: Union[bool, None] = None):
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

    opslag = opslag_name(type, region)

    if week_ID == None:
        week_ID = int(GetLastSaturday())
    else:
        week_ID = week_ID
    
    data = newest_parquet("jyske_quant", lRequestedCols, week_ID)
    override_dict = pd.Series(data["regionName"].values, index = data["countryIso"]).to_dict()

    df = newest_parquet("research_overrides")
    df = clean_dict(df.iloc[:,-2:])

    override_iso(df["iso_change"], data, override_dict)
    override_cap(df["cap_factors"], data)
    
    if marketcap == True:
        return opslag_func_tabel(data, opslag, reg = region, cap = True, week_ID = week_ID).to_dict("records")
    else:
        return opslag_func_tabel(data, opslag, reg = region, week_ID = week_ID).to_dict("records")