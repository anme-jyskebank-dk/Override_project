from factor_ratios.functions import GetLastSaturday, clean_dict, override_iso, override_cap, newest_parquet
import pandas as pd
from typing import Union

def serviceark(week_ID: Union[int, None] = None):
    """
    Extracts information from predefined columns for a given week of Jyske Quant, for users to do there own calculations on.

    Args:
        week_ID: The desired week on which to do the calculations as an integer of the format [YYYYMMDD].
        
    Returns:
        (dict): Returns a dictionary with all the predefined columns.
    """   
    lRequestedCols = ["week", "SEDOL", "isin", "companyName", "countryIso", "regionName", "sectorName", "GIC_GROUP_NM", "industryName", "marketCap", "jyskeQuantQuint", "valueQuint", "qualityQuint", "momentumQuint" , "jyskeQuantScore", "valueScore", 
    "qualityScore", "momentumScore", "absValueQuint", "relValueQuint", "profitabilityQuint", "growthQuint", "safetyQuint", "earningsStabilityQuint", "sentimentQuint", "priceQuint"]

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
    
    return data.to_dict("records")