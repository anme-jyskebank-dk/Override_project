from factor_ratios.functions import BlobConnect, GetWeeks, subfactor_scores_single, getData
import pandas as pd
import numpy as np
from typing import Union, List

def subFactorScoresOverTime(weeks: Union[int, None] = None, factor_score_quint: Union[str, None] = None, factor_score: Union[str, None] = None, factor_subscore: Union[str, None] = None, marketcap: Union[bool, None] = None):
    keyvault = "kv-dad-d"
    lRequestedCols = ["week", "isin", "companyName", "countryIso", "regionName", "sectorName", "industryName", "marketCap", "jyskeQuantQuint", "valueQuint", "qualityQuint", "momentumQuint", "absValueQuint", "relValueQuint", "profitabilityQuint", "growthQuint", "safetyQuint", "earningsStabilityQuint", "sentimentQuint", "priceQuint", "countryName"]
    blob_service = BlobConnect(keyvault)
    blob_service_client_research_overrides = blob_service.get_container_client(container='research-overrides')
    blob_service_client_jyske_quant = blob_service.get_container_client(container='jyske-quant')
    data = getData(blob_service_client_research_overrides, blob_service_client_jyske_quant)[lRequestedCols] # ændre tilabge til getData

    data = data[data["week"] >= int(GetWeeks(weeks))]

    payload = subfactor_scores_single(data, factor_score_quint, factor_score, factor_subscore, cap = marketcap)

    cols = payload.columns.drop(payload.iloc[:, [0]].columns)
    payload[cols] = payload[cols].apply(pd.to_numeric,errors='coerce').round(2).fillna(payload)

    return payload.to_dict("records")

def subFactorScoresOverTime2(weeks: Union[int, None] = None, factor_score_quint: list = None, factor_score: list = None, subfactor_score: list = None, marketcap: Union[bool, None] = None):
    keyvault = "kv-dad-d"
    lRequestedCols = ["week", "isin", "companyName", "countryIso", "regionName", "sectorName", "GIC_GROUP_NM", "industryName", "marketCap", "jyskeQuantQuint", "valueQuint", "qualityQuint", "momentumQuint", "absValueQuint", "relValueQuint", "profitabilityQuint", "growthQuint", "safetyQuint", "earningsStabilityQuint", "sentimentQuint", "priceQuint", "countryName"]
    blob_service = BlobConnect(keyvault)
    blob_service_client_research_overrides = blob_service.get_container_client(container='research-overrides')
    blob_service_client_jyske_quant = blob_service.get_container_client(container='jyske-quant')
    data = getData(blob_service_client_research_overrides, blob_service_client_jyske_quant)[lRequestedCols]

    data = data[data["week"] >= int(GetWeeks(weeks))]

    payload = []
    for i in range(len(factor_score)):
        payload.append(subfactor_scores_single(data, factor_score_quint[i], factor_score[i], subfactor_score[i], cap = marketcap))
    
    if payload[0].columns[1] == payload[1].columns[1]:
        payload = pd.concat(payload, ignore_index= True)
    else:
        payload = payload[0].merge(payload[1], how = "inner", left_on = "week", right_on = "week")

    cols = payload.columns.drop(payload.iloc[:, [0]].columns)
    payload[cols] = payload[cols].apply(pd.to_numeric,errors='coerce').round(2).fillna(payload)

    return payload.to_dict("records")

def subFactorScoresOverTimeRollingMean(weeks: Union[int, None] = None, factor_score_quint: list = None, factor_score: list = None, subfactor_score: list = None, marketcap: Union[bool, None] = None, roll: Union[int, None] = None):
    keyvault = "kv-dad-d"
    lRequestedCols = ["week", "isin", "companyName", "countryIso", "regionName", "sectorName", "GIC_GROUP_NM", "industryName", "marketCap", "jyskeQuantQuint", "valueQuint", "qualityQuint", "momentumQuint", "absValueQuint", "relValueQuint", "profitabilityQuint", "growthQuint", "safetyQuint", "earningsStabilityQuint", "sentimentQuint", "priceQuint", "countryName"]
    blob_service = BlobConnect(keyvault)
    blob_service_client_research_overrides = blob_service.get_container_client(container='research-overrides')
    blob_service_client_jyske_quant = blob_service.get_container_client(container='jyske-quant')
    data = getData(blob_service_client_research_overrides, blob_service_client_jyske_quant)[lRequestedCols]

    data = data[data["week"] >= int(GetWeeks(weeks))]

    payload = []
    for i in range(len(factor_score)):
        #payload.append(subfactor_scores_single(data, factor_score_quint[i], factor_score[i], subfactor_score[i], cap = marketcap))
        payload2 = subfactor_scores_single(data, factor_score_quint[i], factor_score[i], subfactor_score[i], cap = marketcap)
        payload2["movingAvg" + subfactor_score[i]] = payload2.iloc[:,1].rolling(roll).mean()
        payload.append(payload2)
    if payload[0].columns[1] == payload[1].columns[1]:
        payload = pd.concat(payload, ignore_index= True)
    else:
        payload = payload[0].merge(payload[1], how = "inner", left_on = "week", right_on = "week")

    cols = payload.columns.drop(payload.iloc[:, [0]].columns)
    payload[cols] = payload[cols].apply(pd.to_numeric,errors='coerce').round(2).fillna(payload)

    payload.replace([np.nan], None, inplace = True)

    return payload.to_dict("records")