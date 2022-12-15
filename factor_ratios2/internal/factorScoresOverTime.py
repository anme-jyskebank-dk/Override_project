from factor_ratios.functions import  BlobConnect, opslag_name, clean_dict, override_iso, override_cap, getData, opslag_all_tabel, GetWeeks
import io
import pandas as pd
from typing import Union

def factorScoresOverTime(weeks: int, type: Union[str, None] = None, region: Union[str, None] = None, marketcap: Union[bool, None] = None):
    keyvault = "kv-dad-d"
    lRequestedCols = ["week", "isin", "companyName", "countryIso","regionName","sectorName", "GIC_GROUP_NM", "industryName" ,"marketCap","jyskeQuantQuint","valueQuint","qualityQuint","momentumQuint", "countryName"]
    blob_service = BlobConnect(keyvault)
    blob_service_client_research_overrides = blob_service.get_container_client(container='research-overrides')
    blob_service_client_jyske_quant = blob_service.get_container_client(container='jyske-quant')
    data = getData(blob_service_client_research_overrides, blob_service_client_jyske_quant)[lRequestedCols] # Ændre tilbage til getData
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

    override_iso(df["iso_change"], data, override_dict)
    override_cap(df["cap_factors"], data)

    data = data[data["week"] >= int(GetWeeks(weeks))]

    if marketcap == True:
        data = opslag_all_tabel(data, opslag, reg = region, cap = True)
        return data.to_dict("records")
    else:
        data = opslag_all_tabel(data, opslag, reg = region)
        return data.to_dict("records")