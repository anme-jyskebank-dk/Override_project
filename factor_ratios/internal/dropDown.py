from factor_ratios.functions import BlobConnect, new_reg_custom, new_reg_global, clean_dict, override_iso, override_cap
import io
from datetime import datetime
import pandas as pd
import numpy as np
from itertools import permutations, combinations

def drop_down():
    ## Indlæsning og omskrivning af data
    keyvault = "kv-dad-d"
    lRequestedCols = ["week", "regionName", "countryName", "countryIso", "sectorName", "GIC_GROUP_NM", "industryName"]
    blob_service = BlobConnect(keyvault)
    blob_service_client_research_overrides = blob_service.get_container_client(container='research-overrides')
    blob_service_client_jyske_quant = blob_service.get_container_client(container='jyske-quant')
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

    override_iso(df["iso_change"], df_temp, override_dict)
    override_cap(df["cap_factors"], df_temp)

    df = df_temp.copy()
    df.rename(columns = {'GIC_GROUP_NM':'groupName'}, inplace = True)

    ## Start på drop down for regioner og dermed lande
    minimum = 15 # Det minimale antal papir for hvert enkelt land, sektor, industri gruppe eller industri får de fjernes som valgmulighed
    regions = np.unique(df.regionName)[:4] # North America fjernes, da den er i Custom region og ikke i den globale
    
    data_list = []
    data_list.append(pd.Series(np.unique(new_reg_global(df)["regionName"].values), name = "Global"))
    data_list.append(pd.Series(np.unique(new_reg_custom(df)["regionName"].values), name = "Custom"))

    for i in regions:
        bullet = np.unique(df.loc[(df.regionName == i) & (df.countryName != "China")]["countryName"])
        counts = [True if len(df[df["countryName"] == x]) >= minimum else False for x in bullet]
        indexes = [i for i in range(len(counts)) if counts[i] == True]
        data = pd.Series([sub.replace("&", "and") for sub in [bullet[i] for i in indexes]], name = i)
        data_list.append(data)


    ## Start på drop down for Sektorer, industri grupper og industrier
    regions =  np.unique(df.regionName)
    extra_regions = list(set(np.unique(new_reg_global(df[df["week"] == df.week.iloc[0]])["regionName"].values)) - set(np.unique(df[df["week"] == df.week.iloc[0]]["regionName"].values)))
    types =   list(set(df.columns) - set(["week", "regionName", "countryName", "countryIso"])) # Forsøgt gjort nogenlunde dynamisk, så hvis der tilføjes endnu en kategori, underindustri eller lignede, kommer den automatisk med

    for type in types:
        bullet = np.unique(df[type])
        counts = [True if len(df[df[type] == x]) >= minimum else False for x in bullet]
        indexes = [i for i in range(len(counts)) if counts[i] == True]
        data = pd.Series([sub.replace("&", "and") for sub in [bullet[i] for i in indexes]], name = "Global" + "_" + type[0:3].lower())
        data_list.append(data)

        for i in regions:
            df2 = df[df["regionName"] == i]
            counts = [True if len(df2[df2[type] == x]) >= minimum else False for x in bullet]
            indexes = [i for i in range(len(counts)) if counts[i] == True]
            data = pd.Series([sub.replace("&", "and") for sub in [bullet[i] for i in indexes]], name = i + "_" + type[0:3].lower())
            data_list.append(data)
    
        for i in extra_regions:
            df2 = df[df["countryName"] == i]
            counts = [True if len(df2[df2[type] == x]) >= minimum else False for x in bullet]
            indexes = [i for i in range(len(counts)) if counts[i] == True]
            data = pd.Series([sub.replace("&", "and") for sub in [bullet[i] for i in indexes]], name = i + "_" + type[0:3].lower())
            data_list.append(data)
    
    payload_temp = pd.concat(data_list, axis = 1)

    ## Øvre drop down menuer, der skal indeholde navnene på de forrige dataserier, for at disse kan vælges i excel.

    main_drop_downs = []
    faktor_list_temp = [list(comb) for sub in range(4) for comb in combinations(["Jyske Quant", "Value", "Quality", "Momentum"], sub + 1)]
    faktor_list = []
    for i in range(len(faktor_list_temp)):
        faktor_list.append(", ".join(faktor_list_temp[i]))
    faktor_list = pd.Series(faktor_list, name = "Faktorer")
    main_drop_downs.append(faktor_list)

    main_drop_downs.append(pd.Series(np.append((["Global"]),np.sort(np.append(np.unique(df.regionName)[:4],(["Custom"])))), name = "Region"))

    type_list= []
    type_list.append(main_drop_downs[1].name)
    for type in types:
        filter_col = pd.Series([col for col in payload_temp if col.endswith(type[0:3].lower())], name =type.split("N")[0].upper().capitalize())
        main_drop_downs.append(filter_col)
        type_list.append(filter_col.name)
    main_drop_downs.append(pd.Series(type_list, name = "Types"))
    
    payload_temp2 = pd.concat(main_drop_downs, axis = 1)

    ## Drop down til subfaktorer, så disse også er dynamiske, hvis nogle af disse fjernes eller nye kommer til. De skal dog fjernes her også, men så bliver de det også for alle, der bruger excelarket.
    quint = pd.Series(["Q1", "Q2", "Q3", "Q4", "Q5"])
    faktor = pd.Series(["Jyske Quant", "Value", "Quality", "Momentum"])
    subfaktor = pd.Series(["Value", "Quality", "Momentum", "AbsValue", "RelValue", "Profitability", "Growth", "Safety", "EarningsStability", "Sentiment", "Price"])

    payload_temp3 = pd.DataFrame({"Quintil" : quint, "Faktor": faktor, "Subfaktor" : subfaktor})

    ## Sammenkædning af alle dataserier til en enkelt dataframe

    payload = pd.concat([payload_temp2.reset_index(drop=True), payload_temp, payload_temp3], axis=1)
    payload.replace([np.nan], None, inplace = True)

    return payload.to_dict("records")