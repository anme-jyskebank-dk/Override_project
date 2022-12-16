import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re
import io
from typing import Union
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from io import BytesIO

def GetLastSaturday():
    return datetime.strftime(datetime.today() - timedelta(days=(datetime.today().isoweekday()+15)),"%Y%m%d") #Skal ændres inden test til +1, men har ikke data fra nyeste uge

def GetNextSaturday():
    return datetime.strftime(datetime.today() - timedelta(days=(datetime.today().isoweekday()+8)),"%Y%m%d") #Skal ændres inden test til -6, men har ikke data fra nyeste uge

def GetWeeks(x = 0):
    return datetime.strftime(datetime.today() - timedelta(days=(datetime.today().isoweekday() +15), weeks = x), "%Y%m%d") #Skal ændres inden test til +1, men har ikke data fra nyeste uge

def GetMaxWeeks():
    iQuantStartWeek=20170102
    start = (datetime.strptime(str(iQuantStartWeek), '%Y%m%d'))
    end = (datetime.strptime(GetLastSaturday(), '%Y%m%d'))
    return int((end - start).days / 7)


def DateIsSaturday(sDate):
    dDate=datetime.strptime(str(sDate),"%Y%m%d")
    
    return dDate.weekday()!=5

def KeyVaultConnect(key_vault_name=''):
    """
    Creates a service client to access AZ key vault.
    """
    print('Connecting to key vault')
    credentials = DefaultAzureCredential()
    vault_uri = 'https://{}.vault.azure.net/'.format(key_vault_name)
    KeyVaultClient = SecretClient(vault_uri, credentials)

    return KeyVaultClient

def BlobConnect(key_vault_name=''):
    """
    Creates service client to acces AZ blob storage.

    Args:
        key_vault_name: The name of the key vault containing secrets.

    Returns:
        (obj): The blob service client object.
    """
    KeyVaultClient = KeyVaultConnect(key_vault_name)
    print('Getting secret ADL variables')
    ADLSAccessKey = KeyVaultClient.get_secret("ADLSAccessKey").value #Azure DataLake Access Key
    ADLSAccountName = KeyVaultClient.get_secret("ADLSAccountName").value #Azure DataLake Account NAme
    print("Connecting to blob")
    service = BlobServiceClient(account_url='https://{}.blob.core.windows.net'.format(ADLSAccountName), credential=ADLSAccessKey)
    print("Connected to blob")
        
    return(service)

def UploadToBlob(self, dfData, fileName: str):
    """
    """

    week_key = datetime.strftime(datetime.today() - timedelta(days=(datetime.today().isoweekday()-6)),"%Y%m%d")
    day_key = datetime.strftime(datetime.today(),"%Y%m%d")
    time_stamp = datetime.now().timestamp()

    dfData.loc[:,"logDTime"] = time_stamp
    dfData.loc[:,"week"] = int(week_key)
    dfData.loc[:, "day"] = int(day_key)

    colsToMove = ["logDTime","week", "day"]
    dfRearranged = dfData[colsToMove + [col for col in dfData.columns if col not in colsToMove]]

    t1 = datetime.now()
    #create and upload files to blob
    fname = fileName + '_' + week_key + '_' + day_key + '_' + str(time_stamp) + '.parquet'
    parquet_file = BytesIO()
    dfRearranged.to_parquet(parquet_file, engine='pyarrow')
    parquet_file.seek(0)  # change the stream position back to the beginning after writing
    self.upload_blob(data=parquet_file, name=fname, overwrite=True)
    t2 = datetime.now()
    elapse = t2-t1
    parquet_file.close()
    print(f'Upload of {fname} to blob completed succesfully in {elapse.total_seconds()}')

def UploadToBlobNoWeekCol(self, dfData, fileName: str):
    """
    Like the normal UploadToBlob just without the week column, because otherwise it would save the current week on top of all the WeekID's.
    """

    week_key = datetime.strftime(datetime.today() - timedelta(days=(datetime.today().isoweekday()-6)),"%Y%m%d")
    day_key = datetime.strftime(datetime.today(),"%Y%m%d")
    time_stamp = datetime.now().timestamp()

    dfData.loc[:,"logDTime"] = time_stamp
    dfData.loc[:, "day"] = int(day_key)

    colsToMove = ["logDTime", "day"]
    dfRearranged = dfData[colsToMove + [col for col in dfData.columns if col not in colsToMove]]

    t1 = datetime.now()
    #create and upload files to blob
    fname = fileName + '_' + week_key + '_' + day_key + '_' + str(time_stamp) + '.parquet'
    parquet_file = BytesIO()
    dfRearranged.to_parquet(parquet_file, engine='pyarrow')
    parquet_file.seek(0)  # change the stream position back to the beginning after writing
    self.upload_blob(data=parquet_file, name=fname, overwrite=True)
    t2 = datetime.now()
    elapse = t2-t1
    parquet_file.close()
    print(f'Upload of {fname} to blob completed succesfully in {elapse.total_seconds()}')


def countryName_to_iso(data,  filter_str):
    """
    Rewrites the filter_str provided from a countryName to a countryIso.

    Args:
        data: The data to make the dictionary to browse thorugh.
        filter_str: The countryName to rewrite as a countryIso.

    Returns:
        (str): The countryName as a countryIso.
    """
    countries = pd.Series(data["countryIso"].values, index = data["countryName"]).to_dict()
    if (filter_str not in np.unique(new_reg_custom(data)["regionName"].values)) and (filter_str not in np.unique(new_reg_global(data)["regionName"].values)):
        filter_str = countries[filter_str]
    else:
        filter_str = filter_str
    return filter_str


def new_reg_custom(df):
    """
    Makes a new custom region to be used containing ["Canada", "EM ex. China", "North America"], so it ends up creating a larger dataFrame,
    because canadian companies will be duplicated.

    Args:
        df: The dataFrame in which the regionName column will be edited.

    Returns:
        (df): Returns the edited and larger dataFrame.
    """
    df2 = df.copy()
    df2 = df2[((df2.loc[:, ("regionName")] == "Emerging Markets") | (df2.loc[:,("regionName")] == "North America")) & (df2.loc[:, ("countryIso")] != "CHN")]

    df2.loc[df2.loc[:, ("regionName")] == "Emerging Markets", "regionName"] = "EM ex. China"

    df2.loc[df2.loc[:, ("countryIso")] == "CAN", "regionName"] = "Canada"

    df3 = df2[df2["regionName"] == "Canada"]

    df4= pd.concat([df3, df2])

    return df4


def new_reg_global(df):
    """
    Makes a new global region to be used containing ["Europe", "Asia", "Japan", "US", "Emerging Markets", "China"], so it ends up creating a larger dataFrame,
    because chinese companies will be duplicated although canadian companies will be removed.

    Args:
        df: The dataFrame in which the regionName column will be edited.

    Returns:
        (df): Returns the edited and larger dataFrame.
    """
    df2 = df.copy()
    regions = pd.Series(df["regionName"].values, index = df["countryIso"]).to_dict()
    list = ["China" if x == "CHN" else "United States" if x == "USA" else "Canada" if x == "CAN" else regions[x] for x in df2["countryIso"]]
    df2["new_regionName"] = list
    df2 = df2[(df2["new_regionName"] != "North America") & (df2["new_regionName"] != "Canada") ]

    df3 = df2.copy()

    df3.loc[df3.loc[:, ("new_regionName")] == "China", "new_regionName"] = "Emerging Markets"
    df3 = df3[df3["countryIso"] == "CHN"]

    df4= pd.concat([df3, df2])

    df4 = df4.drop(["regionName"], axis = 1)
    df4 = df4.rename(columns = {"new_regionName" : "regionName"})

    return df4

def score(ds):
    """
    Makes the factor ratio score by dividing (Q1 + Q2) with (Q4 + Q5). If not all quintiles are present in the count if will get a value of 0.

    Args:
        ds: The dataSeries with quintiles as indexes and a count of each index as the value.

    Returns:
        (int): Returns the score.
    """
    list = ["Q1", "Q2", "Q3", "Q4", "Q5"]
    while len(ds) < 5:
        for ind in list:
            dat = pd.Series([0], index = [ind])
            ds = pd.concat([ds,dat])
    x = ds[~ds.index.duplicated(keep='first')]
    y = (x.loc["Q1"] + x.loc["Q2"]) / (x.loc["Q4"] + x.loc["Q5"])
    return y
    

def doable_tabel(df, opslag):
    """
    Filters the desired dataFrame for countries with fewer than 15 papers in Jyske Quant and fewer than 20 for all other filtering conditions (sector, industry etc.). 

    Args:
        df: The dataFrame to make the filtering on.
        opslag: The columnName of the desired filtering condition.

    Returns:
        (df): Returns the filtered dataFrame.
    """  
    if opslag == "countryIso":
        df = df.groupby(opslag).filter(lambda x: x[opslag].count() >= 15)
    else:
        df = df.groupby(opslag).filter(lambda x: x[opslag].count() >= 20)
    return df

def opslag_func_tabel(df=pd.DataFrame, opslag=str, reg = [str, None], cap = [bool, None], week_ID = None):
    """
    Calculates all the factor ratios (Jyske Quant, Value, Quality, Momentum) for the desired region, sector or industry.

    Args:
        df: The dataFrame to make the calculations on.
        opslag: The columnName of the desired filtering condition.
        reg: The name of the region to filter the data on, can also be "Global" if no region is wanted.
        cap: A boolean which decides whether the factor ratios are marketCap weighted or not.
        week_ID: The desired week on which to do the calculations as an integer of the format [YYYYMMDD].

    Returns:
        (df): Returns the factor ratios for the given specifications in a dataFrame.
    """  
    if week_ID == None:
        week_ID = int(GetLastSaturday())
    else:
        week_ID = week_ID

    df = df[df["week"] == week_ID]

    if reg == "Europe":
        df = doable_tabel(df[df["regionName"] == "Europe"], opslag)
        bullet = df[opslag].unique()
    elif reg == "Emerging Markets":
        df = doable_tabel(df[df["regionName"] == "Emerging Markets"],opslag)
        bullet = df[opslag].unique()
    elif reg == "Asia":
        df = doable_tabel(df[df["regionName"] == "Asia"],opslag)
        bullet = df[opslag].unique()
    elif reg == "North America":
        df = doable_tabel(df[df["regionName"] == "North America"],opslag)
        bullet = df[opslag].unique()
    elif reg == "Custom":
        df = doable_tabel(new_reg_custom(df), opslag)
        bullet = df[opslag].unique()
    elif reg == "China":
        df = doable_tabel(df[df["countryIso"] == "CHN"], opslag)
        bullet = df[opslag].unique()
    elif reg == "Japan":
        df = doable_tabel(df[df["countryIso"] == "JPN"], opslag)
        bullet = df[opslag].unique()
    elif reg == "US":
        df = doable_tabel(df[df["countryIso"] == "USA"], opslag)
        bullet = df[opslag].unique()
    elif reg == "EM ex. China":
        df = new_reg_custom(df)
        df = doable_tabel(df[df["regionName"]=="EM ex. China"], opslag)
        bullet = df[opslag].unique()
    elif reg == "Global":
        if opslag != "regionName":
            df = doable_tabel(df, opslag)
            bullet = df[opslag].unique()
        else:
            df = new_reg_global(df)
            bullet = df[opslag].unique()
    else:
        bullet = df[opslag].unique()


    quant = [score(df[df[opslag] == x].groupby(["jyskeQuantQuint"])["marketCap"].sum()) if cap == True else score(df[df[opslag] == x].groupby(["jyskeQuantQuint"])["jyskeQuantQuint"].count()) for x in bullet]

    value = [score(df[df[opslag] == x].groupby(["valueQuint"])["marketCap"].sum()) if cap == True else score(df[df[opslag] == x].groupby(["valueQuint"])["valueQuint"].count()) for x in bullet]

    quality = [score(df[df[opslag] == x].groupby(["qualityQuint"])["marketCap"].sum()) if cap == True else score(df[df[opslag] == x].groupby(["qualityQuint"])["qualityQuint"].count()) for x in bullet]

    momentum = [score(df[df[opslag] == x].groupby(["momentumQuint"])["marketCap"].sum()) if cap == True else score(df[df[opslag] == x].groupby(["momentumQuint"])["momentumQuint"].count()) for x in bullet]

    sum = [df[df[opslag] == x].groupby(["jyskeQuantQuint"])["jyskeQuantQuint"].count().sum() for x in bullet]

    share = [(df[df[opslag] == x]["marketCap"].sum() / df["marketCap"].sum())*100 if cap == True else (len(df[df[opslag] == x].index) / len(df.index))*100 for x in bullet]

    share2 = [(df[(df[opslag] == x) & (df["jyskeQuantQuint"] == "Q1")]["marketCap"].sum() / df[df[opslag] == x]["marketCap"].sum())*100 if cap == True else (len(df[(df[opslag] == x) & (df["jyskeQuantQuint"] == "Q1")].index) / len(df[df[opslag] == x].index))*100 for x in bullet]

    week = [week_ID for x in bullet]

    if opslag == "industryName":
        sectors = pd.Series(df["sectorName"].values, index = df["industryName"]).to_dict()
        sector_list = [sectors[x] for x in bullet]
        data = pd.DataFrame([bullet, sum, sector_list, quant, value, quality, momentum, share2, week]).transpose()
    elif opslag == "GIC_GROUP_NM":
        sectors = pd.Series(df["sectorName"].values, index = df["GIC_GROUP_NM"]).to_dict()
        sector_list = [sectors[x] for x in bullet]
        data = pd.DataFrame([bullet, sum, sector_list, quant, value, quality, momentum, share2, week]).transpose()
    else:
        data = pd.DataFrame([bullet, sum, share, quant, value, quality, momentum, share2, week]).transpose()

    data.columns = ["type", "companyQuantity", "share", "jyskeQuantFactorRatio", "valueFactorRatio", "qualityFactorRatio", "momentumFactorRatio", "shareQ1", "week"]

    data = data.sort_values(by = ["jyskeQuantFactorRatio"], ascending=False)

    data.replace([np.inf], "inf+", inplace = True)

    cols = data.columns.drop(data.iloc[:, [8]].columns)
    data[cols] = data[cols].apply(pd.to_numeric,errors='coerce').round(2).fillna(data)

    return data


def opslag_func_single(df=pd.DataFrame, opslag=str, cap = [bool, None], week_ID = None, faktor_list = [list, None]):
    """
    Calculates a single factor ratio or multiple factor ratios from this list (Jyske Quant, Value, Quality, Momentum) for the desired region, sector or industry.

    Args:
        df: The dataFrame to make the calculations on.
        opslag: The columnName of the desired filtering condition.
        cap: A boolean which decides whether the factor ratios are marketCap weighted or not.
        week_ID: The desired week on which to do the calculations as an integer of the format [YYYYMMDD].
        faktor_list: The desired factor or factors to calculate the ratios for as a list.

    Returns:
        (df): Returns the factor ratios for the given specifications in a dataFrame.
    """   
    if week_ID == None:
        week_ID = int(GetLastSaturday())
    else:
        week_ID = week_ID
    
    df = df[df["week"] == week_ID]

    bullet = df[opslag].unique()
    headers = []
    headerNames = []

    if "Jyske Quant" in faktor_list:
        quant = [score(df[df[opslag] == x].groupby(["jyskeQuantQuint"])["marketCap"].sum()) if cap == True else score(df[df[opslag] == x].groupby(["jyskeQuantQuint"])["jyskeQuantQuint"].count()) for x in bullet]
        headers.append(quant)
        headerNames.append("jyskeQuantFactorRatio")
    if "Value" in faktor_list:
        value = [score(df[df[opslag] == x].groupby(["valueQuint"])["marketCap"].sum()) if cap == True else score(df[df[opslag] == x].groupby(["valueQuint"])["valueQuint"].count()) for x in bullet]
        headers.append(value)
        headerNames.append("valueFactorRatio")
    if "Quality" in faktor_list:
        quality = [score(df[df[opslag] == x].groupby(["qualityQuint"])["marketCap"].sum()) if cap == True else score(df[df[opslag] == x].groupby(["qualityQuint"])["qualityQuint"].count()) for x in bullet]
        headers.append(quality)
        headerNames.append("qualityFactorRatio")
    if "Momentum" in faktor_list:
        momentum = [score(df[df[opslag] == x].groupby(["momentumQuint"])["marketCap"].sum()) if cap == True else score(df[df[opslag] == x].groupby(["momentumQuint"])["momentumQuint"].count()) for x in bullet]
        headers.append(momentum)
        headerNames.append("momentumFactorRatio")

    week = [week_ID for x in bullet]

    data = pd.DataFrame([bullet] + headers + [week]).transpose()

    data.columns = ["filterParam"] + headerNames + [ "week"]

    data = data.sort_values(by = headerNames[0], ascending=False)
    
    data.replace([np.inf], np.nan, inplace = True)

    cols = data.columns.drop("week")
    data[cols] = data[cols].apply(pd.to_numeric,errors='coerce').round(2).fillna(data)

    return data


def opslag_all_tabel(df=pd.DataFrame, opslag=str, reg = [str, None], cap = [bool, None]):
    """
    Calculates all the factor ratios (Jyske Quant, Value, Quality, Momentum) for the desired region, sector or industry over time.

    Args:
        df: The dataFrame to make the calculations on.
        opslag: The columnName of the desired filtering condition.
        reg: The name of the region to filter the data on, can also be "Global" if no region is wanted.
        cap: A boolean which decides whether the factor ratios are marketCap weighted or not.

    Returns:
        (df): Returns the factor ratios for the given specifications in a dataFrame for every week_ID contained in the original dataFrame.
    """  
    if opslag == "regionName":
        if (reg == "Global") or (reg == "Custom"):
            opslag = "regionName"
        else:
            opslag = "countryIso"

    bullet = np.unique(df["week"])
    list = pd.concat([doable_tabel(df[df["week"] == x], opslag) for x in bullet])
    list2 = pd.concat([opslag_func_tabel(list, opslag = opslag, reg = reg, cap = cap, week_ID = x) for x in bullet])
    return list2


def opslag_name(type, region):
    """
    Rewrites the "opslag" string on the basis of the information provided in type and region to insure the right filtering. This is necessary because of filtering on sectors
    and industries are the same across regions but not on countries e.g. the "Health Care" sector is in both "Europe" and "Asia" but "Denmark" are only in "Europe" not "Asia".

    Args:
        type: The type of filtering wanted either "Region", "Sector" or "Industry".
        region: The name of the region to filter the data on, can also be "Global" if no region is wanted and Custom if the custom created region is wanted.

    Returns:
        (str): Returns the needed column name to use for the right filtering.
    """  
    if type == "Region":
        if (region == "Global") or (region =="Custom"):
            opslag = "regionName"
        else:
            opslag = "countryIso"
    elif type == "Sector":
        opslag = "sectorName"
    elif type == "Group":
        opslag = "GIC_GROUP_NM"
    elif type == "Industry":
        opslag = "industryName"
    return opslag

def fill_na_Q3(df, subfactor):
    """
    Fills any NAN values in the given (sub)factor score column with "Q3", which doesnt affect the factor ratio but makes sure that no NAN's are taken to the endpoint.

    Args:
        df: The dataFrame to make the calculations on.
        subfactor: A subfactor or factor column.

    Returns:
        (df): Returns the edited dataFrame.
    """  
    df[subfactor] = df[subfactor].fillna("Q3")
    return df

def subfactor_scores_single(data, factor_score_quint, factor_score, subfactor_score, cap = None):
    """
    Calculates the factor ratio of a single subfactor or factor from Jyske Quant for a specific combination of factor and factor score e.g. 
    the factor ratio of "earningsStability" for all "Jyske Quant" "Q2". 

    Args:
        data: The dataFrame to make the calculations on.
        factor_score_quint: A quintil rank for a given factor
        factor_score: A factor column.
        subfactor_score: A subfactor or factor column.

    Returns:
        (df): Returns the factor ratios over time for the given specifications in a dataFrame.
    """  
    factor_score = "jyskeQuantQuint" if factor_score == "Jyske Quant" else "valueQuint" if factor_score == "Value" else "qualityQuint" if factor_score == "Quality" else "momentumQuint" if factor_score == "Momentum" else None
    subfactor_score = "absValueQuint" if subfactor_score == "AbsValue" else "relValueQuint" if subfactor_score == "RelValue" else\
        "profitabilityQuint" if subfactor_score == "Profitability" else "safetyQuint" if subfactor_score == "Safety" else\
        "earningsStabilityQuint" if subfactor_score == "EarningsStability" else "growthQuint" if subfactor_score == "Growth" else\
        "sentimentQuint" if subfactor_score == "Sentiment" else "priceQuint" if subfactor_score == "Price" else "valueQuint" if subfactor_score == "Value" else\
         "qualityQuint" if subfactor_score == "Quality" else "momentumQuint" if subfactor_score == "Momentum" else None
    bullet = np.unique(data["week"])

    data = data[data[factor_score] == factor_score_quint][["week", factor_score, subfactor_score, "marketCap"]]
    data = fill_na_Q3(data, subfactor_score)

    payload = [score(data[(data[factor_score] == factor_score_quint) & (data["week"] == x)].groupby([subfactor_score])["marketCap"].sum()) if cap == True else score(data[(data[factor_score] == factor_score_quint) & (data["week"] == x)].groupby(subfactor_score)[subfactor_score].count()) for x in bullet]

    data = pd.DataFrame([bullet, payload]).transpose()
    data.columns = ["week", re.findall('[a-zA-Z][^Q-Q]*', subfactor_score)[0]+"FactorRatio"]
    data["week"] = pd.to_datetime(data["week"], format = "%Y%m%d")
    data_types = {"week" : str}
    data = data.astype(data_types)
    data.replace([np.inf], np.nan, inplace = True)
    data = data.fillna(method='ffill')
    data.iloc[0,:] = data.iloc[0,:].fillna(0)

    return data

def rename_factors(body):
    """
    Renames the used factor list to the names used in Jyske Quant from the names used as input på the end user e.g. "Jyske Quant" to "jyskeQuantQuint".

    Args:
        body: The factor list wanted to get renamed.

    Returns:
        (list): Returns the edited list of factor names.
    """  
    factor_names = {"Jyske Quant" : "jyskeQuantFactorRatio", "Value" : "valueFactorRatio", "Quality" : "qualityFactorRatio", "Momentum" : "momentumFactorRatio"}

    payload = [factor_names[x] for x in body]

    return payload


def override_iso(dict, data, override_dict):
    """
    Overrides the original data's countryIso and regionName.

    Args:
        dict: A dictionary with the "isin"'s to overwrite as keys and the new "countryIso"'s as values.
        data: The dataFrame containing the data to overwrite.
        override_dict: A dictionary containing all "countryIso"'s as keys and matching "regionName"'s as values.

    Returns:
        (): It has no return argument as it just applies the change to the given dataFrame.
    """  
    bullet = list(dict.keys())
    for x in bullet:
        try:
            y = dict[x]
            data.loc[data["isin"]==x, "countryIso"] = y
            data.loc[data["isin"]==x, "regionName"] = override_dict[y]
        except:
            break


def override_cap(dict, data):
    """
    Overrides the original data's marketCap by multiplying with a factor.

    Args:
        dict: A dictionary with the "isin"'s to overwrite as keys and the factors to mulitply the marketCap with as a float as values.
        data: The dataFrame containing the data to overwrite.

    Returns:
        (): It has no return argument as it just applies the change to the given dataFrame.
    """  
    bullet  =list(dict.keys())

    for x in bullet:
        try:
            y = dict[x]
            data.loc[data["isin"]==x,"marketCap"] = data.loc[data["isin"]==x,"marketCap"]*y
        except:
            break

def clean_dict(df):
    """
    Cleans a dataFrame which is contructed by a dictionary with nested dictionaries, which means it contain NAN values.

    Args:
        df: The dataFrame containing the nested dicts to be cleaned.

    Returns:
        (dict): Returns a dictionary with x nested dictionaries inside.
    """  
    df = df.iloc[:,-2:]
    my_dict = df.to_dict()

    bullet = list(my_dict.keys())
    x = 0
    dicts = []
    while x < len(bullet):
        clean_dict = {k: my_dict[bullet[x]][k] for k in my_dict[bullet[x]] if not pd.isna(my_dict[bullet[x]][k])}

        dicts.append(clean_dict)

        x+=1
    
    clean = dict(zip(bullet, dicts))

    return clean

def filterData(df, opslag, region, filter_str):
    """
    This function is needed because of the output structure wanted by the user with two custom regions. This function therefor makes the neccesary filtering
    based on the input so the data used in the calculations of the factor ratios are a subset to speed up the process.

    Args:
        data: The dataFrame to make the calculations on.
        opslag: The columnName of the desired filtering condition.
        region: The name of the region to filter the data on, can also be "Global" if no region is wanted.
        filter_str: The countryName to rewrite as a countryIso. 

    Returns:
        (df): Returns a subset of the original dataFrame.
    """  

    if opslag == "regionName":
        if filter_str in list(set(np.unique(new_reg_custom(df[df["week"] == df.week.iloc[0]])["regionName"].values)) - set(["North America"])):
            df = new_reg_custom(df)
            df = df[df[opslag] == filter_str]
        elif filter_str in list(set(np.unique(new_reg_global(df[df["week"] == df.week.iloc[0]])["regionName"].values)) - set(["Europe", "Asia", "Emerging Markets", "Japan"])):
            df = new_reg_global(df)
            df = df[df[opslag] == filter_str]
        else:
            df = df[df[opslag] == filter_str]
    elif opslag == "sectorName" or opslag == "industryName"  or opslag == "GIC_GROUP_NM":
        if region == "Global":
            df = df[df[opslag] == filter_str]
        elif region == "United States":
            df = new_reg_global(df)
            df = df[(df["regionName"] == region) & (df[opslag] == filter_str)]
        elif region == "China":
            df = new_reg_global(df)
            df = df[(df["regionName"] == region) & (df[opslag] == filter_str)]
        elif region == "EM ex. China":
            df = new_reg_custom(df)
            df = df[(df["regionName"] == region) & (df[opslag] == filter_str)]
        else:
            df = df[(df["regionName"] == region) & (df[opslag] == filter_str)]
    else:
        df = df[df[opslag] == filter_str]
    return df

def getData(container_over, container_quant):
    """
    This function collects the Data from the research override storage container and appends the newest week(s) from the jyske quant storage container,
    if the the week_ID from the research override container isn't up to date.

    Args:
        container_over: The blob.service container client for research override.
        container_quant: The blob.service container client for jyske Quant.
        
    Returns:
        (df): Returns the newest base dataFrame containing up to 22 columns.
    """  
    lRequestedCols = ["week", "isin", "companyName", "countryIso", "regionName", "sectorName", "GIC_GROUP_NM", "industryName", "marketCap", "jyskeQuantQuint", "valueQuint", "qualityQuint", "momentumQuint", "absValueQuint", "relValueQuint", "profitabilityQuint", "growthQuint", "safetyQuint", "earningsStabilityQuint", "sentimentQuint", "priceQuint", "countryName"]
    my_blobs = container_over.list_blobs()
    file_list = []
    data_list = []
    for blob in my_blobs:
        file_list.append(blob.name)
    try:
        file = [i for i in file_list if i.startswith('all-data_' + GetNextSaturday())]
        file = BytesIO(container_over.download_blob(file[-1]).readall())
        newest_data = pd.read_parquet(file, engine='pyarrow', columns= lRequestedCols)
    except IndexError:
        file = [i for i in file_list if i.startswith('all-data_')]
        file = BytesIO(container_over.download_blob(file[-1]).readall())
        old_data = pd.read_parquet(file, engine='pyarrow', columns= lRequestedCols)
        data_list.append(old_data)
        last_date = max(pd.read_parquet(file, engine='pyarrow', columns= ["week"])["week"])
        numberOfWeeksSinceUpdate = int(((datetime.strptime(str(GetLastSaturday()), "%Y%m%d") - datetime.strptime(str(last_date), "%Y%m%d")).days / 7))
        date_list = [int(datetime.strftime(datetime.strptime(str(last_date), "%Y%m%d") + timedelta(weeks=x), "%Y%m%d")) for x in range(1, numberOfWeeksSinceUpdate +1)]
        file_list2 = []
        my_blobs = container_quant.list_blobs()
        for blob in my_blobs:
            file_list2.append(blob.name)
        for x in date_list:
            file2 = [i for i in file_list2 if i.startswith('weekly-scores_' + str(x))]
            file2 = BytesIO(container_quant.download_blob(file2[-1]).readall())
            data_list.append(pd.read_parquet(file2, engine='pyarrow', columns= lRequestedCols))
        newest_data = pd.concat(data_list, ignore_index= True)
        UploadToBlobNoWeekCol(container_over, newest_data, 'all-data')
    except:
        data_list = []
        file_list2 = []
        my_blobs = container_quant.list_blobs()
        for blob in my_blobs:
            file_list2.append(blob.name)
        file2 = [i for i in file_list2 if i.startswith('weekly-scores_')]
        for i in range(len(file2)):
            file3 = BytesIO(container_quant.download_blob(file2[i]).readall())
            data_list.append(pd.read_parquet(file3, engine='pyarrow', columns= lRequestedCols))
        data = pd.concat(data_list, ignore_index= True)
        data.drop(data.loc[data.week == 20180509].index, inplace=True)
        newest_data = data.drop_duplicates(subset=['week', 'isin'], keep='last')

        countries = pd.Series(newest_data["countryIso"].values, index = newest_data["countryName"]).to_dict()
        country_iso = []
        for i in newest_data.loc[newest_data.loc[:, ("week")] == 20180929, "countryIso"]:
            country_iso.append(countries[i])
        newest_data.loc[newest_data.loc[:, ("week")] == 20180929, "countryIso"] = country_iso
        UploadToBlobNoWeekCol(container_over, newest_data, 'all-data')
    return newest_data.reset_index()


def newest_parquet(service_client = str, lRequestedCols = [list, None], week_ID: Union[int, None] = None):
    keyvault = "kv-dad-d"
    blob_service = BlobConnect(keyvault)
    blob_service_client_jyske_quant = blob_service.get_container_client(container='jyske-quant')
    blob_service_client_research_overrides = blob_service.get_container_client(container='research-overrides')

    if week_ID == None:
        week_ID = int(GetLastSaturday())
    else:
        week_ID = week_ID

    if service_client == "jyske_quant":
        my_blobs = blob_service_client_jyske_quant.list_blobs()
        test = []
        for blob in my_blobs:
            blob.name
            test.append(blob.name)
        test = [i for i in test if i.startswith('weekly-scores_' + str(week_ID))]
        file = io.BytesIO(blob_service_client_jyske_quant.download_blob(test[-1]).readall())
        data = pd.read_parquet(file, engine='pyarrow', columns= lRequestedCols)
    elif service_client == "research_overrides":
        file_list = []
        my_blobs2 = blob_service_client_research_overrides.list_blobs()
        for blob2 in my_blobs2:
            file_list.append(blob2.name)
        file = [i for i in file_list if i.startswith('override_')]
        file = io.BytesIO(blob_service_client_research_overrides.download_blob(file[-1]).readall())
        data = pd.read_parquet(file, engine='pyarrow')

    return data