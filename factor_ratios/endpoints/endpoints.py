from fastapi import APIRouter, Depends
from factor_ratios.functions import GetLastSaturday, GetMaxWeeks
from factor_ratios.schema import Override, ServiceArk, WeekID, Weeks, FactorScores, FactorScoresSingle, SubfactorScores, TypeAndRegion,\
     FaktorList, SubFactorElements, TypeAndRegionList, SubFactorElementsList, SubfactorScoresRoll #SizeFactor
from typing import Union, List
from factor_ratios.internal.factorScores import factorScores
from factor_ratios.internal.factorScoresOverTime import factorScoresOverTime
from factor_ratios.internal.serviceark import serviceark
from factor_ratios.internal.subFactorScores import subFactorScoresOverTime, subFactorScoresOverTime2, subFactorScoresOverTimeRollingMean
from factor_ratios.internal.singleFactorScores import singleFactorScoreOverTime, singleFactorScoreOverTime2
from factor_ratios.internal.override import overrideIsin
#from factor_ratios.internal.sizeFactor import sizeFactor
from factor_ratios.internal.dropDown import drop_down


router = APIRouter()

@router.get(
"/tabel",
response_model=List[FactorScores],
summary="Deskriptive faktorratios (40/40 ratios) på uge basis",
tags = ["Factor ratios"])
async def tabel(week_ID: WeekID=Depends(WeekID), type: TypeAndRegion=Depends(TypeAndRegion), region: TypeAndRegion=Depends(TypeAndRegion), marketcap: Union[bool, None] = None):
    
    """
    Skab den ønskede tabel med faktorratios (40/40 ratios) for Jyske Quant med følgende information

    - **marketcap**: Angives som True, hvis man ønsker at sortere efter marketCap frem for ligevægtet.
    - **week_ID**: Uge ID'et for den ønskede uge, hvis ingen angives bruges seneste data.
    - **type**: Hvilken type man ønsker at sortere efter ["Region", "Sector", "Industry"].
    - **region**: Den region man ønsker at sortere efter ["Global", "Europe", "China", "US", "Emerging Markets", "Asia", "North America", "Custom", "Japan", "EM ex. China"].
 

    """
    week_ID=week_ID.weekID
    if week_ID==None:
        week_ID = int(GetLastSaturday())
    else:
        week_ID = week_ID

    type=type.type

    region=region.region


    response = factorScores(week_ID, type, region, marketcap)

    return response


@router.get("/tabel_historisk",
response_model=List[FactorScores],
summary="Historiske deskriptive faktorratios (40/40 ratios)",
tags = ["Factor ratios"])
async def tabel_historisk(weeks: Weeks=Depends(Weeks), type: TypeAndRegion=Depends(TypeAndRegion), region: TypeAndRegion=Depends(TypeAndRegion), marketcap: Union[bool, None] = None):
   
    """
    Skab den ønskede tabel med faktorratios (40/40 ratios) for Jyske Quant over tid med følgende information.

    - **marketcap**: Angives som True, hvis man ønsker at sortere efter marketCap frem for ligevægtet.
    - **weeks**: Antal uger der ønskes at gå tilbage i tiden fx 52 for et år tilbage.
    - **type**: Hvilken type man ønsker at sortere efter ["Region", "Sector", "Industry"].
    - **region**: Den region man ønsker at sortere efter ["Global", "Europe", "China", "US", "Emerging Markets", "Asia", "North America", "Custom", "Japan", "EM ex. China"].


    """
    weeks = weeks.weeks
    if weeks==None:
        weeks = int(GetMaxWeeks())
    else:
        weeks = weeks

    type=type.type

    region=region.region

    response = factorScoresOverTime(weeks, type, region, marketcap)

    return response

@router.post("/plot_faktor",
response_model=List[FactorScoresSingle],
response_model_exclude_unset=True,
summary="Historiske deskriptive faktorratios (40/40 ratios) for en udvalgt faktorer",
tags = ["A single factor"])
async def tabel_historisk(weeks: Weeks=Depends(Weeks), type: TypeAndRegion=Depends(TypeAndRegion), region: TypeAndRegion=Depends(TypeAndRegion), marketcap: Union[bool, None] = None, filter_str: Union[str, None] = None, faktor_list: FaktorList=Depends(FaktorList)):
   
    """
    Skab den ønskede tabel med faktorratios (40/40 ratios) for Jyske Quant over tid med følgende information.

    - **marketcap**: Angives som True, hvis man ønsker at sortere efter marketCap frem for ligevægtet.
    - **filter_str**: Den enkelte region, land, sektor eller industri, der ønkses faktor ratios for.
    - **weeks**: Antal uger der ønskes at gå tilbage i tiden fx 52 for et år tilbage.
    - **type**: Hvilken type man ønsker at sortere efter ["Region", "Sector", "Industry"].
    - **region**: Den region man ønsker at sortere efter ["Global", "Europe", "China", "US", "Emerging Markets", "Asia", "North America", "Custom", "Japan", "EM ex. China"].
    - **Request body": En liste med de faktorer, der ønskes beregninger for. Dette kan være alle kombinationer af denne liste ["Jyske Quant", "Value", "Quality", "Momentum"].

    """
    
    weeks = weeks.weeks
    if weeks==None:
        weeks = int(GetMaxWeeks())
    else:
        weeks = weeks

    type=type.type
    region=region.region
    faktor_list = faktor_list.faktorList

    response = singleFactorScoreOverTime(weeks, type, region, marketcap, filter_str, faktor_list)

    return response

@router.post("/plot_faktor_multi",
response_model=List[FactorScoresSingle],
response_model_exclude_unset=True,
summary="Historiske deskriptive faktorratios (40/40 ratios) for flere udvalgte faktorer",
tags = ["A single factor"])
async def tabel_historisk(weeks: Weeks=Depends(Weeks), type: TypeAndRegionList=Depends(TypeAndRegionList), region: TypeAndRegionList=Depends(TypeAndRegionList), marketcap: Union[bool, None] = None, filter_str: Union[list, None] = None, faktor_list: FaktorList=Depends(FaktorList)):
   
    """
    Skab den ønskede tabel med faktorratios (40/40 ratios) for Jyske Quant over tid med følgende information.

    - **marketcap**: Angives som True, hvis man ønsker at sortere efter marketCap frem for ligevægtet.
    - **weeks**: Antal uger der ønskes at gå tilbage i tiden fx 52 for et år tilbage.
    - **Request body**:\\
    **filter_str**: Den enkelte region, land, sektor eller industri, der ønkses faktor ratios for som en liste.\\
    **type**: Hvilken type man ønsker at sortere efter ["Region", "Sector", "Industry"] som en liste af kombinationer af foranstående liste.\\
    **region**: Den region man ønsker at sortere efter ["Global", "Europe", "China", "US", "Emerging Markets", "Asia", "North America", "Custom", "Japan", "EM ex. China"] som
    en liste af kombinationer af foranstående liste.\\
    **faktorList**: Den eller de faktorer fra Jyske Quant universet man ønsker beregninger for ["Jyske Quant", "Value", "Quality", "Momentum"] som en liste af kombinationer af foranstående liste

    """
    weeks = weeks.weeks
    if weeks==None:
        weeks = int(GetMaxWeeks())
    else:
        weeks = weeks

    type = type.type
    region = region.region
    faktor_list = faktor_list.faktorList

    response = singleFactorScoreOverTime2(weeks, type, region, marketcap, filter_str, faktor_list)

    return response

'''
@router.get("/plot_size",
response_model = SizeFactor,
summary="Antal rækker per uge ved den valgte filtrering",
tags = ["A single factor"])
async def plot_sub(type: TypeAndRegion=Depends(TypeAndRegion), region: TypeAndRegion=Depends(TypeAndRegion), filter_str: Union[str, None] = None):

    """
    Tjekker antal papirer i Jyske Quant med de angivende filtreringer.

    - **type**: Hvilken type man ønsker at sortere efter ["Region", "Sector", "Industry"].
    - **region**: Den region man ønsker at sortere efter ["Global", "Europe", "China", "US", "Emerging Markets", "Asia", "North America", "Custom", "Japan", "EM ex. China"].
    - **filter_str**: Den enkelte region, land, sektor eller industri, der ønkses antal papirer for.
    """

    type=type.type
    region=region.region


    response = sizeFactor(type, region, filter_str)


    return response
'''

@router.get("/plot_sub",
response_model=List[SubfactorScores],
response_model_exclude_unset=True,
summary="Historiske deskriptive faktorratios (40/40 ratios) for en udvalgt subfaktor",
tags = ["A single subfactor"])
async def plot_sub(weeks: Weeks=Depends(Weeks), factor_score_quint: SubFactorElements=Depends(SubFactorElements), factor_score: SubFactorElements=Depends(SubFactorElements), subfactor_score: SubFactorElements=Depends(SubFactorElements), marketcap: Union[bool, None] = None):
   
    """
    Skab den ønskede tabel med faktorratios (40/40 ratios) for Jyske Quant subfaktorer eller faktorer over tid med følgende information

    - **marketcap**: Angives som True, hvis man ønsker at sortere efter marketCap frem for ligevægtet
    - **weeks**: Antal uger der ønskes at gå tilbage i tiden fx 52 for et år tilbage.
    - **factor_score_quint**: Hvilken quintil der ønskes en tabel for ["Q1", "Q2", "Q3", "Q4", "Q5"].
    - **factor_score**: Den Jyske Quant faktor ovenstående quintil skal tages fra ["JyskeQuant", "Value", "Quality", "Momentum"].
    - **factor_subscore**: Den subfaktor der ønskes 40/40 ratios af målt kun på en filtrering af ovenstående valg ["AbsValue", "RelValue", "Profitability", "Growth", "Safety", "EarningsStability", "Sentiment", "Price"].

    """

    weeks = weeks.weeks
    if weeks==None:
        weeks = int(GetMaxWeeks())
    else:
        weeks = weeks

    factor_score_quint = factor_score_quint.factor_score_quint
    factor_score = factor_score.factor_score
    subfactor_score = subfactor_score.subfactor_score

    response = subFactorScoresOverTime(weeks, factor_score_quint, factor_score, subfactor_score, marketcap)

    return response

@router.post("/plot_sub_multi",
response_model=List[SubfactorScores],
response_model_exclude_unset=True,
summary="Historiske deskriptive faktorratios (40/40 ratios) for flere udvalgt subfaktorer",
tags = ["A single subfactor"])
async def plot_sub(weeks: Weeks=Depends(Weeks), factor_score_quint: SubFactorElementsList = Depends(SubFactorElementsList), factor_score: SubFactorElementsList = Depends(SubFactorElementsList), subfactor_score: SubFactorElementsList = Depends(SubFactorElementsList), marketcap: Union[bool, None] = None):
   
    """
    Skab den ønskede tabel med faktorratios (40/40 ratios) for Jyske Quant subfaktorer eller faktorer over tid med følgende information

    - **marketcap**: Angives som True, hvis man ønsker at sortere efter marketCap frem for ligevægtet.
    - **weeks**: Antal uger der ønskes at gå tilbage i tiden fx 52 for et år tilbage.
    - **Request body**:\\
    **factor_score_quint**: Hvilke kvintiler der ønskes en tabel for ["Q1", "Q2", "Q3", "Q4", "Q5"] som en liste af kombinationer af foranstående liste.
    **factor_score**: De Jyske Quant faktorer ovenstående kvintiler skal tages fra ["JyskeQuant", "Value", "Quality", "Momentum"] som en liste af kombinationer af foranstående liste.
    **factor_subscore**: De subfaktorer der ønskes 40/40 ratios af målt kun på en filtrering af ovenstående valg ["AbsValue", "RelValue", "Profitability", "Growth", "Safety",
     "EarningsStability", "Sentiment", "Price"] som en liste af kombinationer af foranstående liste.

    """

    weeks = weeks.weeks
    if weeks==None:
        weeks = int(GetMaxWeeks())
    else:
        weeks = weeks

    factor_score_quint = factor_score_quint.factor_score_quint
    factor_score = factor_score.factor_score
    subfactor_score = subfactor_score.subfactor_score

    response = subFactorScoresOverTime2(weeks, factor_score_quint, factor_score, subfactor_score, marketcap)

    return response

@router.post("/plot_sub_multi_roll",
response_model=List[SubfactorScoresRoll],
response_model_exclude_unset=True,
summary="Historiske deskriptive faktorratios (40/40 ratios) for flere udvalgt subfaktorer",
tags = ["A single subfactor"])
async def plot_sub(weeks: Weeks=Depends(Weeks), factor_score_quint: SubFactorElementsList = Depends(SubFactorElementsList), factor_score: SubFactorElementsList = Depends(SubFactorElementsList), subfactor_score: SubFactorElementsList = Depends(SubFactorElementsList), marketcap: Union[bool, None] = None, roll: Union[int, None] = None):
   
    """
    Skab den ønskede tabel med faktorratios (40/40 ratios) for Jyske Quant subfaktorer eller faktorer over tid med følgende information

    - **marketcap**: Angives som True, hvis man ønsker at sortere efter marketCap frem for ligevægtet.
    - **roll**: Det ønskede antal uger det rullende gennemsnit skal beregnes på.
    - **weeks**: Antal uger der ønskes at gå tilbage i tiden fx 52 for et år tilbage.
    - **Request body**:\\
    **factor_score_quint**: Hvilke kvintiler der ønskes en tabel for ["Q1", "Q2", "Q3", "Q4", "Q5"] som en liste af kombinationer af foranstående liste.
    **factor_score**: De Jyske Quant faktorer ovenstående kvintiler skal tages fra ["JyskeQuant", "Value", "Quality", "Momentum"] som en liste af kombinationer af foranstående liste.
    **factor_subscore**: De subfaktorer der ønskes 40/40 ratios af målt kun på en filtrering af ovenstående valg ["AbsValue", "RelValue", "Profitability", "Growth", "Safety",
     "EarningsStability", "Sentiment", "Price"] som en liste af kombinationer af foranstående liste.

    """

    weeks = weeks.weeks
    if weeks==None:
        weeks = int(GetMaxWeeks())
    else:
        weeks = weeks

    factor_score_quint = factor_score_quint.factor_score_quint
    factor_score = factor_score.factor_score
    subfactor_score = subfactor_score.subfactor_score

    response = subFactorScoresOverTimeRollingMean(weeks, factor_score_quint, factor_score, subfactor_score, marketcap, roll)

    return response

@router.get("/serviceark",
response_model=List[ServiceArk],
summary= "Dataudtræk af udvalgte kategorier",
tags = ["Serviceark"])
async def service(week_ID: WeekID=Depends(WeekID)):
    """
    Få data udtræk for Jyske Quant for følgende koloner:
    - ["week", "SEDOL", "isin", "companyName", "countryIso", "regionName", "sectorName", "GIC_GROUP_NM", "industryName", "marketCap", "jyskeQuantQuint", "valueQuint",
      "qualityQuint", "momentumQuint" , "jyskeQuantScore", "valueScore", "qualityScore", "momentumScore", "absValueQuint", "relValueQuint", "profitabilityQuint",
      "growthQuint", "safetyQuint", "earningsStabilityQuint", "sentimentQuint", "priceQuint"]

    Med følgende informationer:

    - **week_ID**: Uge ID'et for den ønskede uge, hvis ingen angives bruges seneste data.

    """

    week_ID=week_ID.weekID
    if week_ID==None:
        week_ID = int(GetLastSaturday())
    else:
        week_ID = week_ID
    
    response = serviceark(week_ID)

    return response


@router.put("/override",
summary = "Omskrivning af data",
tags = ["Override"])
async def override(iso_change: Override=Depends(Override), cap_factor: Override=Depends(Override)):
    '''
    Opdatering af et dictionary, hvor der angives to nested dictionaries. Først et dict med "isin"'s på papirer som "keys" og "Iso" på det land, 
    det pågældende papir ønskes ændret til som "value", hvor efter region også bliver opdateret. Dernæst et dict ligeledes med "isin"'s på papirer som "keys" 
    og en float som "value", der angiver hvilken faktor det ønskede papirs omsætning ønskes ganget med for at ændre marketCap til den andel af aktier, der handles.

    '''
    iso_change = iso_change.isoChange
    cap_factors = cap_factor.capFactors

    response = overrideIsin(iso_change, cap_factors)

    return response

@router.get("/drop_down",
summary="Mulige input til brug i andre endpoints",
tags = ["Drop_down"])
async def drop_down_excel():
    '''
    SKaber en tabel med mulige valg for drop down menu i excel ark. De mulige valg skabes udelukkende på baggrund af, hvilke land, region, sektorer, industry grupper
    og industrier, der indgår i den pågældende uge i Jyske Quant.
    '''

    response = drop_down()

    return response