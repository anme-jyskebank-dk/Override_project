from typing import Optional, List, Dict, Union
from math import isnan
from pydantic import BaseModel as PydanticBaseModel, validator, Field #, root_validator Pydantic
from factor_ratios.functions import GetLastSaturday, DateIsSaturday, GetMaxWeeks
from itertools import permutations, product


iQuantStartWeek=20170101

### GENERAL and request models###
class BaseModel(PydanticBaseModel):
    @validator('*', pre=True)
    def change_nan_to_none(cls, v, field):
        if field.outer_type_ is float and (isnan(v) or v is None):
            return None
        return v

class WeekID(PydanticBaseModel):
    weekID: Optional[int] = Field(None,description='The date for which the data relates [YYYYMMDD], and must be a saturday',ge=iQuantStartWeek+6,le=int(GetLastSaturday()))

    @validator('weekID')
    def weekID_validation(cls,weekID):
        if weekID!=None:
            if weekID<iQuantStartWeek or weekID>int(GetLastSaturday()):
                raise ValueError(f'WeekID most be between {iQuantStartWeek+6} and {GetLastSaturday()}')
            if DateIsSaturday(weekID):
                raise ValueError('Date provided is not a Saturday')
            if isinstance(weekID, int) is False:
                raise ValueError('Date provided must be a integer of the format [YYYYMMDD]')
        return weekID

class Weeks(PydanticBaseModel):
    weeks: Optional[int] = Field(None,description='The number of weeks back in time',ge=1,le=int(GetMaxWeeks()))

    @validator('weeks')
    def weeks_validation(cls,weeks):
        if weeks!=None:
            if weeks<1:
                raise ValueError(f'Weeks most be between {1} and {GetMaxWeeks()}')
            if isinstance(weeks, int) is False:
                raise ValueError('Weeks must be a integer')
        return weeks

class TypeAndRegion(PydanticBaseModel):
    type: Optional[str] = Field(None,description= 'The type for which to filter the data, must be Region, Sector or Industry')
    region: Optional[str] = Field(None,description= 'The area for which to filter the data, must be Global, Europe, China, US, Emerging Markets, Asia, North America or Custom')

    @validator('type')
    def type_validation(cls,type):
        if isinstance(type, str) is False:
            raise ValueError('Type provided must be a str')
        if type not in ["Region", "Sector", "Group", "Industry"]:
            raise ValueError('Type provided must be either Region, Sector, Group or Industry')
        return type

    @validator('region')
    def region_validation(cls,region):
        if isinstance(region, str) is False:
            raise ValueError('Region provided must be a str')
        if region not in ["Global", "Europe", "China", "United States", "Emerging Markets", "Asia", "North America", "Custom", "Japan", "EM ex. China"]:
            raise ValueError('Region provided must be either Global, Europe, China, United States, Emerging Markets, Asia, North America, Japan, EM ex. China or Custom')
        return region

class TypeAndRegionList(PydanticBaseModel):
    type: List[str] = Field(None,description= 'The type for which to filter the data, must be Region, Sector or Industry')
    region: List[str] = Field(None,description= 'The area for which to filter the data, must be Global, Europe, China, US, Emerging Markets, Asia, North America or Custom')

    @validator('type')
    def type_validation(cls,type):
        if isinstance(type, list) is False:
            raise ValueError('Types provided must be a list')
        if type not in [list(i) for i in product(["Region", "Sector", "Group", "Industry"], ["Region", "Sector", "Group", "Industry"]) ]:
            raise ValueError('Types provided must be either Region, Sector, Group or Industry')
        return type

    @validator('region')
    def region_validation(cls,region):
        if isinstance(region, list) is False:
            raise ValueError('Regions provided must be a str')
        if region not in [list(i) for i in product(["Global", "Europe", "China", "United States", "Emerging Markets", "Asia", "North America", "Custom", "Japan", "EM ex. China"],
         ["Global", "Europe", "China", "United States", "Emerging Markets", "Asia", "North America", "Custom", "Japan", "EM ex. China"])]:
            raise ValueError('Regions provided must be a list containing either Global, Europe, China, United States, Emerging Markets, Asia, North America, Japan, EM ex. China or Custom or a combination of the before mentioned')
        return region

class SubFactorElements(PydanticBaseModel):
    factor_score_quint: Optional[str] = Field(None,description= 'The the factor score for which to filter the data, must be Q1, Q2, Q3, Q4, Q5')
    factor_score: Optional[str] = Field(None,description= 'The type og factor score for which to filter the data, must be Jyske Quant, Value, Quality, Momentum')
    subfactor_score: Optional[str] = Field(None,description= 'The type of factor or subfactor scorefor which to filter the data, must be Jyske Quant, Value, Quality or Momentum, AbsValue, RelValue, Profitability, Safety, Growth, EarningsStability, Sentiment, Price')

    @validator('factor_score_quint')
    def factor_score_quint_validation(cls,factor_score_quint):
        if isinstance(factor_score_quint, str) is False:
            raise ValueError('factor_score_quint provided must be a str')
        if factor_score_quint not in ["Q1", "Q2", "Q3", "Q4", "Q5"]:
            raise ValueError('factor_score_quint provided must be either Q1, Q2, Q3, Q4 or Q5')
        return factor_score_quint
    @validator('factor_score')
    def factor_score_validation(cls,factor_score):
        if isinstance(factor_score, str) is False:
            raise ValueError('factor_score provided must be a str')
        if factor_score not in ["Jyske Quant", "Value", "Quality", "Momentum"]:
            raise ValueError('factor_Score provided must be either Jyske Quant, Value, Quality or Momentum')
        return factor_score

class SubFactorElementsList(PydanticBaseModel):
    factor_score_quint: List[str] = Field(None,description= 'The the factor score for which to filter the data, must be Q1, Q2, Q3, Q4, Q5')
    factor_score: List[str] = Field(None,description= 'The type og factor score for which to filter the data, must be Jyske Quant, Value, Quality, Momentum')
    subfactor_score: List[str] = Field(None,description= 'The type of factor or subfactor scorefor which to filter the data, must be Jyske Quant, Value, Quality or Momentum, AbsValue, RelValue, Profitability, Safety, Growth, EarningsStability, Sentiment, Price')

    @validator('factor_score_quint')
    def factor_score_quint_validation(cls,factor_score_quint):
        if isinstance(factor_score_quint, list) is False:
            raise ValueError('factor_score_quint provided must be a list')
        if factor_score_quint not in [list(i) for i in product(["Q1", "Q2", "Q3", "Q4", "Q5"], ["Q1", "Q2", "Q3", "Q4", "Q5"]) ]:
            raise ValueError('factor_score_quint provided must be a list of either Q1, Q2, Q3, Q4 or Q5')
        return factor_score_quint
    @validator('factor_score')
    def factor_score_validation(cls,factor_score):
        if isinstance(factor_score, list) is False:
            raise ValueError('factor_score provided must be a list')
        if factor_score not in [list(i) for i in product(["Jyske Quant", "Value", "Quality", "Momentum"], ["Jyske Quant", "Value", "Quality", "Momentum"]) ]:
            raise ValueError('factor_Score provided must be a list of either Jyske Quant, Value, Quality or Momentum')
        return factor_score
    
    @validator('subfactor_score')
    def subfactor_score_validation(cls,subfactor_score):
        if isinstance(subfactor_score, list) is False:
            raise ValueError('subfactor_score provided must be a list')
        if subfactor_score not in [list(i) for i in product(["Value", "Quality", "Momentum", "AbsValue", "RelValue", "Profitability", "Safety", "Growth", "EarningsStability", "Sentiment", "Price"], ["Value", "Quality", "Momentum", "AbsValue", "RelValue", "Profitability", "Safety", "Growth", "EarningsStability", "Sentiment", "Price"]) ]:
            raise ValueError('subfactor_score provided must be either Value, Quality, Momentum, AbsValue, RelValue, Profitability, Safety, Growth, EarningsStability, Sentiment or Price')
        return subfactor_score

''' Kommenteret ud for at give mulighed for nye industrier, lande osv. at blive tilføjet løbende.
class FilterStr(PydanticBaseModel):
    filter_str: Optional[str] = Field(None,description= 'The type for which to filter the data for a single country, region, sector, industry, regional sector or regional industry')

    @validator('filter_str')
    def filter_str_validation(cls,filter_str):
        if isinstance(filter_str, str) is False:
            raise ValueError('The provided filter_str must be a str and a single country, sector or industry from the Jyske Quant universe')
        if filter_str not in ['Global', 'Europe', 'China', 'US', 'Emerging Markets','Asia', 'North America', 'Custom', 'EM ex. China', 'United Arab Emirates', 'United Kingdom', 'Argentina', 'United States', 'Australia', 'Austria', 'Belgium', 'Luxembourg',
       'Bangladesh', 'Brazil', 'Canada', 'Switzerland', 'Chile', 'Hong Kong SAR', 'China', 'Taiwan (Chinese Taipei)', 'Colombia',
       'Germany', 'France', 'Denmark', 'Spain', 'Finland', 'Greece', 'Singapore', 'Hungary', 'Indonesia', 'India', 'Ireland', 'Iceland',
       'Israel', 'Italy', 'Jordan', 'Japan', 'Korea', 'Kuwait', 'Morocco', 'Mexico', 'Malaysia', 'Nigeria', 'Kenya', 'Netherlands', 'Norway',
       'New Zealand', 'Peru', 'Philippines', 'Poland', 'Portugal', 'Qatar', 'Romania', 'Saudi Arabia', 'Slovenia', 'Sweden',
       'Thailand', 'Turkey', 'Vietnam', 'South Africa', 'Industrials', 'Communication Services', 'Health Care',
       'Consumer Discretionary', 'Energy', 'Materials', 'Consumer Staples', 'Information Technology', 'Airlines', 'Diversified Telecommunication Services',
       'Health Care Providers and Services', 'Specialty Retail','Energy Equipment and Services', 'Chemicals','Oil, Gas and Consumable Fuels', 'Metals and Mining',
       'Food and Staples Retailing', 'Containers and Packaging', 'Commercial Services and Supplies', 'Multiline Retail',
       'Trading Companies and Distributors', 'Professional Services','Biotechnology', 'Health Care Equipment and Supplies',
       'Hotels, Restaurants and Leisure', 'IT Services', 'Transportation Infrastructure', 'Software', 'Interactive Media and Services', 'Health Care Technology',
       'Road and Rail', 'Beverages', 'Diversified Consumer Services', 'Machinery', 'Construction and Engineering', 'Pharmaceuticals', 'Distributors', 'Food Products', 'Media',
       'Semiconductors and Semiconductor Equipment', 'Wireless Telecommunication Services', 'Tobacco', 'Textiles, Apparel and Luxury Goods', 'Electrical Equipment',
       'Paper and Forest Products', 'Personal Products','Internet and Direct Marketing Retail', 'Aerospace and Defense', 'Auto Components', 'Leisure Products', 'Construction Materials',
       'Life Sciences Tools and Services', 'Technology Hardware, Storage and Peripherals', 'Building Products', 'Marine', 'Industrial Conglomerates', 'Household Durables',
       'Automobiles', 'Electronic Equipment, Instruments and Components', 'Air Freight and Logistics', 'Communications Equipment', 'Entertainment', 'Household Products', 
       'Automobiles and Components', 'Capital Goods', 'Commercial  and Professional Services', 'Consumer Durables and Apparel', 'Consumer Services', 'Diversified Financials', 'Energy',
       'Food and Staples Retailing','Food, Beverage and Tobacco', 'Health Care Equipment and Services', 'Household and Personal Products', 'Materials', 'Media',
       'Media and Entertainment','Pharmaceuticals, Biotechnology and Life Sciences', 'Real Estate', 'Retailing', 'Semiconductors and Semiconductor Equipment',
       'Software and Services', 'Technology Hardware and Equipment', 'Telecommunication Services', 'Transportation', 'Utilities']:
            raise ValueError('filter_str is not a country, sector, industry groups or industry from the Jyske Quant universe')
        return filter_str

class FilterStrList(PydanticBaseModel):
    filter_str: List[str] = Field(example = '["Germany", "Health Care"]')

    @validator('filter_str')
    def filter_str_validation(cls,filter_str):
        if filter_str not in [list(per) for sub in range(2) for per in permutations(['Global', 'Europe', 'China', 'US', 'Emerging Markets','Asia', 'North America', 'Custom', 'EM ex. China', 'United Arab Emirates', 'United Kingdom', 'Argentina', 'United States', 'Australia', 'Austria', 'Belgium', 'Luxembourg',
       'Bangladesh', 'Brazil', 'Canada', 'Switzerland', 'Chile', 'Hong Kong SAR', 'China', 'Taiwan (Chinese Taipei)', 'Colombia',
       'Germany', 'France', 'Denmark', 'Spain', 'Finland', 'Greece', 'Singapore', 'Hungary', 'Indonesia', 'India', 'Ireland', 'Iceland',
       'Israel', 'Italy', 'Jordan', 'Japan', 'Korea', 'Kuwait', 'Morocco', 'Mexico', 'Malaysia', 'Nigeria', 'Kenya', 'Netherlands', 'Norway',
       'New Zealand', 'Peru', 'Philippines', 'Poland', 'Portugal', 'Qatar', 'Romania', 'Saudi Arabia', 'Slovenia', 'Sweden',
       'Thailand', 'Turkey', 'Vietnam', 'South Africa', 'Industrials', 'Communication Services', 'Health Care',
       'Consumer Discretionary', 'Energy', 'Materials', 'Consumer Staples', 'Information Technology', 'Airlines', 'Diversified Telecommunication Services',
       'Health Care Providers and Services', 'Specialty Retail','Energy Equipment and Services', 'Chemicals','Oil, Gas and Consumable Fuels', 'Metals and Mining',
       'Food and Staples Retailing', 'Containers and Packaging', 'Commercial Services and Supplies', 'Multiline Retail',
       'Trading Companies and Distributors', 'Professional Services','Biotechnology', 'Health Care Equipment and Supplies',
       'Hotels, Restaurants and Leisure', 'IT Services', 'Transportation Infrastructure', 'Software', 'Interactive Media and Services', 'Health Care Technology',
       'Road and Rail', 'Beverages', 'Diversified Consumer Services', 'Machinery', 'Construction and Engineering', 'Pharmaceuticals', 'Distributors', 'Food Products', 'Media',
       'Semiconductors and Semiconductor Equipment', 'Wireless Telecommunication Services', 'Tobacco', 'Textiles, Apparel and Luxury Goods', 'Electrical Equipment',
       'Paper and Forest Products', 'Personal Products','Internet and Direct Marketing Retail', 'Aerospace and Defense', 'Auto Components', 'Leisure Products', 'Construction Materials',
       'Life Sciences Tools and Services', 'Technology Hardware, Storage and Peripherals', 'Building Products', 'Marine', 'Industrial Conglomerates', 'Household Durables',
       'Automobiles', 'Electronic Equipment, Instruments and Components', 'Air Freight and Logistics', 'Communications Equipment', 'Entertainment', 'Household Products', 
       'Automobiles and Components', 'Capital Goods', 'Commercial  and Professional Services', 'Consumer Durables and Apparel', 'Consumer Services', 'Diversified Financials', 'Energy',
       'Food and Staples Retailing','Food, Beverage and Tobacco', 'Health Care Equipment and Services', 'Household and Personal Products', 'Materials', 'Media', 'Media and Entertainment',
       'Pharmaceuticals, Biotechnology and Life Sciences', 'Real Estate', 'Retailing', 'Semiconductors and Semiconductor Equipment', 'Software and Services',
       'Technology Hardware and Equipment', 'Telecommunication Services', 'Transportation', 'Utilities'], sub + 1)]:
            raise ValueError('filter_str is not a list of countries, sectors, industry groups, industries or a combination of the before mentioned from the Jyske Quant universe')
        return filter_str
'''

class FaktorList(PydanticBaseModel):
    faktorList: List[str] = Field(example = '["Jyske Quant", "Value"]') 

    @validator('faktorList')
    def faktorList_validation(cls,faktorList):
        if isinstance(faktorList, list) is False:
            raise ValueError('faktorList must be a list')
        if faktorList not in [list(per) for sub in range(4) for per in permutations(["Jyske Quant", "Value", "Quality", "Momentum"], sub + 1)]:
            raise ValueError('The faktorList provided must be either Jyske Quant, Value, Quality, Momentum or a combination of these')
        return faktorList


class Override(PydanticBaseModel):
    isoChange: Dict[str, str] = Field(..., description= 'Dictionary med isins som keys og countryisos som values', example = {"DK0010272202": "CHN","DK0060252690": "CAN" }) 
    capFactors: Dict[str, float] = Field(example = {"SA14TG012N13": 0.05})

    @validator('isoChange')
    def isoChange_validation(cls,isoChange):
        if isinstance(isoChange, dict) is False:
            raise ValueError('isoChange must be a dictionary')
        return isoChange
    
    @validator('capFactors')
    def capFactor_validation(cls,capFactors):
        if isinstance(capFactors, dict) is False:
            raise ValueError('capFactor must be a dictionary')
        return capFactors


### Endpoints Responsemodels ###
class FactorScores(BaseModel):
    type: str = Field(..., description='The type of filtering done by the endpoint. Can be Regions, countries, sectors or industries')
    companyQuantity: int = Field(..., description='The sum of companies')
    share: Union[float, str] = Field(..., description='The share of companies compared to the total in the picked filtering type, or the of marketcap compared to the total in the picked filtering type')
    jyskeQuantFactorRatio: Union[float, str] = Field(..., description='The Jyske Quant quintile of the companies (Q1=Best,... Q5=Worst) made to a factorscore by (Q1 + Q2) / (Q4 + Q5)')
    valueFactorRatio: Union[float, str] = Field(..., description='The Value quintile of the companies (Q1=Best,... Q5=Worst) made to a factorscore by (Q1 + Q2) / (Q4 + Q5)')
    qualityFactorRatio: Union[float, str] = Field(..., description='The Quality quintile of the companies (Q1=Best,... Q5=Worst) made to a factorscore by (Q1 + Q2) / (Q4 + Q5)')
    momentumFactorRatio: Union[float, str] = Field(..., description='The Momentum quintile of the companies (Q1=Best,... Q5=Worst) made to a factorscore by (Q1 + Q2) / (Q4 + Q5)')
    shareQ1: float = Field(..., description='The share of companies in Jyske Quant Q1, or the share of market cap in Jyske Quant Q1')
    week: str = Field(..., description='The date when data was calculated')

class FactorScoresSingle(BaseModel):
    filterParam: str = Field(..., description='The parameter, which the data was filtered upon')
    week: str = Field(..., description='The date when data was calculated')
    jyskeQuantFactorRatio: Union[float, str] = Field(None, description='The Jyske Quant quintile of the companies (Q1=Best,... Q5=Worst) made to a factorscore by (Q1 + Q2) / (Q4 + Q5)')
    valueFactorRatio: Union[float, str] = Field(None, description='The Value quintile of the companies (Q1=Best,... Q5=Worst) made to a factorscore by (Q1 + Q2) / (Q4 + Q5)')
    qualityFactorRatio: Union[float, str] = Field(None, description='The Quality quintile of the companies (Q1=Best,... Q5=Worst) made to a factorscore by (Q1 + Q2) / (Q4 + Q5)')
    momentumFactorRatio: Union[float, str] = Field(None, description='The Momentum quintile of the companies (Q1=Best,... Q5=Worst) made to a factorscore by (Q1 + Q2) / (Q4 + Q5)')

class SubfactorScores(BaseModel):
    week: str = Field(..., description='The date when data was calculated')
    valueFactorRatio: float = Field(None, description='The Value quintile of the companies (Q1=Best,... Q5=Worst) made to a factorscore by (Q1 + Q2) / (Q4 + Q5)')
    qualityFactorRatio: float = Field(None, description='The Quality quintile of the companies (Q1=Best,... Q5=Worst) made to a factorscore by (Q1 + Q2) / (Q4 + Q5)')
    momentumFactorRatio: float = Field(None, description='The Momentum quintile of the companies (Q1=Best,... Q5=Worst) made to a factorscore by (Q1 + Q2) / (Q4 + Q5)')
    absValueFactorRatio: float = Field(None, description='The quintile of Absolute value (Q1=Best,... Q5=Worst) made to a factorscore by (Q1 + Q2) / (Q4 + Q5)')
    relValueFactorRatio: float = Field(None, description='The quintile of Relative value (Q1=Best,... Q5=Worst) made to a factorscore by (Q1 + Q2) / (Q4 + Q5)')
    profitabilityFactorRatio: float = Field(None, description='The quintile of Profitability (Q1=Best,... Q5=Worst) made to a factorscore by (Q1 + Q2) / (Q4 + Q5)')
    safetyFactorRatio: float = Field(None, description='The quintile of Safety (Q1=Best,... Q5=Worst) made to a factorscore by (Q1 + Q2) / (Q4 + Q5)')
    growthFactorRatio: float = Field(None, description='The quintile of Growth (Q1=Best,... Q5=Worst) made to a factorscore by (Q1 + Q2) / (Q4 + Q5)')
    earningsStabilityFactorRatio: float = Field(None, description='The quintile of Earnings stability (Q1=Best,... Q5=Worst) made to a factorscore by (Q1 + Q2) / (Q4 + Q5)')
    sentimentFactorRatio: float = Field(None, description='The quintile of Sentiment (Q1=Best,... Q5=Worst) made to a factorscore by (Q1 + Q2) / (Q4 + Q5)')
    priceFactorRatio: float = Field(None, description='The quintile of Price (Q1=Best,... Q5=Worst) made to a factorscore by (Q1 + Q2) / (Q4 + Q5)')

class SubfactorScoresRoll(BaseModel):
    week: str = Field(..., description='The date when data was calculated')
    valueFactorRatio: float = Field(None, description='The Value quintile of the companies (Q1=Best,... Q5=Worst) made to a factorscore by (Q1 + Q2) / (Q4 + Q5)')
    qualityFactorRatio: float = Field(None, description='The Quality quintile of the companies (Q1=Best,... Q5=Worst) made to a factorscore by (Q1 + Q2) / (Q4 + Q5)')
    momentumFactorRatio: float = Field(None, description='The Momentum quintile of the companies (Q1=Best,... Q5=Worst) made to a factorscore by (Q1 + Q2) / (Q4 + Q5)')
    absValueFactorRatio: float = Field(None, description='The quintile of Absolute value (Q1=Best,... Q5=Worst) made to a factorscore by (Q1 + Q2) / (Q4 + Q5)')
    relValueFactorRatio: float = Field(None, description='The quintile of Relative value (Q1=Best,... Q5=Worst) made to a factorscore by (Q1 + Q2) / (Q4 + Q5)')
    profitabilityFactorRatio: float = Field(None, description='The quintile of Profitability (Q1=Best,... Q5=Worst) made to a factorscore by (Q1 + Q2) / (Q4 + Q5)')
    safetyFactorRatio: float = Field(None, description='The quintile of Safety (Q1=Best,... Q5=Worst) made to a factorscore by (Q1 + Q2) / (Q4 + Q5)')
    growthFactorRatio: float = Field(None, description='The quintile of Growth (Q1=Best,... Q5=Worst) made to a factorscore by (Q1 + Q2) / (Q4 + Q5)') 
    earningsStabilityFactorRatio: float = Field(None, description='The quintile of Earnings stability (Q1=Best,... Q5=Worst) made to a factorscore by (Q1 + Q2) / (Q4 + Q5)')
    sentimentFactorRatio: float = Field(None, description='The quintile of Sentiment (Q1=Best,... Q5=Worst) made to a factorscore by (Q1 + Q2) / (Q4 + Q5)')
    priceFactorRatio: float = Field(None, description='The quintile of Price (Q1=Best,... Q5=Worst) made to a factorscore by (Q1 + Q2) / (Q4 + Q5)')
    movingAvgValue: float = Field(None, description='The rolling mean calculated from the data', allow_inf_nan = True)
    movingAvgQuality: float = Field(None, description='The rolling mean calculated from the data', allow_inf_nan = True)
    movingAvgMomentum: float = Field(None, description='The rolling mean calculated from the data', allow_inf_nan = True)
    movingAvgAbsValue: float = Field(None, description='The rolling mean calculated from the data', allow_inf_nan = True)
    movingAvgRelValue: float = Field(None, description='The rolling mean calculated from the data', allow_inf_nan = True)
    movingAvgProfitability: float = Field(None, description='The rolling mean calculated from the data', allow_inf_nan = True)
    movingAvgSafety: float = Field(None, description='The rolling mean calculated from the data', allow_inf_nan = True)
    movingAvgGrowth: float = Field(None, description='The rolling mean calculated from the data', allow_inf_nan = True)
    movingAvgEarningsStability: float = Field(None, description='The rolling mean calculated from the data', allow_inf_nan = True)
    movingAvgSentiment: float = Field(None, description='The rolling mean calculated from the data', allow_inf_nan = True)
    movingAvgPrice: float = Field(None, description='The rolling mean calculated from the data', allow_inf_nan = True)

class ServiceArk(BaseModel):
    week: str = Field(..., description='The date when data was calculated')
    SEDOL: Optional[str] = Field(..., description='The SEDOL representing the main listing')
    isin: Optional[str] = Field(..., description='The ISIN representing the main listing')
    companyName: str = Field(..., description='The name of the company')
    countryIso: str = Field(..., description='The country ISO related to the issue')
    regionName: str = Field(..., description='The region related to the issue')
    sectorName: str = Field(..., description='The sector of the company')
    GIC_GROUP_NM: str = Field(..., description='The industry group of the company')
    industryName: Optional[str] = Field(None, description='The company industry name (GICS)')
    marketCap: float = Field(..., description='The market capitalization of the company in USDm', gt=0)
    jyskeQuantQuint: str = Field(..., description='The Jyske Quant quintile of the company (Q1=Best,... Q5=Worst)')
    valueQuint: str = Field(..., description='The Value quintile of the company (Q1=Best,... Q5=Worst)')
    qualityQuint: str = Field(..., description='The Quality quintile of the company (Q1=Best,... Q5=Worst)')
    momentumQuint: str = Field(..., description='The Momentum quintile of the company (Q1=Best,... Q5=Worst)')
    jyskeQuantScore: float = Field(..., description='The Jyske Quant score of the company') 
    valueScore: float = Field(..., description='The Value score of the company')
    qualityScore: float = Field(..., description='The Quality score of the company') 
    momentumScore: float = Field(..., description='The Momentum score of the company') 
    absValueQuint: Optional[str] = Field(None, description='The quintile of Absolute value (Q1=Best,... Q5=Worst)') 
    relValueQuint: Optional[str] = Field(None, description='The quintile of Relative value (Q1=Best,... Q5=Worst)') 
    profitabilityQuint: Optional[str] = Field(None, description='The quintile of Profitability (Q1=Best,... Q5=Worst)') 
    safetyQuint: Optional[str] = Field(None, description='The quintile of Safety (Q1=Best,... Q5=Worst)') 
    growthQuint: Optional[str] = Field(None, description='The quintile of Growth (Q1=Best,... Q5=Worst)') 
    earningsStabilityQuint: Optional[str] = Field(None, description='The quintile of Earnings stability (Q1=Best,... Q5=Worst)') 
    sentimentQuint: Optional[str] = Field(None, description='The quintile of Sentiment (Q1=Best,... Q5=Worst)') 
    priceQuint: Optional[str] = Field(None, description='The quintile of Price (Q1=Best,... Q5=Worst)') 

class SizeFactor(BaseModel):
    Value: int = Field(..., description='The count of companies present in the chosen filtering')