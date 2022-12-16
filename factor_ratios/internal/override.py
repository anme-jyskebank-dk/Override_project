import pandas as pd
from factor_ratios.functions import BlobConnect, UploadToBlob

def overrideIsin(iso_change, cap_factor):
    """
    Saves the newest combination of papers where a new country is desired and papers where the market cap is desired to be multiplied by a factor to the research-overrides container in azure. 

    Args:
        iso_change: A dictionary with isin's as keys and countryIso of the new desired country as values,
        the region will automatically change when the override is applied so is therefor not needed.
        cap_factor: A dictionary with isin's as keys and a float to mulitply by as values. 

    Returns:
        None: It saves a dataframe to azure containing the override information.
    """ 
    keyvault = "kv-dad-d"
    blob_service = BlobConnect(keyvault)
    blob_service_client_research_overrides = blob_service.get_container_client(container='research-overrides')

    try:
        dicts = {"iso_change" : iso_change, "cap_factors" : cap_factor}
        data =  pd.DataFrame.from_dict(dicts)

        UploadToBlob(blob_service_client_research_overrides, data, "override")

        return "Filen er opdateret"
    except:
        return "Der skete en fejl og ændringerne blev ikke gemt"