import pandas as pd
from factor_ratios.functions import BlobConnect, UploadToBlob

def overrideIsin(iso_change, cap_factor):
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