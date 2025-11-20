"""
dlt sources for ArcGIS REST API endpoints

Replaces: get_gis_data(), make_esri_fs_url() from get_ca_data.py
"""

import dlt
from dlt.sources.helpers.rest_client.paginators import BasePaginator
from dlt.sources.rest_api import rest_api_source


class ArcGISPaginator(BasePaginator):
    """
    Custom paginator for ArcGIS REST API that uses exceededTransferLimit

    ArcGIS APIs don't return a total count, but instead use a boolean
    'exceededTransferLimit' to indicate if more pages are available.
    """

    def __init__(
        self, offset_param="resultOffset", limit_param="resultRecordCount", limit=2000
    ):
        super().__init__()
        self.offset_param = offset_param
        self.limit_param = limit_param
        self.limit = limit
        self.offset = 0
        self.page_count = 0

    def update_state(self, response, data=None):
        """Update pagination state based on ArcGIS response"""
        response_json = response.json()
        features = response_json.get("features", [])
        self.page_count += 1

        # Check if there are more records using ArcGIS's pagination indicator
        if not response_json.get("exceededTransferLimit", False):
            self._has_next_page = False
        else:
            self.offset += self.limit
            self._has_next_page = True

    def update_request(self, request):
        """Add pagination parameters to the request"""
        if request.params is None:
            request.params = {}
        request.params[self.offset_param] = self.offset
        request.params[self.limit_param] = self.limit


ARCGIS_BASE_URL = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/"


@dlt.source(name="arcgis_geographies")
def arcgis_geographies_source():
    """
    Extract geographical boundaries from ArcGIS REST API

    Replaces multiple get_gis_data() calls in cesap-epc-load-duckdb-data.py

    Returns:
        dlt source with multiple resources for different LSOA geographies
    """

    config = {
        "client": {
            "base_url": ARCGIS_BASE_URL,
            "paginator": ArcGISPaginator(limit=2000),
            "headers": {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
                "Referer": "https://geoportal.statistics.gov.uk/",
            },
        },
        "resources": [
            {
                "name": "lsoa_2021_boundaries",
                "endpoint": {
                    "path": "Lower_layer_Super_Output_Areas_December_2021_Boundaries_EW_BFC_V10/FeatureServer/0/query",
                    "params": {"where": "1=1", "outFields": "*", "f": "json"},
                    "data_selector": "features",
                },
                "write_disposition": "replace",
            },
            {
                "name": "lsoa_2011_boundaries",
                "endpoint": {
                    "path": "LSOA_Dec_2011_Boundaries_Generalised_Clipped_BGC_EW_V3/FeatureServer/0/query",
                    "params": {"where": "1=1", "outFields": "*", "f": "json"},
                    "data_selector": "features",
                },
                "write_disposition": "replace",
            },
            {
                "name": "lsoa_2021_pwc",
                "endpoint": {
                    "path": "LLSOA_Dec_2021_PWC_for_England_and_Wales_2022/FeatureServer/0/query",
                    "params": {"where": "1=1", "outFields": "*", "f": "json"},
                    "data_selector": "features",
                },
                "write_disposition": "replace",
            },
            {
                "name": "lsoa_2021_lookups",
                "endpoint": {
                    "path": "LSOA21_WD24_LAD24_EW_LU/FeatureServer/0/query",
                    "params": {"where": "1=1", "outFields": "*", "f": "json"},
                    "data_selector": "features",
                },
                "write_disposition": "replace",
            },
            {
                "name": "lsoa_2011_lookups",
                "endpoint": {
                    "path": "LSOA01_LSOA11_LAD11_EW_LU_ddfe1cd1c2784c9b991cded95bc915a9/FeatureServer/0/query",
                    "params": {"where": "1=1", "outFields": "*", "f": "json"},
                    "data_selector": "features",
                },
                "write_disposition": "replace",
            },
        ],
    }

    return rest_api_source(config)


@dlt.source(name="ca_boundaries")
def ca_boundaries_source():
    """
    Extract Combined Authority boundaries

    Replaces: get_ca_geojson() from get_ca_data.py

    Returns:
        dlt source with CA boundary data in GeoJSON format
    """

    config = {
        "client": {
            "base_url": ARCGIS_BASE_URL,
            "headers": {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
                "Referer": "https://geoportal.statistics.gov.uk/",
            },
        },
        "resources": [
            {
                "name": "ca_boundaries_2025",
                "endpoint": {
                    "path": "CAUTH_MAY_2025_EN_BGC/FeatureServer/0/query",
                    "params": {
                        "where": "1=1",
                        "outFields": "*",
                        "f": "geojson",
                    },
                },
                "write_disposition": "replace",
            }
        ],
    }

    return rest_api_source(config)
