"""
dlt sources for EPC (Energy Performance Certificates) API

Replaces: get_epc_pldf() from get_ca_data.py

The EPC API uses a custom pagination mechanism with an X-Next-Search-After header.
"""

import dlt
from dlt.sources.rest_api import rest_api_source
from dlt.sources.helpers.rest_client.paginators import BasePaginator
from datetime import datetime
from typing import Any


class EPCPaginator(BasePaginator):
    """
    Custom paginator for EPC API that uses X-Next-Search-After header

    The EPC API returns a special header 'X-Next-Search-After' that contains
    a token for the next page. When this header is absent, there are no more pages.
    """

    def __init__(self, search_after_param="search-after"):
        super().__init__()
        self.search_after_param = search_after_param
        self.search_after = None
        self.page_count = 0

    def update_state(self, response, data=None):
        """Update pagination state based on EPC response headers"""
        self.page_count += 1

        # Check for the next page token in response headers
        self.search_after = response.headers.get("X-Next-Search-After")

        if self.search_after is None:
            self._has_next_page = False
        else:
            self._has_next_page = True

    def update_request(self, request):
        """Add search-after parameter to the request if available"""
        if self.search_after is not None:
            if request.params is None:
                request.params = {}
            request.params[self.search_after_param] = self.search_after


@dlt.source(name="epc_certificates")
def epc_certificates_source(
    certificate_type: str = "domestic",
    local_authority: str | None = None,
    from_month: int | None = None,
    from_year: int | None = None,
    to_month: int | None = None,
    to_year: int | None = None,
):
    """
    Extract EPC certificates from opendatacommunities.org API

    Args:
        certificate_type: 'domestic' or 'non-domestic'
        local_authority: Local authority code (optional, filters results)
        from_month: Start month (1-12, required for filtering by date)
        from_year: Start year (required for filtering by date)
        to_month: End month (1-12, defaults to current month-1)
        to_year: End year (defaults to current year)

    Replaces: get_epc_pldf() from get_ca_data.py

    Returns:
        dlt source with EPC certificate data
    """

    # Use dlt.secrets to get API key (stored in .dlt/secrets.toml)
    api_key = dlt.secrets.get("sources.epc.api_key")

    # Determine endpoint based on certificate type
    if certificate_type == "domestic":
        endpoint_path = "domestic/search"
    elif certificate_type == "non-domestic":
        endpoint_path = "non-domestic/search"
    else:
        raise ValueError(
            f"Invalid certificate_type: {certificate_type}. "
            "Must be 'domestic' or 'non-domestic'"
        )

    # Build query parameters
    params = {
        "size": 5000,  # Max page size for EPC API
    }

    # Add optional filters
    if local_authority:
        params["local-authority"] = local_authority

    if from_month and from_year:
        params["from-month"] = from_month
        params["from-year"] = from_year

        # Handle to_month/to_year defaults
        if to_month is None or to_year is None:
            current_month = datetime.now().month
            current_year = datetime.now().year
            # Default to previous month
            to_month = current_month - 1 if current_month > 1 else 12
            to_year = current_year if current_month > 1 else current_year - 1

        params["to-month"] = to_month
        params["to-year"] = to_year

    config = {
        "client": {
            "base_url": "https://epc.opendatacommunities.org/api/v1/",
            "auth": {
                "type": "api_key",
                "name": "Authorization",
                "api_key": f"Basic {api_key}",
                "location": "header",
            },
            "headers": {
                "Accept": "text/csv",
            },
            "paginator": EPCPaginator(),
        },
        "resources": [
            {
                "name": f"epc_{certificate_type.replace('-', '_')}",
                "endpoint": {
                    "path": endpoint_path,
                    "params": params,
                    "data_selector": "$",  # CSV data doesn't have a selector
                },
                "write_disposition": "replace",
            }
        ],
    }

    return rest_api_source(config)
