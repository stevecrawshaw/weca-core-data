import logging
import math
import shutil
import zipfile
from datetime import datetime
from io import StringIO
from pathlib import Path
from urllib.parse import urlencode, urljoin, urlparse, urlunparse

import janitor.polars
import polars as pl
import requests
import yaml
from dateutil import parser
from dateutil.relativedelta import relativedelta
from requests.exceptions import (
    ConnectionError,
    HTTPError,
    RequestException,
    Timeout,
)

from epc_schema import (
    all_cols_polars,  # schema for the EPC certificates
    nondom_polars_schema,  # non domestic certificates
)

"""
Functions to get geographies and EPC data for combined authorities
These are used in the notebook cesap_epc_load_duckdb.ipynb
To populate a duckdb database which holds base data for analyses
supporting the CESAP indicators. Plotting and further analysis are done in
R scripts in this project folder.
"""
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),  # You can also add logging to a file
        logging.FileHandler("etl.log"),  # Log to a file named etl.log
    ],
)


def make_esri_fs_url(
    base_url: str, service_portion: str, tail_url: str
) -> str:
    """
    Construct a valid URL for an Esri FeatureServer API call.
    supports rational changing of url components when sources updated
    """
    joined_url = urljoin(base_url, f"{service_portion}{tail_url}")
    parsed_url = urlparse(joined_url)
    parsed_url_valid = all(
        [parsed_url.scheme, parsed_url.netloc, parsed_url.path]
    )
    if parsed_url_valid:
        return joined_url
    else:
        raise ValueError(f"Invalid URL: {parsed_url}")
        return None


def download_zip(
    url: str, directory: str = "data", filename: str = None
) -> str:
    """
    Downloads a zip file from the given URL
    and saves it to the specified directory with an optional custom filename.

    Args:
        url (str): The URL of the zip file to download.
        directory (str): The directory where the zip file will be saved.
        Defaults to "data".
        filename (str, optional):
        The name to save the zip file as.
        If not provided, the name is extracted from the URL.

    Returns:
        str: The full path to the downloaded file.
    """
    logging.info(f"Starting download from URL: {url}")
    # Create a Path object for the directory
    directory_path = Path(directory)

    # Ensure the directory exists, create it if it doesn't
    directory_path.mkdir(parents=True, exist_ok=True)

    # Use the provided filename or extract the filename from the URL
    if filename is None:
        filename = url.split("/")[-1]

    # Create the full file path
    file_path = directory_path / filename
    logging.info(f"Saving zip file to: {file_path}")
    # Stream the download for efficiency
    with requests.get(url, stream=True, timeout=10) as r:
        r.raise_for_status()  # Check if the request was successful
        with file_path.open("wb") as f:
            shutil.copyfileobj(r.raw, f)

    logging.info(f"Download completed: {file_path}")

    return str(file_path)


def extract_csv_from_zip(zip_file_path: str) -> str:
    """
    Extracts the CSV file from the immediate 'Data'
    folder inside the given zip file
    and saves it directly to the same folder where the zip file is located
    without retaining the 'Data/' folder structure.

    Args:
        zip_file_path (str): The path to the zip file.

    Returns:
        str: The path to the extracted CSV file.

    Raises:
        FileNotFoundError: If no CSV file is found in the 'Data'
        folder inside the zip.
        ValueError: If multiple or no CSV files
        are found in the immediate 'Data' folder.
    """
    logging.info(f"Extracting CSV from zip file: {zip_file_path}")

    zip_file = Path(zip_file_path)
    extract_path = zip_file.parent  # Extract to the same directory

    # Ensure the zip file exists
    if not zip_file.exists():
        logging.error(f"Zip file '{zip_file}' does not exist.")
        raise FileNotFoundError(f"Zip file '{zip_file}' does not exist.")

    # Open the zip file
    with zipfile.ZipFile(zip_file, "r") as z:
        # List all files in the zip archive
        all_files = z.namelist()

        # Filter for CSV files in the immediate 'Data/' folder (no subfolders)
        csv_files = [
            f
            for f in all_files
            if f.startswith("Data/")
            and f.count("/") == 1
            and f.endswith(".csv")
        ]

        # Ensure there's exactly one CSV file in the immediate 'Data' folder
        if len(csv_files) == 0:
            logging.error("No CSV file found in the immediate 'Data' folder.")
            raise FileNotFoundError(""""
                                    No CSV file found in the immediate 'Data'
                                     folder inside the zip.
                                    """)
        elif len(csv_files) > 1:
            logging.error("Multiple CSV files found in the 'Data' folder.")
            raise ValueError("""
                             Multiple CSV files found in the
                              immediate 'Data' folder.
                              Only one expected.
                             """)

        # Extract the CSV file without the 'Data/' folder structure
        csv_file = csv_files[0]
        csv_filename = Path(csv_file).name
        # Get only the file name, ignoring the folder
        extracted_csv_path = extract_path / csv_filename

        logging.info(f"Extracting CSV to: {extracted_csv_path}")

        # Extract the file, but rename it to remove the folder structure
        with (
            z.open(csv_file) as source,
            extracted_csv_path.open("wb") as target,
        ):
            shutil.copyfileobj(source, target)

        logging.info(f"Extraction completed: {extracted_csv_path}")

        return str(extracted_csv_path)


def delete_zip_file(zip_file_path: str):
    """
    Deletes the specified zip file if it exists.

    Args:
        zip_file_path (str): The path to the zip file that should be deleted.

    Raises:
        FileNotFoundError: If the zip file does not exist.
    """
    # Create a Path object for the zip file
    zip_file = Path(zip_file_path)

    # Check if the file exists
    if zip_file.exists() and zip_file.is_file():
        # Delete the file
        logging.info(f"Deleting zip file: {zip_file}")
        zip_file.unlink()
        print(f"Deleted zip file: {zip_file}")
        logging.info(f"Deleted zip file: {zip_file}")
    else:
        logging.error(f"Zip file '{zip_file}' not found.")
        raise FileNotFoundError(f"Zip file '{zip_file}' not found")


def load_config(config_path: str) -> dict:
    """
    Function to retrieve credentials from a config file.
    """
    try:
        with open(config_path) as config_file:
            return yaml.safe_load(config_file)
    except FileNotFoundError:
        logging.error(f"Config file not found: {config_path}")
        raise


def make_zipfile_list(
    ca_la_df: pl.DataFrame, epc_base_url: str, type: str = "domestic"
):
    """
    Generates a list of dictionaries containing URLs
    for zip files and their corresponding local authority codes.

    Args:
        ca_la_df (pl.DataFrame): A Polars DataFrame containing local authority
        data with columns 'ladnm' and 'ladcd'.
        type (str, optional): The type of data to be fetched. Defaults to
        "domestic" but also accepts "non-dom

    Returns:
        List[Dict[str, str]]: A list of dictionaries,
        each containing 'url' and 'ladcd' keys.
    """
    return (
        ca_la_df.with_columns(
            pl.col("ladnm").str.replace_all(", |\\. | ", "-").alias("la")
        ).select(
            [
                pl.concat_str(
                    pl.lit(
                        # "https://epc.opendatacommunities.org/api/v1/files/"
                        epc_base_url
                    ),
                    pl.lit(type),
                    pl.lit("-"),
                    pl.col("ladcd"),
                    pl.lit("-"),
                    pl.col("la"),
                    pl.lit(".zip"),
                ).alias("url"),
                pl.col("ladcd"),
            ]
        )
    ).to_dicts()


def dl_bulk_epc_zip(la_zipfile_list, path="data/epc_bulk_zips"):
    """
    Downloads bulk EPC (Energy Performance Certificate) zip files
    for a list of local authorities.

    This function reads a configuration file to get the
    EPC authentication token, then iterates over
    a list of local authority zip file information,
    downloading each zip file and saving it to the
    local filesystem.

    Args:
        la_zipfile_list (list): A list of dictionaries, each containing:
            - 'url' (str): The URL to download the zip file from.
            - 'ladcd' (str): The local authority district code
            used to name the downloaded zip file.

    Raises:
        requests.exceptions.RequestException:
        If there is an issue with the HTTP request.
        IOError: If there is an issue writing the file to the local filesystem.

    Example:
        la_zipfile_list = [
            {'url': 'http://example.com/file1.zip', 'ladcd': 'E07000026'},
            {'url': 'http://example.com/file2.zip', 'ladcd': 'E07000027'}
        ]
        dl_bulk_epc_zip(la_zipfile_list)
    """
    try:
        config = load_config("../config.yml")
        epc_key = config["epc"]["auth_token"]
    except Exception as e:
        logging.error(f"Error loading config: {e}")
        raise

    for la in la_zipfile_list:
        url = la["url"]
        headers = {"Authorization": f"Basic {epc_key}"}
        try:
            response = requests.get(
                url, headers=headers, allow_redirects=True, timeout=10
            )
            response.raise_for_status()
        except requests.RequestException as e:
            logging.error(
                f"Error downloading {la['ladcd']}.zip from {url}: {e}"
            )
            continue

        try:
            with open(f"{path}/{la['ladcd']}.zip", "wb") as file:
                file.write(response.content)
            logging.info(f"Downloaded {la['ladcd']}.zip")
        except OSError as e:
            logging.error(
                f"Error writing {la['ladcd']}.zip to filesystem: {e}"
            )


def extract_and_rename_csv_from_zips(
    zip_folder: str = "data/epc_bulk_zips",
) -> None:
    """
    Extract the file called certificates.csv
    from each zip file in the specified folder
    and rename the certificates.csv file to the
    name of the zip file (e.g., E06000001.zip
    will result in E06000001.csv).
    The CSV files will be saved in the same folder.

    Args:
        zip_folder (str): The folder containing the zip files.
        Defaults to "data/epc_bulk_zips".
    """
    zip_folder_path = Path(zip_folder)

    # Ensure the folder exists
    if not zip_folder_path.exists():
        logging.error(f"Zip folder '{zip_folder}' does not exist.")
        raise FileNotFoundError(f"Zip folder '{zip_folder}' does not exist.")

    # Iterate over all zip files in the folder
    for zip_file in zip_folder_path.glob("*.zip"):
        try:
            with zipfile.ZipFile(zip_file, "r") as z:
                # Check if certificates.csv exists in the zip file
                if "certificates.csv" in z.namelist():
                    # Extract certificates.csv and rename it
                    extracted_csv_path = (
                        zip_folder_path / f"{zip_file.stem}.csv"
                    )
                    with (
                        z.open("certificates.csv") as source,
                        extracted_csv_path.open("wb") as target,
                    ):
                        shutil.copyfileobj(source, target)
                    logging.info(
                        f"Extract renamed {zip_file} to {extracted_csv_path}"
                    )
                else:
                    logging.warning(f"No certificates.csv found in {zip_file}")
        except zipfile.BadZipFile:
            logging.error(f"Bad zip file: {zip_file}")
        except Exception as e:
            logging.error(f"Error processing {zip_file}: {e}")


def delete_all_csv_files(folder_path):
    # Convert the folder path to a Path object
    folder = Path(folder_path)

    # Get the list of all CSV files in the folder
    csv_files = folder.glob("*.csv")

    # Loop through the list and remove each file
    for file in csv_files:
        logging.info(f"File deleted: {file}")
        file.unlink()
        print(f"Deleted: {file}")


def delete_file(file_path):
    """
    Delete a file if it exists using pathlib.
    """
    path = Path(file_path)
    if path.exists():
        path.unlink()
        logging.info(f"File deleted: {file_path}")
        print(f"Deleted: {file_path}")
    else:
        logging.warning(f"File not found: {file_path}")
        print(f"File not found: {file_path}")


def remove_numbers(input_string: str) -> str:
    """
    For an input string remove all numbers and return as a string
    """
    # rename columns
    # Create a translation table that maps each digit to None
    lowercase_string = input_string.lower()
    translation_table = str.maketrans("", "", "0123456789")
    # Use the translation table to remove all numbers from the input string
    result_string = lowercase_string.translate(translation_table)
    return result_string


def get_ca_la_df(
    year: int,
    baseurl: str,
    inc_ns: bool = True,
    remove_numbers=remove_numbers,
) -> pl.DataFrame:
    """
    Download the lookup table for Combined and Local Authorities
    From ArcGIS ONS Geography portal
    Different versions for year - boundary changes
    Rename columns by removing numerals
    """
    c_year = (datetime.now().year) + 1
    start_year = c_year - 3
    years = list(range(start_year, c_year))

    try:
        if year not in years:
            raise ValueError(
                f"Year {year} is not within the valid range: {years}"
            )
        year_suffix = str(year)[2:4]
        lad_cauth_portion = f"LAD{year_suffix}_CAUTH{year_suffix}"
        url_suffix = "_EN_LU/FeatureServer/0/query"
        url = f"{baseurl}{lad_cauth_portion}{url_suffix}"
        params = {
            "where": "1=1",  # maps to True
            "outFields": "*",  # all
            "SR": "4326",  # WGS84
            "f": "json",
        }

        r = requests.get(url=url, params=params, timeout=10)
        if r.status_code != 200:
            logging.error(f"API call failed: {baseurl}: {r.status_code}")
            raise Exception(f"API call failed {r.status_code}")
    except AssertionError:
        print(f"API call failed {r.status_code}")

    else:
        response = r.json()
        attrs = response.get("features")
        rows = [attr.get("attributes") for attr in attrs]
        ca_la_df = pl.DataFrame(rows).select(pl.exclude("ObjectId"))

        old_names = ca_la_df.columns
        new_names = [remove_numbers(colstring) for colstring in old_names]
        rename_dict = dict(zip(old_names, new_names, strict=False))

        clean_ca_la_df = ca_la_df.rename(rename_dict)

        ns_line_df = pl.DataFrame(
            {
                "ladcd": "E06000024",
                "ladnm": "North Somerset",
                "cauthcd": "E47000009",
                "cauthnm": "West of England",
            }
        )

        if inc_ns:  # if North Somerset to be included add a line to the df
            logging.info("ca_la_df with north somerset created")
            return clean_ca_la_df.vstack(ns_line_df)

        else:
            logging.info("ca_la_df without north somerset created")
            return clean_ca_la_df


def get_chunk_list(
    base_url: str, params_base: dict, max_records: int = 2000
) -> list:
    """
    Get a list of offsets to query the ArcGIS API
    based on the record count limit
    Sometimes the limit is 1000, sometimes 2000
    Required due to pagination of API
    """
    params_rt = {"returnCountOnly": "true", "where": "1=1"}
    # combine base and return count parameters
    params = {**params_base, **params_rt}
    try:
        with requests.get(base_url, params=params, timeout=10) as r:
            r.raise_for_status()
            record_count = r.json().get("count")
            chunk_size = round(
                record_count / math.ceil(record_count / max_records)
            )
            chunk_list = list(range(0, record_count, chunk_size))
    except requests.RequestException as e:
        logging.error(f"API request error: from {base_url} {e}")
        raise requests.RequestException(
            f"Error fetching data from API: {e}"
        ) from e
    return chunk_list


def get_gis_data(
    offset: int, params_base: dict, params_other: dict, base_url: str
) -> pl.DataFrame:
    """
    Get a dataset containing geometry from the
    ArcGIS API based on the offset.

    Handles network errors, invalid data, and
    issues with the response structure.
    """
    try:
        # Attempt to make the request
        with requests.get(
            base_url,
            params={**params_base, **{"resultOffset": offset}, **params_other},
            stream=True,
            timeout=10,
        ) as r:
            # Raise an error if status code is not 200
            r.raise_for_status()

            # Attempt to parse the JSON response
            try:
                data = r.json()
            except ValueError as e:
                raise ValueError("Invalid JSON response") from e

            # Extract features from the response
            features = data.get("features")
            if not features:
                raise ValueError("No 'features' found in the response")

            # Try to convert features into a DataFrame
            try:
                features_df = (
                    pl.DataFrame(features)  # Convert to DataFrame
                    .unnest("attributes")  # Unnest the 'attributes' column
                    .drop("GlobalID")  # Drop unnecessary columns
                    .unnest("geometry")  # Unnest the 'geometry' column
                )
            except Exception as e:
                raise RuntimeError(
                    "Error processing data into DataFrame"
                ) from e

    except HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        return pl.DataFrame()  # Return an empty df on HTTP error
    except ConnectionError as conn_err:
        print(f"Connection error occurred: {conn_err}")
        return pl.DataFrame()  # Return an empty df on connection error
    except Timeout as timeout_err:
        print(f"Timeout error occurred: {timeout_err}")
        return pl.DataFrame()  # Return an empty df on timeout
    except RequestException as req_err:
        print(f"Request error occurred: {req_err}")
        return pl.DataFrame()  # Return an empty df on general request error
    except ValueError as json_err:
        print(f"JSON or data error occurred: {json_err}")
        return pl.DataFrame()  # Return an empty df on JSON/data error
    except RuntimeError as df_err:
        print(f"DataFrame processing error occurred: {df_err}")
        return pl.DataFrame()  # Return an empty df on DataFrame error
    except Exception as err:
        print(f"An unexpected error occurred: {err}")
        return pl.DataFrame()  # Catch-all for any other exceptions
    return features_df


def get_flat_data(
    offset: int, params_base: dict, params_other: dict, base_url: str
) -> pl.DataFrame:
    """
    Get the data from the ArcGIS API based on the offset
    This is for data without a geometry field
    """
    try:
        with requests.get(
            base_url,
            params={**params_base, **{"resultOffset": offset}, **params_other},
            stream=True,
            timeout=10,
        ) as r:
            r.raise_for_status()
            print(r.url)
            features = r.json().get("features")
            features_df = pl.DataFrame(features).unnest("attributes")
    except requests.RequestException as e:
        logging.error(f"API request error: from {base_url} {e}")
        raise requests.RequestException(
            f"Error fetching data from API: {e}"
        ) from e
    return features_df


def make_poly_url(
    base_url: str, params_base: dict, lsoa_code_list: list, lsoa_code_name: str
) -> str:
    """
    Make a url to retrieve lsoa polygons given a list of lsoa codes
    """
    lsoa_in_clause = str(tuple(list(lsoa_code_list)))
    where_params = {"where": f"{lsoa_code_name} IN {lsoa_in_clause}"}
    params = {**params_base, **where_params}
    # use urlencode to avoid making an actual call to get the urls
    query_string = urlencode(params)
    url = urlunparse(("", "", base_url, "", query_string, ""))
    return url


def make_lsoa_pwc_df(
    base_url: str,
    params_base: dict,
    params_other: dict,
    max_records: int = 2000,
) -> pl.DataFrame:
    """
    Make a polars DataFrame of the LSOA data from the ArcGIS API
    by calling the get_chunk_range and get_data functions
    concatenated and sorted by the FID
    """
    chunk_range = get_chunk_list(base_url, params_base, max_records)
    df_list = []
    for offset in chunk_range:
        df_list.append(
            get_gis_data(offset, params_base, params_other, base_url)
        )
    lsoa_df = pl.concat(df_list).unique()
    return lsoa_df.rename(lambda x: x.lower())


def get_rename_dict(
    df: pl.DataFrame, remove_numbers, rm_numbers=False
) -> dict:
    old = df.columns
    counts = {}
    if not rm_numbers:
        new = [colstring.lower() for colstring in df.columns]
    else:
        new = [remove_numbers(colstring).lower() for colstring in df.columns]

    for i, item in enumerate(new):
        if new.count(item) > 1:
            counts[item] = counts.get(item, 0) + 1
            new[i] = f"{item}_{counts[item]}"

    return dict(zip(old, new, strict=False))


def get_ca_la_codes(ca_la_df: pl.DataFrame) -> list:
    """
    Return a list of the LA codes which comprise each Combined Authority
    """
    return ca_la_df.select(pl.col("ladcd")).to_series().to_list()


def make_epc_update_pldf(
    la_list: list, from_date_dict: dict, type: str
) -> pl.DataFrame:
    """
    Loop through the list of local authorities and get the
    EPC data for each one.
    For the specified period, which is calculated in the get_epc_pldf function.
    The data is then concatenated into a single DataFrame and renamed
    to conform to the schema in the epc_schema.py file
    """
    epc_update_list_pldf = [
        get_epc_pldf(la, type, from_date_dict) for la in la_list
    ]
    epc_update_pldf = pl.concat(epc_update_list_pldf, how="vertical").rename(
        lambda x: x.upper().replace("-", "_")
    )
    return epc_update_pldf


def get_epc_from_date(type: str = "domestic") -> dict[str, int]:
    """
    Get the last date of the EPC data on the open data portal.

    Returns:
        dict: A dictionary containing the year and month of the last date.
              Format: {'year': int, 'month': int}

    Raises:
        requests.RequestException: If there's an error with the API request.
        ValueError: If there's an error parsing the response or the date.
    """
    base_url = "https://westofenglandca.opendatasoft.com/api/explore/v2.1"
    endpoint = "catalog/datasets"
    if type == "domestic":
        dataset = "lep-epc-domestic-point"
    elif type == "non-domestic":
        dataset = "non-domestic-epc"
    else:
        raise ValueError(f"Invalid EPC type: {type}")

    call_type = "records"
    query_param = {
        "select": "max(date) as max_date",
        "limit": 1,
        "offset": 0,
        "timezone": "UTC",
        "include_links": "false",
        "include_app_metas": "false",
    }

    url = f"{base_url}/{endpoint}/{dataset}/{call_type}"

    try:
        with requests.Session() as session:
            response = session.get(url, params=query_param)
            response.raise_for_status()
            data = response.json()

        max_date = data.get("results", [{}])[0].get("max_date")
        if not max_date:
            raise ValueError("No max_date found in the API response")

        parsed_date = parser.parse(max_date) + relativedelta(months=1)
        return {"year": parsed_date.year, "month": parsed_date.month}

    except requests.RequestException as e:
        raise requests.RequestException(
            f"Error fetching data from API: {e}"
        ) from e
    except (ValueError, KeyError, IndexError) as e:
        raise ValueError(f"Error parsing API response: {e}") from e


def get_epc_pldf(
    la: str,
    type: str,
    from_date: dict[str, int],
    to_date: dict[str, int] | None = None,
):
    """
    Uses the opendatacommunities API to get EPC data for a given
    local authority and time period. It creates a polars dataframe.
    This is intended for updates to the database not bulk downloads.
    Hence it doesn't write to file, but uses polars dataframe to store results.

    Args:
        la (str): Local authority code.
        type (str): Type of EPC data to retrieve. Either 'domestic' or 'non-domestic'.
        from_date (Dict[str, int]): Start date with 'year' and 'month' keys.
        to_date (Optional[Dict[str, int]]): End date with 'year' and 'month'
        keys. If None, uses current date.

    Returns:
        polars.DataFrame:
        Combined DataFrame of all EPC data for the specified period.

    Raises:
        requests.RequestException: If there's an error with the API request.
    """

    # Load the EPC auth token from the config file
    config = load_config("../config.yml")
    epc_key = config["epc"]["auth_token"]

    # Deal with data parameters
    from_year = from_date.get("year")
    from_month = from_date.get("month")
    if to_date is None:
        to_month = datetime.now().month - 1 or 12
        to_year = [
            datetime.now().year if to_month != 12 else datetime.now().year - 1
        ]
    else:
        to_year = to_date.get("year")
        to_month = to_date.get("month")

    # Page size (max 5000)
    query_size = 5000

    # Base url and example query parameters
    if type == "domestic":
        base_url = "https://epc.opendatacommunities.org/api/v1/domestic/search"
        sch = all_cols_polars
    elif type == "non-domestic":
        base_url = (
            "https://epc.opendatacommunities.org/api/v1/non-domestic/search"
        )
        sch = nondom_polars_schema
    else:
        raise ValueError(f"Invalid EPC type: {type}")

    query_params = {
        "size": query_size,
        "local-authority": la,
        "from-month": from_month,
        "from-year": from_year,
        "to-month": to_month,
        "to-year": to_year,
    }

    # Set up authentication
    headers = {"Accept": "text/csv", "Authorization": f"Basic {epc_key}"}

    try:
        first_request = True
        search_after = None
        all_data = []

        while search_after is not None or first_request:
            if not first_request:
                query_params["search-after"] = search_after

            response = requests.get(
                base_url, headers=headers, params=query_params, timeout=10
            )
            response.raise_for_status()

            body = response.text

            # Check if body is empty or only contains header
            if not body or body.count("\n") <= 1:
                break

            search_after = response.headers.get("X-Next-Search-After")

            if not first_request:
                body = body.split("\n", 1)[1]
                # Skip header for subsequent requests

            # Convert the CSV string to a Polars DataFrame
            df = pl.read_csv(StringIO(body), schema_overrides=sch)

            if not df.is_empty():
                all_data.append(df)
                logging.info(f"Retrieved {df.shape[0]} rows for {la}")

            first_request = False

        if not all_data:
            logging.warning(f"No data found for {la}")
            return pl.DataFrame()

        # Combine all DataFrames
        final_df = pl.concat(all_data)
        logging.info(f"""
                     Created final DataFrame with
                      {final_df.shape[0]} rows for {la}
                    """)

        return final_df

    except requests.RequestException as e:
        logging.error(f"API request error: {e}")
        raise


def get_ca_la_dft_lookup(dft_csv_path: str, la_list: list) -> pl.DataFrame:
    """
    Read the DFT annual traffic data, get the most recent year's data
    and return just the ONS la codes (ladcd)
    And the corresponding DFT ID which will be used in the R script
    to retrieve detailed link data. Year retained for context.
    Filter for LA's in CA's (la_list).
    Not all LA's in the CA's are within this set.
    """
    ca_la_dft_lookup_df = (
        pl.read_csv(dft_csv_path)
        .filter(pl.col("year") == pl.col("year").max())
        .select(
            [
                pl.col("local_authority_id").alias("dft_la_id"),
                pl.col("local_authority_code").alias("ladcd"),
                pl.col("year"),
            ]
        )
        .filter(pl.col("ladcd").is_in(la_list))
    )

    return ca_la_dft_lookup_df


def get_nomis_data(
    base_url: str, dataset_params: dict, creds_params: dict
) -> pl.DataFrame:
    """
    Get data from NOMIS API
    Args:
        base_url: str: the base url for the API
        dataset_params: dict: parameters for the dataset
        - e.g. table, date, geography
        creds_params: dict: credentials for the API from the config file

    Returns:
        A polars dataframe, long format for Bronze layer.
        Needs pivoting for tenures as cols
    """
    params = {**dataset_params, **creds_params}
    url = base_url + "?" + urlencode(params)
    return pl.read_csv(url)


def create_directories(directories):
    for directory in directories:
        dir_path = Path(directory)
        try:
            if not dir_path.exists():
                dir_path.mkdir(parents=True, exist_ok=True)
                logging.info(f"Directory created: {dir_path}")
            else:
                logging.info(f"Directory already exists: {dir_path}")
        except Exception as e:
            logging.error(f"Error creating directory {dir_path}: {e}")


def clean_colname(colnm: str) -> str:
    """
    Cleans the column names of the pivoted LSOA data columns
    For use in a pl.DataFrame.rename() method
    After a pyjanitor.clean_names() method
    """
    if colnm[0:4] != "lsoa":
        return (
            colnm.replace('"', "")
            .replace("{", "")
            .replace("}", "")
            .replace(",", "")[2:]
        )
    else:
        return colnm


def read_process_imd(imd_data_path: str) -> pl.DataFrame:
    """
    Reads in the raw IMD data
    and processes it into a clean table
    with all IMD features as columns
    and LSOA11CD as the index
    """
    try:
        imd_raw = pl.read_csv(imd_data_path, ignore_errors=True)
    except Exception as e:
        print(f"Error reading in the data: {e} from {imd_data_path}")
        return None
    try:
        imd_clean_wide_tbl = (
            (
                imd_raw.rename(lambda x: x.replace(" ", "_").lower())
                .rename({"featurecode": "lsoa11cd"})
                .drop(pl.col(["datecode", "units"]))
                .pivot(
                    on=["indices_of_deprivation", "measurement"],
                    index=["lsoa11cd"],
                    values="value",
                    separator="_",
                )
            )
            .clean_names(strip_underscores=True)
            .rename(clean_colname)
            .clean_names(strip_underscores=True)
        )
    except Exception as e:
        print(f"Error processing the data: {e}")
        return None
    return imd_clean_wide_tbl


def validate_urls(url_dict: dict[str, str]) -> int | dict[str, str]:
    """
    Validates a dictionary of URLs by checking if each URL returns a successful HTTP status code (200-299).

    Args:
        url_dict: A dictionary where keys are descriptions (strings) and values are URLs (strings).

    Returns:
        An integer (0) if all URLs are valid.
        Otherwise, a dictionary where keys are descriptions and values are invalid URLs.
        Returns -1 if there is a general network error.
    """

    invalid_urls = {}
    for description, url in url_dict.items():
        try:
            response = requests.get(
                url, timeout=5
            )  # Added timeout to prevent indefinite hanging
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        except RequestException as e:
            invalid_urls[description] = url
            print(
                f"Error validating {description} ({url}): {e}"
            )  # helpful print statement
        except Exception as e:
            print(f"General Network Error {description} ({url}): {e}")
            return -1  # indicate a general network error

    if invalid_urls:
        return invalid_urls
    else:
        return 0


def get_ca_geojson(
    url: str, output_path: str = "data/ca_boundaries.geojson"
) -> None:
    """
    Downloads a GeoJSON file from the specified URL and saves it to the given path.

    Args:
        url (str): URL of the GeoJSON file to download
        output_path (str): Path where the GeoJSON file will be saved

    Returns:
        None

    Raises:
        RequestException: If there is an error downloading the file
        OSError: If there is an error saving the file
    """
    logging.info(f"Attempting to download GeoJSON from {url}")

    try:
        # Create output directory if it doesn't exist
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        # Download the file
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        # Save to file
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(response.text)

        logging.info(f"Successfully saved GeoJSON to {output_path}")

    except requests.RequestException as e:
        logging.error(f"Error downloading GeoJSON: {e}")
        raise
    except OSError as e:
        logging.error(f"Error saving GeoJSON file: {e}")
        raise
