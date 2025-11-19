# %%
import requests
from requests.exceptions import RequestException

# %%


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


# %%
if __name__ == "__main__":
    # Example Usage
    urls_to_validate = {
        "Google": "https://www.google.com",
        "Broken Link": "https://www.google.com/broken_link",
        "Python Website": "https://www.python.org",
    }

    result = validate_urls(urls_to_validate)

    if result == 0:
        print("All URLs are valid.")
    elif result == -1:
        print("General network error")
    else:
        print("Invalid URLs:")
        for description, url in result.items():
            print(f"- {description}: {url}")

    # Example 2 - error handling

    urls_to_validate = {
        "Good link": "https://www.google.com",
        "Timeout": "https://www.google.com:81",  # common way to create timeout errors
        "Python Website": "https://www.python.org",
    }

    result = validate_urls(urls_to_validate)

    if result == 0:
        print("All URLs are valid.")
    elif result == -1:
        print("General network error")
    else:
        print("Invalid URLs:")
        for description, url in result.items():
            print(f"- {description}: {url}")

# %%
