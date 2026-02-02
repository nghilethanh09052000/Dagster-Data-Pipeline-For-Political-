from __future__ import annotations

from typing import TYPE_CHECKING

import requests
import truststore
from bs4 import BeautifulSoup
from bs4.element import Tag
from bs4.filter import SoupStrainer
from fake_useragent import UserAgent
from requests import Response

truststore.inject_into_ssl()

if TYPE_CHECKING:
    from _typeshed import FileDescriptorOrPath


def fetch_url_with_chrome_user_agent(url: str) -> Response:
    """
    Function to act as chrome web browser.

    Parameters
    ----------
        url: (string)
             base url to get virgnia campaign finance report

    Returns
    -------
        response: (requests.models.Response)
                  get request response
    """

    # Get default Chrome User Agent
    ua = UserAgent()
    chrome = ua.chrome

    # Get url response
    response = requests.get(
        url=url,
        headers={
            "User-Agent": str(chrome),
            "Upgrade-Insecure-Requests": "1",
            "Accept": "text/html,application/xhtml+xml,application/xml;"
            + "q=0.9,image/webp,image/apng,*/*;q=0.8,"
            + "application/signed-exchange;v=b3;q=0.9",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate",
        },
    )

    return response


def get_all_links_from_html(
    page_source: str, base_url="https://apps.elections.virginia.gov"
) -> list[str]:
    """
    Extract download links from url.

    Parameters
    ----------
        page_source: (string)
                     page source in html format

    Returns
    -------
        links: (list)
                list of found on the page source
    """

    links = []
    for link in BeautifulSoup(page_source, "html.parser", parse_only=SoupStrainer("a")):
        if isinstance(link, Tag) and link.has_attr("href"):
            href = link.get("href")
            if (
                href is not None
                and "".join(str(href).split("/")[-2].split("_")).isdigit()
            ):
                links.append(base_url + str(href))

    return links


def save_response_as_file(response: Response, destination_file_path: str) -> None:
    """
    Function to download csv report files.

    Parameters
    ----------
        response: (requests.models.Response)
                  response object from fetch_url_with_chrome_user_agent
        save_file: (string)
                   path where the file will be written
    """

    if response.status_code == 200:
        with open(destination_file_path, "wb") as file:
            file.write(response.content)

    else:
        raise RuntimeError(f"Could not write data from URL: {response.url}")


def stream_download_file_to_path(
    request_url: str,
    file_save_path: FileDescriptorOrPath,
    headers: dict[str, str] | None = None,
    verify: bool = False,
):
    """
    Requests a URL while streaming the result to a file

    Parameters
    ----------
        request_url: (str)
                     valid URL with protocols to a download-able file
        file_save_path: (FileDescriptorOrPath)
                        path where the file will be written
    """
    request_session = requests.Session()
    if not verify:
        request_session.verify = False
    request_session.trust_env = False
    with request_session.get(request_url, stream=True, headers=headers) as response:
        if not response.ok:
            raise RuntimeError(f"Could not get data for URL {request_url}")

        with open(file_save_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)


def download_file_from_http(
    request_url: str,
    file_save_path: FileDescriptorOrPath,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    request_body: dict | None = None,
    verify: bool | None = None,
):
    """
    Requests a URL using a specified HTTP method while streaming the result to a file.

    Parameters
    ----------
        request_url: (str)
            Valid URL with protocol to a downloadable file.
        file_save_path: (FileDescriptorOrPath)
            Path where the file will be written.
        method: (str)
            The HTTP method to use ('GET' or 'POST').
        headers: (dict, optional)
            Additional headers to include in the request.
        request_body: (dict, optional)
            Data to include in the request body (for POST method).
        verify: (bool, optional)
            Whether to verify the server's TLS certificate.
    """
    request_session = requests.Session()
    request_session.trust_env = False

    if method.upper() not in ["GET", "POST"]:
        raise ValueError("Unsupported HTTP method. Use 'GET' or 'POST'.")

    with request_session.request(
        method=method.upper(),
        url=request_url,
        stream=True,
        headers=headers,
        json=request_body,
        verify=verify,
    ) as response:
        if not response.ok:
            raise RuntimeError(
                f"Could not get data for URL {request_url}. "
                f"Status code: {response.status_code}"
            )

        with open(file_save_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
