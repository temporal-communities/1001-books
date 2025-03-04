from __future__ import (
    annotations,
)  # Handle forward reference in typehint for __enter__ method
import logging
import time

from limits import strategies, storage, parse
import requests
from requests.adapters import HTTPAdapter
from types import TracebackType  # for type hints
from typing import Callable, Concatenate  # for type hints
from urllib3.util import Retry

logger = logging.getLogger(__name__)


# Decorator for rate limiting
def rate_limited[Self, **P, T](func: Callable[Concatenate[Self, P], T],) -> Callable[Concatenate[Self, P], T]:  # pyright: ignore
    """
    Decorator to enforce rate limiting on instance methods.

    This decorator ensures that calls to the decorated method comply with
    the rate limit set in the instance. If the limit is exceeded, it pauses
    execution until the rate limit resets before retrying the request.

    The decorated function must be an instance method of a class that has:
    - A `_limiter` attribute (an instance of `MovingWindowRateLimiter`).
    - A `_limit` attribute (a rate limit object, see https://limits.readthedocs.io/en/stable/api.html#limits.RateLimitItem).
    - A `_key` attribute (a string identifying the rate limit scope).
    """

    def wrapper(self, *args: P.args, **kwargs: P.kwargs) -> T:

        if not self._limiter.hit(self._limit, self._key):  # Wait until rate limit resets
            reset_time = self._limiter.get_window_stats(
                self._limit, self._key
            ).reset_time
            wait_time = reset_time - time.time()
            logger.debug(f"Wait time is type {type(wait_time)}")
            logger.info(f"Rate limit exceeded. Waiting for {wait_time:.2f} seconds before retrying...")
            time.sleep(wait_time)

        return func(self, *args, **kwargs)

    return wrapper


# Classes
class HttpClient:
    """
    HttpClient handles HTTP requests with rate limiting and automatic retries.
    Can be used as a context manager.
    """

    def __init__(self, rate_limit: str) -> None:
        self.session = self._setup_session()

        # Rate limiter settings
        self._store = storage.MemoryStorage()
        self._limiter = strategies.MovingWindowRateLimiter(self._store)
        self._limit = parse(rate_limit)  # returns RateLimitItemPerSecond or RateLimitItemPerHour
        self._key = "global"

    @staticmethod
    def _setup_session() -> requests.Session:
        """Set up an HTTP session with retry logic and default headers."""
        session = requests.Session()
        retries = Retry(
            backoff_factor=0.1,
            total=5,
            connect=2,  # Retries failed connection attempts (ConnectionError)
            read=2,  # Retries on read timeouts (ReadTimeout)
            status_forcelist=[500, 502, 503, 504],  # Retries on HTTP status codes (HTTPError)
        )
        session.mount("https://", HTTPAdapter(max_retries=retries))
        user_agent = f"1001 books (https://github.com/temporal-communities/1001-books) requests/{requests.__version__}"
        session.headers = {"User-Agent": user_agent, "Accept": "*/*"}
        return session

    @rate_limited
    def fetch_page(self, url: str, timeout: int = 10) -> requests.Response | None:
        """Make HTTP request for a url."""

        try:
            response = self.session.get(url, timeout=timeout)
            response.raise_for_status()  # Handle HTTP 4xx and 5xx errors after unsuccessful retries
            logger.info(f"Fetched {url} with status code: {response.status_code}")
            return response
        except requests.exceptions.RetryError as e:
            logger.warning(f"Max retries exceeded for {url}.")
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request for {url} failed with exception: {e}")
        return None

    def close(self) -> None:
        """Close the HTTP session."""
        self.session.close()

    def __enter__(self) -> HttpClient:
        """Enable the HTTP client to be used as a context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the HTTP session when exiting the context manager."""
        self.close()


# TESTS
if __name__ == "__main__":

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler("client_test.log"), logging.StreamHandler()],
    )

    with HttpClient("1/3seconds") as client:
        # Test rate limiting
        for i in range(1, 6):
            client.fetch_page(
                "https://lobid.org/gnd/search?q='aesops fables'&type=Titel&from=0&size=10&format=json"
            )
        # Test retry mechanism
        client.fetch_page(
            "https://lobid.org/gnd/search?q='[aesops fables]'&type=Titel&from=0&size=10&format=json"
        )  # malformed url

    with HttpClient("1/hour") as client:
        # Test rate limiting
        for i in range(1, 6):
            client.fetch_page(
                "https://lobid.org/gnd/search?q='aesops fables'&type=Titel&from=0&size=10&format=json"
            )
