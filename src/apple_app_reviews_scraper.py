import random
import requests
import re
import time
import os

def record_error(message: str) -> None:
    """Append a single-line error record to a shared log file if configured.

    The file path is taken from env var ERROR_LOG_FILE. Used by orchestrator run.sh
    to aggregate and report total errors after all downloads are complete.
    """
    path = os.environ.get('ERROR_LOG_FILE')
    if not path:
        return
    try:
        with open(path, 'a') as f:
            f.write(f"{int(time.time())} {message}\n")
    except Exception:
        pass

def get_token(country:str , app_name:str , app_id: str, user_agents: dict):

    """
    Retrieves the bearer token required for API requests
    Regex adapted from base.py of https://github.com/cowboy-bebug/app-store-scraper
    """

    response = requests.get(f'https://apps.apple.com/{country}/app/{app_name}/id{app_id}', 
                            headers = {'User-Agent': random.choice(user_agents)},
                            )
    
    if response.status_code != 200:
        print(f"GET request failed. Response: {response.status_code} {response.reason}")
        # treat non-200 on token fetch as error
        record_error(f"token {response.status_code} {response.reason}")

    tags = response.text.splitlines()
    for tag in tags:
        if re.match(r"<meta.+web-experience-app/config/environment", tag):
            token = re.search(r"token%22%3A%22(.+?)%22", tag).group(1)
    
    return token
    
def fetch_reviews(country:str , app_name:str , app_id: str, user_agents: dict, token: str, offset: str = '1'):

    """
    Fetches reviews for a given app from the Apple App Store API.

    - Default sleep after each call to reduce risk of rate limiting
    - Retry with increasing backoff if rate-limited (429)
    - No known ability to sort by date, but the higher the offset, the older the reviews tend to be
    """

    ## Define request headers and params ------------------------------------
    landingUrl = f'https://apps.apple.com/{country}/app/{app_name}/id{app_id}'
    requestUrl = f'https://amp-api-edge.apps.apple.com/v1/catalog/{country}/apps/{app_id}/reviews'

    headers = {
        'Accept': 'application/json',
        'Authorization': f'bearer {token}',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Origin': 'https://apps.apple.com',
        'Referer': landingUrl,
        'User-Agent': random.choice(user_agents)
        }

    offset_str = str(offset)
    params = (
        ('l', 'en-GB'),                 # language
        ('offset', offset_str),        # paginate this offset
        ('limit', '20'),                # max valid is 20
        ('platform', 'web'),
        ('additionalPlatforms', 'appletv,ipad,iphone,mac')
        )

    ## Perform request & exception handling ----------------------------------
    retry_count = 0
    MAX_RETRIES = 5
    BASE_DELAY_SECS = 10
    # Assign dummy variables in case of GET failure
    result = {'data': [], 'next': None}
    reviews = []

    while retry_count < MAX_RETRIES:

        # Perform request
        print(f"Вызван запрос: country={country} app_id={app_id} offset={offset_str}")
        response = requests.get(requestUrl, headers=headers, params=params)

        # SUCCESS
        # Parse response as JSON and exit loop if request was successful
        if response.status_code == 200:
            result = response.json()
            reviews = result['data']
            break

        # FAILURE
        elif response.status_code != 200:

            # RATE LIMITED
            if response.status_code == 429:
                # Count rate limit as an error occurrence for summary
                record_error("429 rate_limited")
                # Perform backoff using retry_count as the backoff factor
                retry_count += 1
                backoff_time = BASE_DELAY_SECS * retry_count
                # тихая пауза без логов
                time.sleep(backoff_time)
                continue

            # NOT FOUND
            elif response.status_code == 404:
                print(f"{response.status_code} {response.reason}. There are no more reviews.")
                break

            else:
                # Count all other non-200 (except 404) as errors
                record_error(f"{response.status_code} {response.reason}")

    ## Final output ---------------------------------------------------------
    # Get pagination offset for next request
    if 'next' in result and result['next'] is not None:
        offset = re.search("^.+offset=([0-9]+).*$", result['next']).group(1)
    else:
        offset = None

    # Append offset, number of reviews in batch, and app_id
    for rev in reviews:
        rev['offset'] = offset
        rev['n_batch'] = len(reviews)
        rev['app_id'] = app_id

    # Default sleep to decrease rate of calls
    time.sleep(0.5)
    return reviews, offset, response.status_code 