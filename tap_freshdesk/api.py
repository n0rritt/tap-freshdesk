import logging
import time

import backoff
from ratelimit import sleep_and_retry, limits
import requests

from tap_freshdesk import const


LOGGER = logging.getLogger()


class FreshdeskClient(object):

    def __init__(self, config_dict):
        self.session = requests.Session()
        self.api_key = config_dict.get('api_key')
        self.domain = config_dict.get('domain')
        self.start_date = config_dict.get('start_date')
        self.user_agent = config_dict.get('user_agent', const.USER_AGENT)
        self.rate_limit_requests = config_dict.get('rate_limit_requests', const.RATE_LIMIT_REQUESTS)
        self.rate_limit_seconds = config_dict.get('rate_limit_seconds', const.RATE_LIMIT_SECONDS)
        self.per_page = config_dict.get('per_page', const.PER_PAGE)
        self.max_retries = config_dict.get('max_retries', const.MAX_RETRIES)
        self.backoff_factor = config_dict.get('backoff_factor', const.BACKOFF_FACTOR)

        # usually we would use Python decorators on the request method, but since we want to change the arguments
        # for the decorators dynamically during runtime based on the provided config we have to override the
        # request method here
        self.request = limits(calls=self.rate_limit_requests, period=self.rate_limit_seconds)(self.request)
        self.request = sleep_and_retry(self.request)
        self.request = backoff.on_exception(
            backoff.expo,
            requests.exceptions.RequestException,
            max_tries=self.max_retries,
            giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500,
            factor=self.backoff_factor)(self.request)

    def request(self, url, params=None):
        params = params or {}
        headers = {'User-Agent': self.user_agent}

        req = requests.Request('GET', url, params=params, auth=(self.api_key, ""), headers=headers).prepare()
        LOGGER.info("GET {}".format(req.url))
        resp = self.session.send(req)

        if 'Retry-After' in resp.headers:
            retry_after = int(resp.headers['Retry-After'])
            LOGGER.info("Rate limit reached. Sleeping for {} seconds".format(retry_after))
            time.sleep(retry_after)
            return self.request(url, params)

        resp.raise_for_status()
        return resp
