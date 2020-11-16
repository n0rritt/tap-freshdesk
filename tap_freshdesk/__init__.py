#!/usr/bin/env python3

import sys

from requests.exceptions import HTTPError
import singer

from tap_freshdesk import api
from tap_freshdesk import const
from tap_freshdesk import utils


REQUIRED_CONFIG_KEYS = ['api_key', 'domain', 'start_date']
CONFIG = {}
STATE = {}

endpoints = {
    "tickets": "/api/v2/tickets",
    "sub_ticket": "/api/v2/tickets/{id}/{entity}",
    "agents": "/api/v2/agents",
    "roles": "/api/v2/roles",
    "groups": "/api/v2/groups",
    "companies": "/api/v2/companies",
    "contacts": "/api/v2/contacts",
}

logger = singer.get_logger()


def get_url(endpoint, **kwargs):
    base_url = "https://{}.freshdesk.com"
    return base_url.format(CONFIG['domain']) + endpoints[endpoint].format(**kwargs)


def get_start(entity):
    if entity not in STATE:
        STATE[entity] = CONFIG['start_date']

    return STATE[entity]


def gen_request(client, url, params=None):
    params = params or {}
    params["per_page"] = const.PER_PAGE
    page = 1
    while True:
        params['page'] = page
        data = client.request(url, params).json()
        for row in data:
            yield row

        if len(data) == const.PER_PAGE:
            page += 1
        else:
            break


def transform_dict(d, key_key="name", value_key="value", force_str=False):
    # Custom fields are expected to be strings, but sometimes the API sends
    # booleans. We cast those to strings to match the schema.
    rtn = []
    for k, v in d.items():
        if force_str:
            v = str(v).lower()
        rtn.append({key_key: k, value_key: v})
    return rtn


def sync_tickets(client, fetch_ticket_status=None, fetch_sub_entities=None):
    bookmark_property = 'updated_at'

    singer.write_schema("tickets",
                        utils.load_schema("tickets"),
                        ["id"],
                        bookmark_properties=[bookmark_property])
    singer.write_schema("conversations",
                        utils.load_schema("conversations"),
                        ["id"],
                        bookmark_properties=[bookmark_property])
    singer.write_schema("satisfaction_ratings",
                        utils.load_schema("satisfaction_ratings"),
                        ["id"],
                        bookmark_properties=[bookmark_property])
    singer.write_schema("time_entries",
                        utils.load_schema("time_entries"),
                        ["id"],
                        bookmark_properties=[bookmark_property])

    if 'all' in fetch_ticket_status:
        sync_tickets_by_filter(client, bookmark_property, fetch_sub_entities=fetch_sub_entities)
    if 'all' in fetch_ticket_status or 'deleted' in fetch_ticket_status:
        sync_tickets_by_filter(client, bookmark_property, predefined_filter="deleted", fetch_sub_entities=fetch_sub_entities)
    if 'all' in fetch_ticket_status or 'spam' in fetch_ticket_status:
        sync_tickets_by_filter(client, bookmark_property, predefined_filter="spam", fetch_sub_entities=fetch_sub_entities)


def sync_tickets_by_filter(client, bookmark_property, predefined_filter=None, fetch_sub_entities=None):
    endpoint = "tickets"

    state_entity = endpoint
    if predefined_filter:
        state_entity = state_entity + "_" + predefined_filter

    start = get_start(state_entity)

    params = {
        'updated_since': start,
        'order_by': bookmark_property,
        'order_type': "asc",
        'include': "company,requester,stats"
    }

    if predefined_filter:
        logger.info("Syncing tickets with filter {}".format(predefined_filter))

    if predefined_filter:
        params['filter'] = predefined_filter

    tickets_schema = utils.load_schema('tickets')
    conversations_schema = utils.load_schema('conversations')
    ratings_schema = utils.load_schema('satisfaction_ratings')
    time_entries_schema = utils.load_schema('time_entries')

    for i, row in enumerate(gen_request(client, get_url(endpoint), params)):
        logger.info("Ticket {}: Syncing".format(row['id']))
        row.pop('attachments', None)
        row['custom_fields'] = transform_dict(row['custom_fields'], force_str=True)

        # get all sub-entities and save them

        if 'conversations' in fetch_sub_entities:
            try:
                logger.info("Ticket {}: Syncing conversations".format(row['id']))
                for subrow in gen_request(client, get_url("sub_ticket", id=row['id'], entity="conversations")):
                    subrow.pop("attachments", None)
                    subrow.pop("body", None)
                    if subrow[bookmark_property] >= start:
                        subrow = utils.reorder_fields_by_schema(subrow, conversations_schema)
                        singer.write_record("conversations", subrow, time_extracted=singer.utils.now())
            except HTTPError as e:
                if e.response.status_code == 403:
                    logger.info('Invalid ticket ID requested from Freshdesk {0}'.format(row['id']))
                else:
                    raise

        if 'satisfaction_ratings' in fetch_sub_entities:
            try:
                logger.info("Ticket {}: Syncing satisfaction ratings".format(row['id']))
                for subrow in gen_request(client, get_url("sub_ticket", id=row['id'], entity="satisfaction_ratings")):
                    subrow['ratings'] = transform_dict(subrow['ratings'], key_key="question")
                    if subrow[bookmark_property] >= start:
                        subrow = utils.reorder_fields_by_schema(subrow, ratings_schema)
                        singer.write_record("satisfaction_ratings", subrow, time_extracted=singer.utils.now())
            except HTTPError as e:
                if e.response.status_code == 403:
                    logger.info("The Surveys feature is unavailable. Skipping the satisfaction_ratings stream.")
                else:
                    raise

        if 'time_entries' in fetch_sub_entities:
            try:
                logger.info("Ticket {}: Syncing time entries".format(row['id']))
                for subrow in gen_request(client, get_url("sub_ticket", id=row['id'], entity="time_entries")):
                    if subrow[bookmark_property] >= start:
                        subrow = utils.reorder_fields_by_schema(subrow, time_entries_schema)
                        singer.write_record("time_entries", subrow, time_extracted=singer.utils.now())

            except HTTPError as e:
                if e.response.status_code == 403:
                    logger.info("The Timesheets feature is unavailable. Skipping the time_entries stream.")
                elif e.response.status_code == 404:
                    # 404 is being returned for deleted tickets and spam
                    logger.info("Could not retrieve time entries for ticket id {}. This may be caused by tickets "
                                "marked as spam or deleted.".format(row['id']))
                else:
                    raise

        utils.update_state(STATE, state_entity, row[bookmark_property])
        row = utils.reorder_fields_by_schema(row, tickets_schema)
        singer.write_record(endpoint, row, time_extracted=singer.utils.now())
        singer.write_state(STATE)


def sync_time_filtered(client, entity):
    bookmark_property = 'updated_at'
    entity_schema = utils.load_schema(entity)

    singer.write_schema(entity,
                        entity_schema,
                        ["id"],
                        bookmark_properties=[bookmark_property])
    start = get_start(entity)

    logger.info("Syncing {} from {}".format(entity, start))
    for row in gen_request(client, get_url(entity)):
        if row[bookmark_property] >= start:
            if 'custom_fields' in row:
                row['custom_fields'] = transform_dict(row['custom_fields'], force_str=True)

            row = utils.reorder_fields_by_schema(row, entity_schema)
            utils.update_state(STATE, entity, row[bookmark_property])
            singer.write_record(entity, row, time_extracted=singer.utils.now())

    singer.write_state(STATE)


def do_sync(client):
    logger.info("Starting FreshDesk sync")

    try:
        fetch_ticket_status = [k for k, v in CONFIG.get('fetch_ticket_status', {}).items() if v is True]
        fetch_sub_entities = [k for k, v in CONFIG.get('fetch_sub_entities', {}).items() if v is True]

        sync_tickets(client, fetch_ticket_status=fetch_ticket_status, fetch_sub_entities=fetch_sub_entities)
        sync_time_filtered(client, "agents")
        sync_time_filtered(client, "roles")
        sync_time_filtered(client, "groups")
        # commenting out this high-volume endpoint for now
        #sync_time_filtered(client, "contacts")
        sync_time_filtered(client, "companies")
    except HTTPError as e:
        logger.critical(
            "Error making request to Freshdesk API: GET %s: [%s - %s]",
            e.request.url, e.response.status_code, e.response.content)
        sys.exit(1)

    logger.info("Completed sync")


def main_impl():
    config, state = utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(config)
    STATE.update(state)
    client = api.FreshdeskClient(config)
    do_sync(client)


def main():
    try:
        main_impl()
    except Exception as exc:
        logger.critical(exc)
        raise exc


if __name__ == '__main__':
    main()
