import logging
from common.package import PackageIndexer

indexer = PackageIndexer('packages.db')

LOGGER = logging.getLogger(__name__)


def main(context):
    page = 0
    query = ''

    query_params = context.get_request_query_params()

    if 'q' in query_params:
        q = query_params['q'][0]

    if 'p' in query_params and query_params['p'][0].isdigit():
        page = int(query_params['p'][0])

    try:
        results = indexer.search_by_name(query, page)
        return {
            'status': 200,
            'body': results,
            'headers': {'Content-Type': 'application/json'}
        }
    except:
        LOGGER.exception("Error searching {}".format(query))
        return {'status': 500}
