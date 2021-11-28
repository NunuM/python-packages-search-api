import asyncio
import datetime
import hashlib
import json
import logging
import math
import re
import sqlite3
import ssl
import time
import xmlrpc.client
from calendar import monthrange

import aiohttp
import certifi

GITHUB_URL = re.compile("https?:\/\/github.com\/([aA-zZ0-9_\-\.]+)\/([aA-zZ0-9_\-\.]+)[\w|\d|\/|-|\.]*?\"")
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36'
LOGGER = logging.getLogger(__name__)

STALE_DELTA = 2629743
ROWS_PER_PAGE = 5

HEADERS = {
    'User-Agent': USER_AGENT,
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5'
}


class PackageManager:

    def __init__(self, connection_string):
        """
        :param str connection_string:
        """
        self.con = sqlite3.connect(connection_string)
        self._create_tables()

    def _create_tables(self):
        """
        Create tables and indices if not exists
        :return: bool
        """
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS packages (
                name TEXT, 
                description TEXT,
                home_page TEXT,
                package_url TEXT,
                stars NUMBER, 
                version TEXT,
                updated NUMBER
            );
        """)

        self.con.execute("CREATE INDEX IF NOT EXISTS idx_stars on packages(stars)")
        self.con.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_package_name on packages(name)")

        self.con.execute("CREATE VIRTUAL TABLE IF NOT EXISTS names USING fts5(name)")

        self.con.execute("CREATE TABLE IF NOT EXISTS state (letter TEXT, hash TEXT )")

        # self._bootstrap_packages_names()

        return True

    def _bootstrap_packages_names(self):
        """
        Process all packages, sharded by the first letter
        :return: bool
        """
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        ssl_context.verify_mode = ssl.CERT_OPTIONAL
        ssl_context.load_verify_locations(certifi.where())

        client = xmlrpc.client.ServerProxy('https://pypi.org/pypi', context=ssl_context)
        all_repos = client.list_packages()
        all_repos.sort()

        all_repos = self._partial_update(all_repos)

        previous_key = None
        response_hashes = {}
        packages = []

        for package_name in all_repos:
            key = package_name[0]

            if previous_key is None:
                previous_key = key

            if previous_key != key:

                LOGGER.debug("Processing key {}".format(previous_key))

                items_hashed = hashlib.md5(
                    "".join(response_hashes[previous_key]['items']).encode('utf8')).hexdigest()

                response_hashes[previous_key]['hash'] = items_hashed

                rows = self.con.execute("SELECT * FROM state WHERE letter=? LIMIT 1", (previous_key,)).fetchall()
                list_hash = None

                if len(rows) == 1:
                    list_hash = rows[0][1]

                if list_hash is None or list_hash != items_hashed:
                    for item in response_hashes[previous_key]['items']:
                        if list_hash is None or len(
                                self.con.execute("SELECT * FROM names WHERE name=?", (item,)).fetchall()) == 0:
                            packages.append((item,))

                    if len(packages) > 0:
                        self.con.executemany("""INSERT INTO names (name) VALUES (?)""", packages)

                        if list_hash is None:
                            self.con.execute("INSERT INTO state (letter, hash) VALUES (?,?)",
                                             (previous_key, items_hashed))
                        else:
                            self.con.execute("UPDATE state SET hash=? WHERE letter=?", (items_hashed, previous_key))

                        self.con.commit()

                        packages.clear()

                del response_hashes[previous_key]['items']

                previous_key = key

            if key not in response_hashes:
                response_hashes[key] = {'items': [], 'hash': ''}

            response_hashes[key]['items'].append(package_name)

        return True

    def _partial_update(self, all_repos):
        """
        Slice shards by day and update the keys for today
        :param list all_repos:
        :return: bool
        """
        now = datetime.datetime.now()

        cursor = self.con.execute("SELECT letter FROM state ORDER BY letter")

        rows = cursor.fetchall()
        row_count = len(rows)

        if row_count == 0:
            return all_repos

        num_of_days = monthrange(now.year, now.month)[1]

        shards_per_day = math.ceil(len(rows) / num_of_days)

        start_idx = (3 - 1) * shards_per_day
        end_idx = start_idx + shards_per_day

        if start_idx >= row_count:
            all_repos.clear()
            return []

        if end_idx > row_count:
            end_idx = row_count

        shard_keys = [i[0] for i in rows[start_idx:end_idx]]

        return filter(lambda x: x[0] in shard_keys, all_repos)

    def search_by_name(self, package_name, page=0):
        """
        Query database using full text search for a given search param
        and then get the metadata for each package
        :param str package_name:
        :param int page:
        :return: list
        """
        current_page = abs(page)
        now = round(time.time())
        cursor = self.con.execute("SELECT * FROM names WHERE name MATCH ? ORDER BY rank LIMIT ? OFFSET ?",
                                  ("\"{}\"".format(package_name), ROWS_PER_PAGE, ROWS_PER_PAGE * current_page))

        candidates_packages = cursor.fetchall()

        if len(candidates_packages) == 0:
            return []

        markers = ("?," * len(candidates_packages))[0:-1]
        candidate_names = [candidate[0] for candidate in candidates_packages]

        search_params = ()

        for candidate in candidates_packages:
            search_params = search_params + candidate

        packages_metadata = self.con.execute(
            "SELECT * FROM packages WHERE name IN ({}) ORDER BY stars DESC".format(markers), search_params)

        result_from_db = packages_metadata.fetchall()
        packages_without_metadata = []

        keys = {item[0]: {
            'name': item[0],
            'description': item[1],
            'home_url': item[2],
            'package_url': item[3],
            'stars': item[4],
            'version': item[5],
            'updated': item[6]
        } for
            item in result_from_db}

        result = []
        for candidate in candidate_names:
            if candidate not in keys:
                packages_without_metadata.append(candidate)
            elif (now - keys[candidate]['updated']) > STALE_DELTA:
                del keys[candidate]
                packages_without_metadata.append(candidate)
                break
            else:
                result.append(keys[candidate])

        try:

            asyncio.get_event_loop()
            loop = asyncio.get_event_loop()
            packages_with_meta = loop.run_until_complete(PackageManager._get_metadata(packages_without_metadata))

            self.insert_packages_metadata(packages_with_meta)

            result += packages_with_meta
        except:
            LOGGER.exception("Error fetching metadata from pypi")

        def sort_key(d):
            return d['stars']

        result.sort(key=sort_key, reverse=True)

        return {'current_page': current_page, 'has_more': len(result) == ROWS_PER_PAGE, 'packages': result}

    def insert_packages_metadata(self, packages):
        """
        Insert metadata about the packages. First try in batch,
        On UNIQUE error, it tries to insert one by one
        :param list packages:
        :return: bool
        """
        now = int(time.time())
        tuples = []
        query = "INSERT INTO packages (name,description,home_page,package_url,version,stars,updated) VALUES (?,?,?,?,?,?,?)"

        for package in packages:
            tuples.append((
                package['name'],
                package['description'],
                package['home_page'],
                package['package_url'],
                package['version'],
                package['stars'],
                now))

        try:
            self.con.executemany(query, tuples)
            self.con.commit()
            return True
        except:
            LOGGER.exception("Metadata batch insert error")

        for item in tuples:
            try:
                self.con.execute(query, item)
                self.con.commit()
            except:
                self.con.execute("UPDATE packages SET version=?, stars=?, updated=? WHERE name=?",
                                 (item[4], item[5], item[6], item[0]))
                self.con.commit()

        return True

    @staticmethod
    async def _get_metadata(packages_names):
        """
        Given a list of packages it tries to get the correspondent
        metadata
        :param list packages_names:
        :return: list
        """
        now = int(time.time())

        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        ssl_context.verify_mode = ssl.CERT_OPTIONAL
        ssl_context.load_verify_locations(certifi.where())

        metadata = []

        timeout = aiohttp.ClientTimeout(total=60)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            for packages_name in packages_names:
                async with session.get("https://pypi.org/pypi/{}/json".format(packages_name),
                                       ssl=ssl_context) as request:
                    if request.status > 300:
                        continue

                    body = await request.read()
                    packages_metadata = json.loads(body)

                    metadata.append({
                        'name': packages_name,
                        'description': packages_metadata['info']['summary'],
                        'home_page': packages_metadata['info']['home_page'],
                        'package_url': packages_metadata['info']['package_url'],
                        'version': packages_metadata['info']['version'],
                        'stars': await PackageManager._get_stars(session, body, ssl_context),
                        'updated': now
                    })

        return metadata

    @staticmethod
    async def _get_stars(session, pip_metadata, ssl_context):
        """
        Tries to get the stars that a given project has. Scrape HTML due to the GitHub's API rate limiting
        :param bytes pip_metadata:
        :param ssl.SSLContext ssl_context:
        :return: int
        """
        try:
            matches = GITHUB_URL.search(str(pip_metadata))

            if matches is None:
                return 0

            groups = matches.groups()
            github_user = groups[0]
            github_project = groups[1]

            async with session.get("https://github.com/{}/{}".format(github_user, github_project),
                                   headers=HEADERS,
                                   ssl=ssl_context) as response:

                if response.status > 299:
                    return 0

                html = str(await response.read())

                idx = html.index("social-count")
                counter = ''

                while html[idx] != '>':
                    idx += 1

                while html[idx] != '<':
                    if '0' <= html[idx] <= '9' or html[idx] == 'k':
                        counter += html[idx]
                    idx += 1

                stars = counter.strip()

                if stars.endswith("k"):
                    return int(stars[0:-1]) * 1000

                return int(stars)
        except:
            LOGGER.exception("Error getting stars from GitHub")
            return 0
