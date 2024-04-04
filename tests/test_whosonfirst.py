#!/usr/bin/python
"""
Unit tests for peacekeeping.

"""
from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.data.vocabulary import Vocabulary
from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent

from whosonfirst import WhosOnFirst


class TestPeaceSecurity:
    dataset = {
        "dataset_date": "[2015-08-18T00:00:00 TO *]",
        "groups": [{"name": "afg"}],
        "name": "whosonfirst-data-admin-afg",
        "subnational": "1",
        "title": "Afghanistan: Who's On First Administrative Subdivisions and Human Settlements",
    }
    resource = {
        "description": "Administrative polygons for Afghanistan",
        "format": "shp",
        "name": "whosonfirst-data-admin-af-latest.zip",
        "resource_type": "api",
        "url": "https://data.geocode.earth/wof/dist/shapefile/whosonfirst-data-admin-af-latest.zip",
        "url_type": "api",
    }

    @pytest.fixture(scope="function")
    def fixtures(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="function")
    def configuration(self):
        Configuration._create(
            hdx_read_only=True,
            user_agent="test",
            project_config_yaml=join("config", "project_configuration.yaml"),
        )
        UserAgent.set_global("test")
        tags = (
            "complex emergency-conflict-security",
            "peacekeeping",
            "fatalities",
        )
        Vocabulary._tags_dict = {tag: {"Action to Take": "ok"} for tag in tags}
        tags = [{"name": tag} for tag in tags]
        Vocabulary._approved_vocabulary = {
            "tags": tags,
            "id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            "name": "approved",
        }
        return Configuration.read()

    def test_generate_dataset_and_showcase(self, configuration, fixtures):
        with temp_dir(
            "test_whosonfirst", delete_on_success=True, delete_on_failure=False
        ) as folder:
            with Download() as downloader:
                retriever = Retrieve(downloader, folder, fixtures, folder, False, True)
                whosonfirst = WhosOnFirst(configuration, retriever, folder, ErrorsOnExit())
                dataset_names, _ = whosonfirst.get_data(
                    {"DEFAULT": parse_date("2023-01-01")},
                    datasets="whosonfirst-data-admin-af-latest.zip",
                )
                assert dataset_names == [{"name": "whosonfirst-data-admin-af-latest.zip"}]

                dataset = whosonfirst.generate_dataset("whosonfirst-data-admin-af-latest.zip")
                assert dataset == self.dataset
                resources = dataset.get_resources()
                assert resources[0] == self.resource
