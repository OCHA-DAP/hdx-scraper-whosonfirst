#!/usr/bin/python
"""Who's On First scraper"""

import logging
from datetime import datetime, timezone
from typing import List, Optional

from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date
from slugify import slugify

logger = logging.getLogger(__name__)


class WhosOnFirst:
    def __init__(self, configuration, retriever, folder, errors):
        self.configuration = configuration
        self.retriever = retriever
        self.folder = folder
        self.errors = errors
        self.dataset_data = {}

    def get_data(self, datasets=None) -> List[str]:
        inventory_url = self.configuration["inventory_json"]
        inventory = self.retriever.download_json(inventory_url)

        for entry in inventory:
            dataset_name = entry["name_compressed"]
            if len(dataset_name.split("-")) != 5:
                continue

            if dataset_name.split("-")[2] != "admin":
                continue

            if entry["vintage"] != "latest":
                continue

            if datasets and dataset_name not in datasets:
                continue
            last_update_date = entry["last_updated"]
            if last_update_date:
                last_update_date = parse_date(last_update_date)
            else:
                last_update_date = datetime.now(tz=timezone.utc)
            self.dataset_data[dataset_name] = {
                "name": dataset_name,
                "date": last_update_date,
            }

        dataset_names = sorted(list(self.dataset_data.keys()))
        return dataset_names

    def generate_dataset(self, dataset_name: str) -> Optional[Dataset]:
        metadata = self.dataset_data[dataset_name]
        dataset_name = metadata["name"]
        country = dataset_name.split("-")[3]
        country_info = Country.get_country_info_from_iso2(country)
        if country == "xk":
            country_info = {
                "#country+code+v_iso3": "XKX",
                "#country+name+preferred": "Kosovo",
            }
        if not country_info:
            if country not in self.configuration["non_country_territories"]:
                self.errors.add(f"Could not get country info from {dataset_name}")
            return None

        country_iso = country_info["#country+code+v_iso3"]
        country_name = country_info["#country+name+preferred"]
        if country == "gb":
            country_name = "United Kingdom"

        name = f"whosonfirst-data-admin-{country_iso.lower()}"
        end_date = metadata["date"]
        old_end_date = check_dataset_date(name)
        if old_end_date and end_date <= old_end_date:
            return None

        title = f"{country_name}: WOF Administrative Subdivisions and Human Settlements"
        dataset = Dataset({"name": slugify(name), "title": title})
        dataset.set_maintainer("f2e346a1-f2d5-4178-ab52-0b23455e8bef")
        dataset.set_organization("c9f41aaf-4aa7-4c2f-b9c1-6290a20fd45d")
        dataset.set_subnational(True)
        if country == "xk":
            dataset.add_other_location(country_iso)
        else:
            dataset.add_country_location(country_name)
        start_date = parse_date("2015-08-18")
        dataset.set_time_period(start_date, end_date)
        dataset.add_tags(
            [
                "administrative boundaries-divisions",
                "geodata",
                "populated places-settlements",
                "population",
            ]
        )
        resource = Resource(
            {
                "name": dataset_name,
                "description": f"Shapefile(s) for {country_name}",
                "url": f"https://data.geocode.earth/wof/dist/shapefile/{dataset_name}",
            }
        )
        resource.set_format("shp")
        dataset.add_update_resource(resource)

        return dataset


def check_dataset_date(dataset_name: str) -> Optional[datetime]:
    dataset = Dataset.read_from_hdx(dataset_name)
    if not dataset:
        return None
    end_date = dataset.get_time_period()["enddate"]
    return end_date
