from hdx.utilities.downloader import Download
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.whosonfirst.whosonfirst import WhosOnFirst


class TestWhosOnFirst:
    dataset = {
        "dataset_date": "[2015-08-18T00:00:00 TO 2024-03-04T23:59:59]",
        "groups": [{"name": "afg"}],
        "maintainer": "f2e346a1-f2d5-4178-ab52-0b23455e8bef",
        "name": "whosonfirst-data-admin-afg",
        "owner_org": "c9f41aaf-4aa7-4c2f-b9c1-6290a20fd45d",
        "subnational": "1",
        "tags": [
            {
                "name": "administrative boundaries-divisions",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            },
            {
                "name": "geodata",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            },
            {
                "name": "populated places-settlements",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            },
            {
                "name": "population",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            },
        ],
        "title": "Afghanistan: WOF Administrative Subdivisions and Human Settlements",
    }
    resources = [
        {
            "description": "Shapefile(s) for Afghanistan",
            "format": "shp",
            "name": "whosonfirst-data-admin-af-latest.zip",
            "url": "https://data.geocode.earth/wof/dist/shapefile/whosonfirst-data-admin-af-latest.zip",
        }
    ]

    def test_generate_dataset_and_showcase(
        self,
        configuration,
        fixtures_dir,
        input_dir,
        config_dir,
    ):
        with temp_dir(
            "test_whosonfirst", delete_on_success=True, delete_on_failure=False
        ) as tempdir:
            with Download() as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                whosonfirst = WhosOnFirst(
                    configuration, retriever, tempdir, ErrorsOnExit()
                )
                dataset_names = whosonfirst.get_data(
                    "whosonfirst-data-admin-af-latest.zip"
                )
                assert dataset_names == ["whosonfirst-data-admin-af-latest.zip"]

                dataset = whosonfirst.generate_dataset(
                    "whosonfirst-data-admin-af-latest.zip"
                )
                assert dataset == self.dataset
                resources = dataset.get_resources()
                assert resources == self.resources
