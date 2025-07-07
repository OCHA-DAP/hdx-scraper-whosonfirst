#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this
script then creates in HDX.

"""

import logging
from copy import deepcopy
from os.path import dirname, expanduser, join

from hdx.api.configuration import Configuration
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.path import temp_dir_batch
from hdx.utilities.retriever import Retrieve
from hdx.utilities.state import State

from hdx.scraper.whosonfirst.whosonfirst import WhosOnFirst

logger = logging.getLogger(__name__)

_USER_AGENT_LOOKUP = "hdx-scraper-whosonfirst"
_SAVED_DATA_DIR = "saved_data"  # Keep in repo to avoid deletion in /tmp
_UPDATED_BY_SCRIPT = "HDX Scraper: Who's On First"


def main(
    save: bool = True,
    use_saved: bool = False,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to True.
        use_saved (bool): Use saved data. Defaults to False.

    Returns:
        None
    """
    configuration = Configuration.read()
    User.check_current_user_write_access("wof")

    with ErrorsOnExit() as errors:
        with State(
            "dataset_dates.txt",
            State.dates_str_to_country_date_dict,
            State.country_date_dict_to_dates_str,
        ) as state:
            state_dict = deepcopy(state.get())
            with temp_dir_batch(_USER_AGENT_LOOKUP) as info:
                folder = info["folder"]
                with Download() as downloader:
                    retriever = Retrieve(
                        downloader, folder, "saved_data", folder, save, use_saved
                    )
                    folder = info["folder"]
                    batch = info["batch"]
                    whosonfirst = WhosOnFirst(configuration, retriever, folder, errors)
                    dataset_names, state_dict = whosonfirst.get_data(state_dict)
                    logger.info(f"Number of datasets to upload: {len(dataset_names)}")

                    for dataset_name in dataset_names:
                        dataset = whosonfirst.generate_dataset(dataset_name)
                        if dataset:
                            dataset.update_from_yaml(
                                path=join(
                                    dirname(__file__),
                                    "config",
                                    "hdx_dataset_static.yaml",
                                )
                            )
                            dataset["notes"] = dataset["notes"].replace(
                                "\n", "  \n"
                            )  # ensure markdown has line breaks
                            dataset.create_in_hdx(
                                remove_additional_resources=True,
                                match_resource_order=False,
                                hxl_update=False,
                                updated_by_script=_UPDATED_BY_SCRIPT,
                                batch=batch,
                            )

            state.set(state_dict)


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_USER_AGENT_LOOKUP,
        project_config_yaml=join(
            dirname(__file__), "config", "project_configuration.yaml"
        ),
    )
