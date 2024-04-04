### Collector for Who's On First Datasets
[![Build Status](https://github.com/OCHA-DAP/hdx-scraper-whosonfirst/actions/workflows/run-python-tests.yaml/badge.svg)](https://github.com/OCHA-DAP/hdx-scraper-whosonfirst/actions/workflows/run-python-tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-whosonfirst/badge.svg?branch=main&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-whosonfirst?branch=main)

This script connects to the [Who's On First site](https://geocode.earth/data/whosonfirst) and extracts data links and metadata creating one dataset per country in HDX. It makes one read from the data inventory and around 200 read/writes (API calls) to HDX in a one hour period. It creates one temporary file and is run every month.


### Usage

    python run.py

For the script to run, you will need to have a file called .hdx_configuration.yaml in your home directory containing your HDX key eg.

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod
    
 You will also need to supply the universal .useragents.yaml file in your home directory as specified in the parameter *user_agent_config_yaml* passed to facade in run.py. The collector reads the key **hdx-scraper-whosonfirst** as specified in the parameter *user_agent_lookup*.
 
 Alternatively, you can set up environment variables: USER_AGENT, HDX_KEY, HDX_SITE, TEMP_DIR, LOG_FILE_ONLY