# OpsCenter for Snowflake

This is a Snowflake Native Application (using the v2 apis). It provides various reports around Snowflake consumption as well as Labels to categorize queries into workloads and Probes to automatically monitor and cancel and/or email on queries.


## Install via Marketplace

You can use this app by installing it from it's [Snowflake Marketplace](https://app.snowflake.com/marketplace/listing/GZTYZT5BVO).

## Local Development

This repo includes the following components:

* /deploy/deploy.py: A python script that deploys the native app to Snowflake
* /bootstrap: A set of scripts that are concatenated and uploaded as a single "setup" script for the native app. These scripts are executed in alphabetic order and must all start with an empty new line (to aid future debugging)
* /ui: A set of files that are uploaded to the web server and served as the UI for the native app

## Project Setup

Clone OpsCenter repository by running:

```
git clone <URL of the repository> --recurse-submodules
```

This project uses [Poetry](https://python-poetry.org/). Please refer to [Poetry's documentation](https://python-poetry.org/docs/#installation)
on how to install this tool.

To set up OpsCenter for local development the first time, run the following command to create a virtual environment and install dependencies:

```
poetry install
```

This command will create a Poetry environment which is used by Poetry for all future calls. Please refer
to the [Poetry documentation on environments](https://python-poetry.org/docs/managing-environments/)
for managing multiple environments.

### Local Development as App
To start using, add the following items to your ~/.snowsql/config file:

```
[connections.myprofile]
accountname=...
username="..."
password="..."
warehousename ="..."
dbname = "..."
```

Once that is configured, you can run the following to setup a new application package and install it. This invocation is in debug/local mode.
```
python deploy/deploy.py -p myprofile
```

### Local Development outside App
If you want iterate on the UI/objects quickly, run the following command. This will setup a development database with most objects and then let you run Streamlit
from your desktop or laptop. Most funtionality works in this mode.

Like above, add the following items to your ~/.snowsql/config file

```
[connections.local_dev]
accountname=...
username="..."
password="..."
warehousename ="..."
dbname = "..."
```

Then run the following commands:
```
# Create dev database
poetry run python deploy/devdeploy.py -p local_dev

# Run streamlit locally
OPSCENTER_PROFILE=local_dev poetry run streamlit run app/ui/Home.py
```

### Versioned App Development
If you want to confirm functioning of all behavior, you should install the application package and application using "versioned" mode. You can do this by passing deploy a versioned
identifier. For example:

```
python deploy/deploy.py -p myprofile -v V1
```


## Deployment Details

Deployment will upload all of the native app to the server, creating any required objects.
It will then install that native app.

Note: The deployment setup is designed such that if a
bootstrap script has errors in it, error messages should indicate the file of the error
and the correct line/character offset in that file (despite the concatenation when uploading).
