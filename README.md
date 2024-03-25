# OpsCenter for Snowflake

This is a Snowflake Native Application (using the v2 apis). It provides various reports around Snowflake consumption as well as Labels to categorize queries into workloads and Probes to automatically monitor and cancel and/or email on queries.


## Install via Marketplace

You can use this app by installing it from its [Snowflake Marketplace](https://app.snowflake.com/marketplace/listing/GZTYZT5BVO).

## Local Development

This repo includes the following components:

* /deploy/deploy.py: A python script that deploys the native app to Snowflake
* /bootstrap: A set of scripts that are concatenated and uploaded as a single "setup" script for the native app. These scripts are executed in alphabetic order and must all start with an empty new line (to aid future debugging)
* /ui: A set of files that are uploaded to the web server and served as the UI for the native app

## Project Setup

Clone OpsCenter repository by running:

```
git clone --recurse-submodules https://github.com/sundeck-io/OpsCenter.git
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
python deploy/deploy.py -p myprofile --mock-sundeck-sharing
```

### Local Development outside App
If you want to iterate on the UI/objects quickly, run the following command. This will set up a development database with most objects and then let you run Streamlit
from your desktop or laptop. Most functionality works in this mode.

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
If you want to confirm functioning of behavior such as email integration, you should install the application package and application using "versioned" mode. You can do this by passing deploy a versioned
identifier. For example:

```
export OPSCENTER_PACKAGE=<your unique package name>
export OPSCENTER_APP=<your unique app name>

python deploy/deploy.py -p myprofile -v V1 --mock-sundeck-sharing
```

The environment variables are necessary to prevent a conflict with existing deployments.

`[-d <sundeck-deployment>]` option of deploy.py can be ignored in most cases. It is needed only for testing `Enable notifications via Snowflake SSO` with different Sundeck deployments. Supported values are dev, stage and prod, default is prod.

The `--mock-sundeck-sharing` option will create Snowflake objects such that the `REPORTING.SUNDECK_QUERY_HISTORY` view can be executed normally.

## Deployment Details

Deployment will upload all of the native app to the server, creating any required objects.
It will then install that native app.

Note: The deployment setup is designed such that if a
bootstrap script has errors in it, error messages should indicate the file of the error
and the correct line/character offset in that file (despite the concatenation when uploading).
