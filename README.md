# Snowflake stages export

![Support level](https://img.shields.io/badge/support-Tier%202-yellowgreen)

[Snowflake stages](https://docs.snowflake.com/en/sql-reference/ddl-stage.html) are Snowflake objects pointing to a storage location.
They are used to load and unload data from Snowflake, without passing credentials in the copy command.

This plugin is about unloading data from a DSS Snowflake dataset to a Snowflake stage.

## Installation

Please refer to the [Installing plugins](https://doc.dataiku.com/dss/latest/plugins/installing.html) DSS documentation.

### Python code env

This plugin requires Python >= 3.6.

If your DSS built-in Python env doesn't match, please create a [plugin code env](https://doc.dataiku.com/dss/latest/code-envs/plugins.html#creating-code-environment-instances-for-plugins)

## Usage

### Pre-requisites

Before using this plugin, the Snowflake stages must be already created.

This also applies to [custom file formats](https://docs.snowflake.com/en/sql-reference/sql/create-file-format.html) if you wish to use any.

### From your flow

- select a dataset
- click `Export to Snowflake stages` on the right panel
- fill the required parameters
- run the macro.

### From a scenario

- add a `Execute macro` step
- select the `Export to Snowflake stages` macro
- fill the required parameters
- run the scenario.

## Release notes

See the [changelog](CHANGELOG.md) for a history of notable changes to this plugin.

## License

This plugin is distributed under the [Apache License version 2.0](LICENSE).
