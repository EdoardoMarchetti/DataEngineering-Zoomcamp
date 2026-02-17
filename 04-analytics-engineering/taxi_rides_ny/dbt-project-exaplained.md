## Analysis folder

* A place for sql files that you do not want to expose
* Used for SQL queries to check data quality

## dbt_project.yml

* The most important file in dbt
* Tell dbt some defaults
* You need it tor run dbt commands
* For dbt core, your profile should match the one in the .dbt/profiles.yml

## macros

* Behave like Python functions -> reusable logic
* They help you ecanpsulate logic
* They can be tested

## Readme

- The documentation of the project
- Installation/setup guide
- Contact Information

## seeds

- A space to upload csv and flat files (to add them to dbt later)
- Quick and dirty approach (better to fix at source)

## snapshots

- Take a picture of a table at a moment in time
- Useful to track the history of a column that overwrites itself

## tests

- A place to put assertions in SQL format
- A place for singular tests
- If this SQL command returns more than 0 rows, the dbt build fails

## models

* dbt suggests 3 subfolders:
  * staging
    * all sql sources (raw tables)
    * staging files are 1 to 1 copy of your data with minimal cleaning stages (data types, renaming columns, ...)
  * intermidiate
    * Anything that is not raw nor ready to use
  * marts
    * all data to expose to end users
    * data ready for comsumptions
    * tables ready for dashboards
    * properly modeled, clea tables
