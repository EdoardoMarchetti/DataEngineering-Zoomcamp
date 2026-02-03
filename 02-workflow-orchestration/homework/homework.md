## Module 2 Homework

ATTENTION: At the end of the submission form, you will be required to include a link to your GitHub repository or other public code-hosting site. This repository should contain your code for solving the homework. If your solution includes code that is not in file format, please include these directly in the README file of your repository.

> In case you don't get one option exactly, select the closest one

For the homework, we'll be working with the _green_ taxi dataset located here:

`https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/green/download`

To get a `wget`-able link, use this prefix (note that the link itself gives 404):

`https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/`

### Assignment

So far in the course, we processed data for the year 2019 and 2020. Your task is to extend the existing flows to include data for the year 2021.

![homework datasets](../../../02-workflow-orchestration/images/homework.png)

As a hint, Kestra makes that process really easy:

1. You can leverage the backfill functionality in the [scheduled flow](../../../02-workflow-orchestration/flows/09_gcp_taxi_scheduled.yaml) to backfill the data for the year 2021. Just make sure to select the time period for which data exists i.e. from `2021-01-01` to `2021-07-31`. Also, make sure to do the same for both `yellow` and `green` taxi data (select the right service in the `taxi` input).
2. Alternatively, run the flow manually for each of the seven months of 2021 for both `yellow` and `green` taxi data. Challenge for you: find out how to loop over the combination of Year-Month and `taxi`-type using `ForEach` task which triggers the flow for each combination using a `Subflow` task.

   To load the 2021 data there is the `homework2_assignment `flow that can be used for backfilling.

### Quiz Questions

Complete the quiz shown below. It's a set of 6 multiple-choice questions to test your understanding of workflow orchestration, Kestra, and ETL pipelines.

1) Within the execution for `Yellow` Taxi data for the year `2020` and month `12`: what is the uncompressed file size (i.e. the output file `yellow_tripdata_2020-12.csv` of the `extract` task)?

- 128.3 MiB
- 134.5 MiB
- 364.7 MiB
- 692.6 MiB

  Solution:

```yaml
- id: extract
	.......

- id: get_extract_file_size
    description: "Task to get file size in byte"
    type: io.kestra.plugin.core.storage.Size
    uri: "{{render(vars.data)}}"

- id: convert_extract_size_to_mib
    description: "Convert the byte size into MiB" 
    type: io.kestra.plugin.scripts.python.Script
    containerImage: python:slim
    dependencies:
      - kestra
    inputFiles:
      size_bytes: "{{outputs.get_extract_file_size.size}}"
    script: |
      with open("size_bytes", "r") as f:
          size_bytes = int(f.read().strip())
  
      size_mib = size_bytes / (1024 * 1024)
  
      from kestra import Kestra
      Kestra.outputs({
          'size_bytes': size_bytes,
          'size_mib': round(size_mib, 2)
      })
```

   Answer: 128.3 MiB. The size of `yellow_tripdata_2020-12.csv` is 134481400 bytes, that are 128.3 MiB or 134.5 MB.

2) What is the rendered value of the variable `file` when the inputs `taxi` is set to `green`, `year` is set to `2020`, and `month` is set to `04` during execution?

- `{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv`
- `green_tripdata_2020-04.csv`
- `green_tripdata_04_2020.csv`
- `green_tripdata_2020.csv`

  Answer: `green_tripdata_2020-04.csv`.  The render function allows you to render expressions during execution, so in the expression file: "{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv", the placeholders are replaced with the input values for taxi type, year and month.

3) How many rows are there for the `Yellow` Taxi data for all CSV files in the year 2020?

- 13,537.299
- 24,648,499
- 18,324,219
- 29,430,127

  Answer: 24,648,499. To optimize the rows count I created a flow that download each file, unzip it and count the rows. Then in the GCP dataset I have a table where taxi_type, year, month, file and row count are stored. The goal was to reduce the execution time removing the longest task (file uploading). The complete flow is in the repository. The query used to answer the questions 3 and 4 is the following one.

  ```sql
  SELECT taxi_type, SUM(n_rows) 
  FROM `my_dataset.taxi_file_row_counts` 
  WHERE year = 2020
  GROUP by taxi_type
  ```

4) How many rows are there for the `Green` Taxi data for all CSV files in the year 2020?

- 5,327,301
- 936,199
- 1,734,051
- 1,342,034

  Answer: 1,734,051

5) How many rows are there for the `Yellow` Taxi data for the March 2021 CSV file?

- 1,428,092
- 706,911
- 1,925,152
- 2,561,031

  Answer: 1,925,152.

  ```sql
  SELECT *
  FROM `my_dataset.taxi_file_row_counts` 
  WHERE year = 2021 and month=3 and taxi_type = 'yellow'

  ```

6) How would you configure the timezone to New York in a Schedule trigger?

- Add a `timezone` property set to `EST` in the `Schedule` trigger configuration
- Add a `timezone` property set to `America/New_York` in the `Schedule` trigger configuration
- Add a `timezone` property set to `UTC-5` in the `Schedule` trigger configuration
- Add a `location` property set to `New_York` in the `Schedule` trigger configuration

  Answer: Add a `timezone` property set to `America/New_York` in the `Schedule` trigger configuration

```yaml
triggers:
  - id: "yellow_schedule"
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "0 10 1 * *"
    timezone: America/New_York
    inputs:
      taxi: yellow
```
