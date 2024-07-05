# R example
Mike Spencer

## Intro

Welcome to our quick start guide to working with Hive partitioned data.
This is a data format which grew out of the distributed computing
landscape of Hadoop and helps scale workflows, both through speed
improvements and resource reduction.

This guide is written in Quarto, which is a literate programming wrapper
for combining code and text into compiled output.

The document is split into code chunks to give you some examples of how
to efficiently work with Hive partitioned data.

## Data

These are stored on disk in a parquet format. The user does not need to
interact with data at a file level. The directory (in the example case
`test_data`) is treated as a whole dataset with operations implemented
in a similar way to a database.

This database interaction is key - use tools to subset and/or aggregate
only the required data prior to loading into R. This dramatically
reduces the hardware resource use and increases code speed.

Sample data (if required) can be created with `generate_test_data.R`.

## Packages

R has two main libraries for working with Hive partitioned data -
`arrow` and `duckdb`. You do not need to use both - choose which fits
best into your workflow. Both of these packages are linked to the
`dplyr` API so you can use most of your usual tidyverse workflow to
interact with the dataset.

``` r
library(arrow)
```


    Attaching package: 'arrow'

    The following object is masked from 'package:utils':

        timestamp

``` r
# or library(duckdb)

library(dplyr) # both above require
```


    Attaching package: 'dplyr'

    The following objects are masked from 'package:stats':

        filter, lag

    The following objects are masked from 'package:base':

        intersect, setdiff, setequal, union

## Connect to the data

Unlike working with CSVs, or similar, data Hive partitioned data are not
loaded directly into R. Rather, a connection to the dataset is made -
much like in database operations. This connection (either Arrow or
DuckDB) is used for rapid selection, filtering, etc. of data, which can
then be written to a file or read into R.

``` r
ds = open_dataset("test_data")

# print the column names and types
ds
```

    FileSystemDataset with 287 Parquet files
    cid: string
    sex: string
    income: double
    income_salary: double
    income_benefits: double
    income_pension: double
    income_investment: double
    income_interest: double
    income_other: double
    expenditure: double
    expenditure_committed: double
    expenditure_essential: double
    expenditure_qol: double
    expenditure_discretionary: double
    expenditure_uncategorized: double
    cash_balance_final: double
    cash_min: double
    cash_max: double
    end_of_this_period: string

``` r
# One quirk is that the column used for the partition becomes a string, so let's fix this
ds = open_dataset("test_data") %>% 
  mutate(end_of_this_period = as.Date(end_of_this_period))
```

## Selecting columns, filtering rows

As mentioned in the packages section, Arrow and DuckDB are mapped to
dplyr’s API. This means you can do usual dplyr activity.

``` r
# From ds (dataset) connection in previous code chunk
# ID and total income columns for all males
x = ds %>% 
  select(cid, sex, income) %>% 
  filter(sex == "M")

# summary of result (still in Arrow/DuckDB)
x
```

    FileSystemDataset (query)
    cid: string
    sex: string
    income: double

    * Filter: (sex == "M")
    See $.data for the source Arrow object

## Aggregations

We can use dplyr’s `group_by` and `summarise` to aggregate data.

``` r
# From ds (dataset) connection in previous code chunk
x = ds %>% 
  group_by(sex) %>% 
  summarise(mean_income = mean(income),
            median_income = median(income))
```

    Warning: median() currently returns an approximate median in Arrow
    This warning is displayed once per session.

``` r
# summary of result (still in Arrow/DuckDB)
x
```

    FileSystemDataset (query)
    sex: string
    mean_income: double
    median_income: double

    See $.data for the source Arrow object

## Bring data into R

In the previous code chunk we made some simple aggregations of mean and
median income by sex. We’ll now bring these into our R session.

Now R has only touched the data it needs, with Arrow/DuckDB doing all
the heavy lifting.

``` r
# execute query and print
x %>% 
  collect()
```

    # A tibble: 2 × 3
      sex   mean_income median_income
      <chr>       <dbl>         <dbl>
    1 M          41236.        25084.
    2 F          41233.        25078.

``` r
# Make pretty
x %>% 
  collect() %>%
  knitr::kable(caption = "Example results from an aggregation",
               col.names = c("Sex", "Mean income", "Median income"),
               format.args = list(big.mark = ","),
               digits = 0)
```

| Sex | Mean income | Median income |
|:----|------------:|--------------:|
| M   |      41,236 |        25,019 |
| F   |      41,233 |        25,051 |

Example results from an aggregation

## Rolling windows/moving averages

Much of our data is available at a weekly timestep and so it can be
useful to apply tools like rolling windows to retain the temporal
resolution but make the results more relevant to the way people manage
their finances.

Two common rolling windows we apply are a five and thirteen week. The
former is the closest we can get to a month, with the latter being a
good approximation of a quarter.

One way to create rolling windows is shown below, the speed performance
of this is acceptable (given the data volume), but quicker methods may
exist. The method subsets each window from the main dataset and writes
results out. Note that the final `group_by` is used to create the new
Hive partitions.

``` r
# Date series
x = ds %>% 
  distinct(end_of_this_period) %>% 
  collect()

date_start = x %>% 
  filter(end_of_this_period == min(end_of_this_period))

date_end = x %>% 
  filter(end_of_this_period == max(end_of_this_period))

# List of rolling windows
seq_5wk = seq.Date(as.Date(date_start[[1]]) + 35,
                  as.Date(date_end[[1]]),
                  by = "week") %>% 
  lapply(function(i){
    tibble(end_of_5wk_period = i,
           end_of_this_period = i - (7 * (4:0)))
  })

# Use parallel computation kindly!
parallel::mclapply(seq_5wk, mc.cores = 5, function(i){
  ds %>% 
    filter(end_of_this_period %in% i$end_of_this_period) %>% 
    select(-end_of_this_period) %>% 
    group_by(cid) %>% 
    summarise(observations = n(),
              overdraft_weeks = sum(cash_min < 0),
              salary_tot = sum(income_salary)) %>% 
    mutate(end_of_5wk_period = i$end_of_5wk_period[1]) %>% 
    group_by(end_of_5wk_period) %>% 
    write_dataset("example_rolling/")
  gc()
}) %>% 
  invisible()
```
