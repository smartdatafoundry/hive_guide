# Hive guide

Quick start guide for new users working with Hive partitioned data in R or Python.


## Files

### Sample data

* `generate_test_data.R` standalone script to make some sample data, which is also accessed from the container
* `Dockerfile` instruction set for building the data generator container


To create the data in a container using Podman or Docker:

1. Build the container `podman build --platform linux/amd64 -t demo_data .`
1. Run the container `podman run demo_data`
1. Check the container hash `podman ps --all`
1. Fetch the sample data from the container with: `podman cp <container hash>:data-output/test_data.tar.gz data-output/test_data.tar.gz`
1. Decompress the archive and try the examples.


### Hive examples

* `R_hive_example.qmd` quarto doc with R code chunks
* `R_hive_example.md` *read this file in Github* for the worked examples
* python example - WIP


## The need for speed

If you're interested in some Arrow benchmarks, check out this repo: https://github.com/mikerspencer/arrow_test/, which was used for an EdinbR talk.
