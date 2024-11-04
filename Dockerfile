# Base R image
FROM docker.io/rocker/r-ver

# Install R dependencies
RUN Rscript -e "install.packages('pak')"
RUN Rscript -e "pak::pak(c('dplyr', 'arrow'))"

WORKDIR /home/hive-guide

# Copy our R script to the container and postcode data file into the container
COPY R/ R/
COPY data-input/ data-input/
COPY data-output/ data-output/

# Run the R script
CMD Rscript R/generate_test_data.R
