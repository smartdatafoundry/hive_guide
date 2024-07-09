# Base R image
FROM docker.io/rocker/r-ver

# Make a directory in the container
RUN mkdir /home/r-environment

# Install R dependencies
RUN R -e "install.packages(c('dplyr', 'arrow'))"

# Copy our R script to the container
COPY generate_test_data_container.R /home/r-environment/script.R

# Run the R script
CMD R -e "source('/home/r-environment/script.R')"
