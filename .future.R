# This script sets up parallelisation.
# It gets sourced when `library(future)` is run in one of the other scripts.

# Picked up via the container's run command
workers <- as.integer(Sys.getenv("PARALLEL_WORKERS"))

if (is.na(workers)) {
  # If running the script locally this is where you set the number of cores to use.
  # Out of respect for your resources we set 1 core by default.
  # To use ALL available cores, use `workers = future::availableCores()`
  workers <- 1L
}

future::plan(future::multisession(workers = workers))