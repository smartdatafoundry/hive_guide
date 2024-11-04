# ---------------------------------
# ---------------------------------
# generate test data
# ---------------------------------
# ---------------------------------

library(dplyr)
library(arrow)

# ---------------------------------
# Demographic
sample_size = 50000



# Load in list of all UK postal districts
all_postal_districts = read.csv("data-input/postal_districts.csv", header = FALSE)

# Create df of customer information
customer_information = tibble(
  cid = paste0(sample(0:9, sample_size, replace = TRUE), sample(100000000:999999999, sample_size, replace = FALSE)),
  postal_district = sample(all_postal_districts$V1, sample_size, replace = TRUE),
  sex = sample(c("M", "F"), sample_size, replace = TRUE),
  dob = as.Date(runif(sample_size, as.numeric(as.Date("1919-01-06")), as.numeric(as.Date("2001-01-06"))), origin = "1970-01-01"),

  # Whether customer has a mortgage/ is renting/ neither / both for use in expenditure categories
  has_mortgage = rbinom(sample_size, 1, 0.2),
  is_renting = ifelse(has_mortgage == 1, rbinom(sample_size,1, 0.001), rbinom(sample_size,1,0.2)),
  # Whether customer has fluctuating weekly income, or monthly salary
  weekly_earner = rbinom(sample_size, 1, 0.5),
  # Initialise the cash balance for the first week
  cash_balance_final = rnorm(sample_size, mean = 1000, sd = 200)

)
# ---------------------------------
# Weekly

dates = seq.Date(as.Date("2019-01-06"), by = 7, to = Sys.Date())



parallel::mclapply(dates, mc.cores = 1, function(i){
  # Delete a random sample of 1% of customers with 10% probability from the data set to mimic customers closing accounts (so on average we lose 1% of customers every 10 weeks)
  # Adjust probability/proportion as necessary, or remove if statement to invoke a smooth decline
  if  (runif(1) < 0.1){
    rows_to_delete = sample(1:nrow(customer_information), size = round(0.01 * nrow(customer_information)), replace = FALSE)
    customer_information <<- customer_information[-rows_to_delete,]
  }
  # Sample 99% of all ids in a particular week
  sample_of_cids = sample(1:nrow(customer_information), size = round(0.99 * nrow(customer_information)), replace = FALSE)
  sample_customer_info = customer_information[sample_of_cids,]
  new_sample_size = nrow(sample_customer_info)

  sample_customer_info %>%
    mutate(end_of_this_period = i,
           end_of_previous_period = end_of_this_period -7,
           age = as.integer(difftime(i, dob, units = "weeks") / 52),  # Calculate age based on dob and end_of_this_period
           age_band = case_when(
             age >= 18 & age <= 19 ~ "18-19",
             age >= 20 & age <= 24 ~ "20-24",
             age >= 25 & age <= 29 ~ "25-29",
             age >= 30 & age <= 34 ~ "30-34",
             age >= 35 & age <= 39 ~ "35-39",
             age >= 40 & age <= 44 ~ "40-44",
             age >= 45 & age <= 49 ~ "45-49",
             age >= 50 & age <= 54 ~ "50-54",
             age >= 55 & age <= 59 ~ "55-59",
             age >= 60 & age <= 64 ~ "60-64",
             age >= 65 & age <= 69 ~ "65-69",
             age >= 70 & age <= 74 ~ "70-74",
             age >= 75 & age <= 79 ~ "75-79",
             age >= 80 & age <= 84 ~ "80-84",
             age >= 85 ~ "85+"
           ),
           income_salary = if_else(weekly_earner == 1, rlnorm(new_sample_size) * 500, ifelse(runif(1)<0.75,0,rlnorm(new_sample_size) * 2000)),
           income_benefits_universal_credit = rlnorm(new_sample_size) * 10,
           income_benefits_housing_credit = rlnorm(new_sample_size) * 10,
           income_benefits_tax_credit = rlnorm(new_sample_size) * 10,
           income_benefits_pension_credit = rlnorm(new_sample_size) * 10,
           income_benefits_other = rlnorm(new_sample_size) * 10,
           income_pension_lump_sum = rlnorm(new_sample_size) * 5000,
           income_pension_regular_payment = rlnorm(new_sample_size) * 100,
           income_investment = ifelse(runif(1) < 0.9,0,rlnorm(new_sample_size) * 50),
           income_interest = ifelse(runif(1) < 0.9,0,rlnorm(new_sample_size) * 50),
           income_other = rlnorm(new_sample_size) * 100,
           income = income_salary + income_benefits_universal_credit + income_benefits_housing_credit +
             income_benefits_tax_credit + income_benefits_pension_credit + income_benefits_other +
             income_pension_lump_sum + income_pension_regular_payment + income_investment +
             income_interest + income_other,
           expenditure_committed_mortgage = ifelse(has_mortgage == 1, (income + runif(new_sample_size, income*-1, income))* 0.2, 0),
           expenditure_committed_rent = ifelse(is_renting == 1, (income + runif(new_sample_size, income*-1, income))* 0.2, 0),
           expenditure_committed_other = (income + runif(new_sample_size, income*-1, income))* 0.1,
           expenditure_essential_council_tax = (income + runif(new_sample_size, income*-1, income))* 0.1,
           expenditure_essential_other = (income + runif(new_sample_size, income*-1, income))* 0.4,
           expenditure_qol = (income + runif(new_sample_size, income*-1, income))* 0.1,
           expenditure_discretionary_pension = (income + runif(new_sample_size, income*-1, income))* 0.0025,
           expenditure_discretionary_other = (income + runif(new_sample_size, income*-1, income))*0.0025,
           expenditure_uncategorized = (income + runif(new_sample_size, income*-1, income))* 0.05,
           expenditure = expenditure_committed_mortgage + expenditure_committed_rent +
             expenditure_committed_other + expenditure_essential_council_tax +
             expenditure_essential_other + expenditure_qol +
             expenditure_discretionary_pension + expenditure_discretionary_other +
             expenditure_uncategorized,
           cash_min = runif(new_sample_size, cash_balance_final - expenditure, cash_balance_final),
           cash_max = runif(new_sample_size, cash_balance_final, cash_balance_final + income),
           # Implement persistent cash_balance_final such that cash_balance_final(n) = cash_balance_final(n-1) + income - expenditure
           cash_balance_final = cash_balance_final + income - expenditure) %>%
    # Remove cols that are not present in NWG data
    select(-age,-dob, -is_renting, -has_mortgage, -weekly_earner) %>%
    group_by(end_of_this_period) %>%
    write_dataset("data-output/test_data/")

  # Update cash balances in customer_information for next week
  customer_information[c(sample_of_cids), "cash_balance_final"] <<- sample_customer_info$cash_balance_final

  gc()
})

# Compress
system("tar -zcvf data-output/test_data.tar.gz data-output/test_data")
