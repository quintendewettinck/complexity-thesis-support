# Data processing

# Clear the environment ---------------------------------------------------
rm(list=ls())

# Packages ----------------------------------------------------------------
library(dplyr)
library(data.table)
library(glue)
library(readr)
library(readxl)
library(economiccomplexity)

# Config ------------------------------------------------------------------
# Set the raw data path
RAW_DATA_PATH <- "data/data_raw"

# Choose BACI and HS version, and number of digits
baci_version <- "V202501"
HS_version <- "HS22"
HS_digits <- 4

# Choose years
baci_year1 <- 2022
baci_yearT <- 2023

# Import BACI data --------------------------------------------------------
# Select the correct path where the BACI data is stored
baci_data_path <- paste0(
  RAW_DATA_PATH, "/CEPII BACI/BACI_", HS_version, "_", baci_version
)

# Initialise baci_df, a data frame at the cpt-level containing the baci for 
# all years
baci_df <- data.table()

## Loop over all csv files and sum over c' (sum over importers)
for (year in baci_year1:baci_yearT) {
  # Construct the full file path dynamically
  file_path <- paste0(baci_data_path, "/BACI_", HS_version, "_Y", year, "_", 
                      baci_version, ".csv")
  # Check if the file exists before reading
  if (file.exists(file_path)) {
    # Import CSV file using fread and select only required columns
    data <- fread(file_path, select = c(
      "t", "i", "k", "v"
    ), 
    # Ensure "k" (products) is read as character
    colClasses = list(character = "k"))  
    # Rename columns directly in data.table
    setnames(data, 
             old = c("t", "i", "k", "v"), 
             new = c("year", "exporter", "prod_code", "X_cp")
             )
    # Sum both X_cp and Q_cp over year, exporter, prod_code using data.table's 
    # fast aggregation
    data <- data[, .(
      X_cp = sum(X_cp, na.rm = TRUE)
    ), by = .(year, exporter, prod_code)]
    # Append the data efficiently
    baci_df <- rbindlist(list(baci_df, data), use.names = TRUE, fill = TRUE)
    
  } else {
    warning(paste("File not found:", file_path))
  }
}

head(baci_df)
dim(baci_df)

# Aggregate to HS_digits --------------------------------------------------
# Aggregate the X_cp trade-flows to the right nr of digits
# If HS_digits == 4, then aggregate them
HS_digits

if (HS_digits == 4) {
  baci_df <- baci_df %>% 
    # truncate HS product codes to 4-digit level
    mutate(prod_code = substr(prod_code, 1, HS_digits)) %>% 
    group_by(year, exporter, prod_code) %>% 
    summarise(
      X_cp = sum(X_cp, na.rm = TRUE), 
      .groups = "drop"
    )
  message("X_cp aggregated to ", HS_digits, "-digit level")
} else {
  # If HS_digits is 6, then baci_df remains unchanged
  message("HS_digits = ", HS_digits, 
          ", no aggregation is applied")
}

# Import country and product codes ----------------------------------------
country_codes_baci <- read_csv(glue(
  RAW_DATA_PATH, 
  "/CEPII BACI/BACI_{HS_version}_{baci_version}/country_codes_{baci_version}.csv")
)

product_codes_baci <- read_csv(glue(
  RAW_DATA_PATH, 
  "/CEPII BACI/BACI_{HS_version}_{baci_version}/product_codes_{HS_version}_{baci_version}.csv")
)

# Add ISO3 codes to baci_df -----------------------------------------------
baci_df <- baci_df %>% 
  select(-any_of("country_iso3")) %>%  # Remove the existing column if present 
  # to avoid duplicate columns
  left_join(country_codes_baci %>% select(country_code, country_iso3),
            by = c("exporter" = "country_code")) %>% 
  # Reorder columns
  relocate(country_iso3, .after = exporter) %>% 
  # Remove the exporter code
  select(-exporter)

head(baci_df)

# Create cp_df ------------------------------------------------------------
# cp_df is the final country-product-year level dataset
cp_df <- baci_df

# Add product descriptions ------------------------------------------------
# The HS codes in the BACI data are at 6-digits. To add the correct 4-digit
# product codes, we use the UN product codes file which contains the product
# descriptions for all levels

## Import UN HS codes ####
UN_HS_codes <- read_excel(
  paste0(
    RAW_DATA_PATH, 
    "/UN HSCodeandDescription/HSCodeandDescription.xlsx"
  ),
  sheet = HS_version
)

# Select the descriptions for the digits chosen
product_codes_UN <- UN_HS_codes %>% 
  # keep only rows with the correct digits (6 or 4)
  filter(as.integer(Level) == HS_digits) %>%   
  mutate(
    code = Code, 
    description = Description, 
    .keep = "none" # keep only these variables
  )

# Add these descriptions to cp_df
cp_df <- cp_df %>% 
  left_join(product_codes_UN, by = c("prod_code" = "code")) %>% 
  rename(prod_descr = description) %>% 
  relocate(prod_descr, .after = prod_code)

head(cp_df)

# Sample selection --------------------------------------------------------
# Select the sample of countries and products (remove small nodes)
# Optionally, could download population data and filter out countries with
# population sizes smaller than 1000000
tot_exp_c_threshold <- 1000000 # 1 billion USD
tot_exp_p_threshold <- 500000 # 500 million USD
sample_selection_year <- 2022

## Country sample ####
# Select countries with total exports
country_sample <- baci_df %>%
  group_by(country_iso3, year) %>%
  summarise(tot_exp_c = sum(X_cp, na.rm = TRUE), .groups = "drop") %>%
  filter(
    year == sample_selection_year,
    
    # Total exports threshold
    tot_exp_c >= tot_exp_c_threshold,
    
    # Manually excluded countries with unreliable data as done in Jun.etal2020, 
    # Pinheiro.etal2022, ...
    !country_iso3 %in% c(
      "IRQ", 
      "TCD", 
      "MAC", 
      "AFG"
    )
  ) %>%
  
  # Extract the remaining country codes
  pull(country_iso3)

country_sample
length(country_sample) # number of selected countries

## Product sample ####
product_sample <- baci_df %>%
  # Keep only rows for selected countries
  filter(country_iso3 %in% country_sample) %>%
  
  # Compute total exports per product-year (aggregated across countries)
  group_by(year, prod_code) %>%
  mutate(tot_exp_p = sum(X_cp, na.rm = TRUE)) %>%
  ungroup() %>%
  
  # Drop country info and collapse to product-year level
  select(-country_iso3) %>%
  group_by(year, prod_code) %>%
  summarise(across(everything(), first), .groups = "drop") %>%
  
  # Add product description from product code mapping
  left_join(product_codes_baci, by = c("prod_code" = "code")) %>%
  rename(prod_descr = description) %>%
  
  # Filter out the small products
  filter(year == sample_selection_year, 
         tot_exp_p >= tot_exp_p_threshold) %>% 
  
  # Extract the product codes 
  pull(prod_code)

head(product_sample)
length(product_sample) # number of selected products

## Filter out countries and products
dim(cp_df)

cp_df <- cp_df %>% 
  filter(country_iso3 %in% country_sample, 
         prod_code %in% product_sample)

dim(cp_df)

# Compute RCA_cp, s_cp, M_cp ----------------------------------------------
cp_df <- cp_df %>% 
  ## Total exports c ####
  # tot_exp_c = Total exports of c = sum_{p'} X_{cp'}
  group_by(year, country_iso3) %>% 
  mutate(tot_exp_c = sum(X_cp, na.rm = TRUE)) %>% 
  ungroup() %>%
  
  ## Total exports p ####
  # tot_exp_p = World export of p (by all c) = sum_{c} X_{c'p}
  group_by(year, prod_code) %>% 
  mutate(tot_exp_p = sum(X_cp, na.rm = TRUE)) %>% 
  ungroup() %>% 
  
  ## Total world exports
  # tot_world_exp = Total world export of all products = sum_{c'p'}X_{c'p'}
  group_by(year) %>% 
  mutate(tot_world_exp = sum(X_cp, na.rm = TRUE)) %>% 
  ungroup() %>% 
  
  ## RCA_cp ####
  # RCA_{cp} = (X_{cp} / sum_{p'} X_{cp'}) / (sum_{c} X_{c'p} / 
    # sum_{c'p'} X_{c'p'})
  mutate(RCA_cp = (X_cp / tot_exp_c) / (tot_exp_p / tot_world_exp)) %>% 
  
  ## s_cp (Export shares) ####
  # s_cp = X_cp / sum_(p') X_(cp') = X_cp / tot_exp_c
  mutate(s_cp = X_cp / tot_exp_c) %>% 
  
  ## M_cp ####
  # M_cp = 1 if RCA_cp >= 1, = 0 if RCA_cp < 1
  mutate(M_cp = if_else(RCA_cp >= 1, 1, 0)) %>% 

  ## Diversity_c ####
  # diversity_c = sum_p M_cp = the number of RCAs >= 1 a country has
  group_by(year, country_iso3) %>% 
  mutate(diversity_c = sum(M_cp, na.rm = TRUE)) %>% 
  ungroup() %>% 

  ## Ubiquity_p ####
  # ubiquity_p = sum_c M_cp = the number of times p has been exported with an 
  # RCA >= 1
  group_by(year, prod_code) %>% 
  mutate(ubiquity_p = sum(M_cp, na.rm = TRUE)) %>% 
  ungroup()

# ECI & PCI ---------------------------------------------------------------
# Compute ECI & PCI separately for each year

## Subset cp_df for each year ####
baci_year1:baci_yearT # created in baci_import.R

for (year in baci_year1:baci_yearT) {
  assign(paste0("cp_df_", year), cp_df %>% filter(year == !!year))
}

head(cp_df_2022)

## Create a list to store the cp-matrices for each year
cp_mat_list <- list()

## Loop through each year and create the matrices ####
for (year in baci_year1:baci_yearT) {
  # Create the matrix for the current year using the corresponding cp_df
  cp_mat <- get(paste0("cp_df_", year)) %>%
    balassa_index(country = "country_iso3", 
                  product = "prod_code",
                  value = "X_cp", 
                  cutoff = 1, discrete = TRUE)
  
  # Assign the matrix to the corresponding name
  assign(paste0("cp_mat_", year), cp_mat)
  
  # Add the matrix to the list
  cp_mat_list[[year - (baci_year1 - 1)]] <- cp_mat
} 

## Check values
# cp_mat_2017[1:8, 1:8]
# cp_mat_2018[1:8, 1:8]
# cp_mat_2022[1:8, 1:8]

## Compute ECI & PCI ####
for (year in baci_year1:baci_yearT) {
  # Create the complexity measures object for the current year
  compl_object <- complexity_measures(
    get(paste0("cp_mat_", year)), 
    # method = "reflections"
    method = "eigenvalues"
  ) 
  
  # Assign the object to the corresponding name (compl_2007, compl_2008, etc.)
  assign(paste0("compl_", year), compl_object)
}

head(compl_2022$complexity_index_country)
head(compl_2022$complexity_index_product)

## Create eci_df ####
# A data frame containing all the ECI values for all years

# Initialize an empty data frame
eci_df <- data.frame(year = numeric(0), 
                     country_iso3 = character(0), 
                     eci_c = numeric(0))
head(eci_df)

### Loop over all compl_YEAR objects and combine their values into one df
for (year in baci_year1:baci_yearT) {
  compl_object <- get(paste("compl_", year, sep = ""))
  
  year_data <- data.frame(
    year = year,
    country_iso3 = names(compl_object$complexity_index_country),
    eci_c = compl_object$complexity_index_country
  )
  
  eci_df <- rbind(eci_df, year_data)
  
  # Remove the row names
  row.names(eci_df) = NULL
}

head(eci_df)

### Add ECI to cp_df and c_df ####
cp_df <- cp_df %>% 
  left_join(eci_df, by = c("year", "country_iso3"))
head(cp_df)

# Remove the object
rm(eci_df)

## Create pci_df ####
# A data frame containing all the PCI values for all years

# Initialize an empty data frame
pci_df <- data.frame(year = numeric(0), 
                     prod_code = character(0), 
                     pci_p = numeric(0))
head(pci_df)

### Loop over all compl_YEAR objects and combine their PCI values into one df
for (year in baci_year1:baci_yearT) {
  compl_object <- get(paste("compl_", year, sep = ""))
  
  year_data <- data.frame(
    year = year,
    prod_code = names(compl_object$complexity_index_product),
    pci_p = compl_object$complexity_index_product
  )
  
  pci_df <- rbind(pci_df, year_data)
  
  # Remove the row names
  row.names(pci_df) = NULL
}

head(pci_df)

### Add PCI to cp_df and p_df ####
cp_df <- cp_df %>% 
  left_join(pci_df, by = c("year", "prod_code"))
head(cp_df)

# Remove the object
rm(pci_df)

# Relatedness -------------------------------------------------------------
# Compute relatedness metrics: proximities_pp' and densities_cp

## Proximity_pp' ----------------------------------------------------------
# Compute proximities using economiccomplexity::proximity for multiple years
prox_year1 <- baci_year1
prox_yearT <- baci_yearT
prox_years <- prox_year1:prox_yearT

### Loop through each year (over each cp_mat_yyyy object) and compute 
# proximities
for(year in prox_year1:prox_yearT) {
  # Construct the matrix name for the current year
  cp_mat_name <- paste("cp_mat", year, sep = "_")
  
  # Use get() to retrieve the matrix by name
  cp_mat_current <- get(cp_mat_name)
  
  # Calculate proximity for the current matrix
  proximity_current <- economiccomplexity::proximity(cp_mat_current)
  
  # Construct the name for the proximity result, now using 'prox_' as prefix
  proximity_name <- paste("prox", year, sep = "_")
  
  # Use assign() to create a new variable with the calculated proximity
  assign(proximity_name, proximity_current)
  
  # Remove the temporary proximity_current variable to free up memory
  rm(cp_mat_current, proximity_current)
} 

# Check objects that are created
grep(pattern = "prox_\\d{4}", ls(), value = TRUE)

prox_2022$proximity_country[1:5, 1:5]

## Distance_cp ------------------------------------------------------------
# Compute distances_cp between countries and products 

### Loop through each year and compute distances
for(year in prox_year1:prox_yearT) {
  # Construct the prox_yyyy variable name
  prox_var_name <- paste0("prox_", year)
  
  # Construct the cp_mat_yyyy variable name
  cp_mat_var_name <- paste0("cp_mat_", year)
  
  # Use get() to retrieve the prox_yyyy object based on its name
  prox_object <- get(prox_var_name)
  
  # Similarly, retrieve the cp_mat_yyyy object
  cp_mat_object <- get(cp_mat_var_name)
  
  # Calculate the distance using the economiccomplexity::distance function
  dist_object <- economiccomplexity::distance(cp_mat_object, 
                                              prox_object$proximity_product)
  
  # Construct the dist_yyyy variable name
  dist_var_name <- paste0("dist_", year)
  
  # Use assign() to create a new variable with the name stored in dist_var_name 
  # and assign the calculated dist_object to it
  assign(dist_var_name, dist_object)
}

# Check the distance matrices that were created
grep(pattern = "dist_\\d{4}", ls(), value = TRUE)

dist_2022[1:5, 1:5]

# Remove the prox_yyyy matrices to save memory
rm(list = grep("prox_\\d{4}", ls(), value = TRUE))

## Combine distances into a data frame
### Initialize an empty data frame for storing all dist_yyyy data
dist_df <- data.frame()

### Loop through each dist matrix name, convert to long format, and combine
for (matrix_name in grep("dist_\\d{4}", ls(), value = TRUE)) {
  # Extract the year from the matrix name
  year <- gsub("dist_", "", matrix_name)
  
  # Retrieve the matrix from the name
  matrix <- get(matrix_name)
  
  # Convert the matrix to a data frame in long format
  temp_df <- as.data.frame(as.table(as.matrix(matrix)))
  
  # Rename columns for clarity
  colnames(temp_df) <- c("country_iso3", "prod_code", "dist_cp")
  
  # Add the year column
  temp_df$year <- year
  
  # Combine with the main data frame
  dist_df <- bind_rows(dist_df, temp_df)
}

rm(temp_df)

### Convert the year to numeric and reorder columns
dist_df <- dist_df %>%
  mutate(year = as.numeric(year)) %>%
  select(country_iso3, prod_code, year, everything()) 

head(dist_df) 

### Merge dist_df with cp_df
cp_df <- left_join(cp_df, dist_df, 
                   by = c("year", "country_iso3", "prod_code"))
head(cp_df)
dim(cp_df) 

# Remove dist_df to save memory
rm(dist_df)

## Density_cp -------------------------------------------------------------
# Relatedness density = 1 - distance = "closeness"
cp_df <- cp_df %>% 
  # Compute relatedness density (dens_cp)
  mutate(dens_cp = 1 - dist_cp) %>% 
  # Remove dist_cp to save memory
  select(-dist_cp)

# Complexity Outlook ------------------------------------------------------
# Try to use the economiccomplexity::complexity_outlook() function to compute
# the complexity outlook index for countries, and the complexity outlook gain
# for country-product pairs

# Export final dataset ----------------------------------------------------
dim(cp_df)
head(cp_df)

write.csv(
  cp_df, 
  file = "data/data_processed/cp_df.csv", 
  row.names = FALSE
)
