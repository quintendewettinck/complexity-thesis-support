# Data processing

# Clear the environment ---------------------------------------------------
rm(list=ls())

# Packages ----------------------------------------------------------------
library(dplyr)
library(data.table)
library(glue)

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
             old = c("t", "i", "k", "v"
             ), 
             new = c("year", "exporter", "prod_code", "X_cp"
             ))
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