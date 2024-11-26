library(sf)
library(dplyr)
library(tidyr)
library(lubridate)
setwd("C:\\Users\\jayde\\Documents\\climate data\\Flood")

school2014 <- read.csv("C:\\Users\\jayde\\Documents\\climate data\\pccf_nov2014_JCgeocoded\\pccf_nov2014_fccp\\data\\pccf_school2014_20240612.csv") %>%
  filter(!is.na(LONG) & !is.na(LAT)) %>%
  mutate(survey_month = as.Date(paste(year, month, "01", sep = "-"), format = "%Y-%m-%d"))

school2018 <- read.csv("C:\\Users\\jayde\\Documents\\climate data\\schoollocation\\pccf_schools2018withdates.csv") %>%
  filter(!is.na(LONG) & !is.na(LAT)) %>%
  mutate(survey_month = as.Date(paste(year, month, "01", sep = "-"), format = "%Y-%m-%d"))

# Create flood month column
floodintersect <- read.csv("intersect.csv")
floodintersect <- floodintersect %>%
  mutate(flood_month = substr(date_utc,1,10))
floodintersect$flood_month <- as.Date(floodintersect$flood_month, format = "%Y/%m/%d")

# subset flood data (by school survey year)
flood2014 <- floodintersect %>%
  filter(year == 2013 | year == 2014 | year == 2015)
flood2018 <- floodintersect %>% 
  filter(year == 2018)

# Filter based on date range (fires) and drop geometry (three floods)
flood2014 <- flood2014 %>%
  filter(flood_month >= date1ya & flood_month <= date) %>%
  st_drop_geometry(filterjoin)

flood2018 <- flood2018 %>%
  filter(flood_month >= date1ya & flood_month <= date) %>%
  st_drop_geometry(filterjoin)

# all the fire events filtered out base on the date range are in 2013 and 2014
# Group and count fire events by school and month
# if firemonth is 2017 extract month number, if 2018 +12
flood_count_2018100 <- flood2014 %>%
  group_by(schoolid, date, flood_month) %>%  # Group by schoolid, survey_month (date), and fire_month
  summarise(count = n(), .groups = "drop") %>% # Summarize to get counts and drop grouping afterwards
  mutate(floodmonth = ifelse(grepl("2013", flood_month), substr(flood_month, 6, 7),
                            ifelse(grepl("2014", flood_month), as.character(as.numeric(substr(flood_month, 6, 7)) + 12), NA)))

flood_count_2018100 <- flood2018 %>%
  group_by(schoolid, date, flood_month) %>%  # Group by schoolid, survey_month (date), and fire_month
  summarise(count = n(), .groups = "drop") %>% # Summarize to get counts and drop grouping afterwards
  mutate(floodmonth = ifelse(grepl("2017", flood_month), substr(flood_month, 6, 7),
                             ifelse(grepl("2018", flood_month), as.character(as.numeric(substr(flood_month, 6, 7)) + 12), NA)))

# remove leading zeros from firemonth
flood_count_2018100$floodmonth <- sub("^0+", "", flood_count_2018100$floodmonth)


# Pivot data to wide format
f_2018100 <- flood_count_2018100 %>%
  pivot_wider(names_from = floodmonth, values_from = count, values_fill = 0)

f_2018100 <- f_2018100 %>% select(-flood_month)

f_2018100 <- f_2018100 %>%
  group_by(schoolid, date) %>%
  summarise(across(where(is.numeric), sum, na.rm = TRUE), .groups = 'drop')

# Create a complete set of month columns from 1 to 24
month_cols <- as.character(1:24)

# Check for missing columns and add them filled with zeros
missing_cols <- setdiff(month_cols, names(f_2018100))
for (col in missing_cols) {
  f_2018100[[col]] <- 0
}

# Add start and end months
f_2018100 <- f_2018100 %>%
  mutate(end_month = as.numeric(sub("^0+", "", substr(date, 6, 7))) + 12,
         start_month = end_month - 11)
# Ensure the columns are ordered numerically
f_2018100 <- f_2018100 %>%
  select(order(as.numeric(colnames(.))))

# Create an empty data frame with 12 columns
new_data <- as.data.frame(matrix(0, nrow = 0, ncol = 12))
names(new_data) <- paste("FLOOD_", 11:0, sep = "")


# Loop through each row of the dataset f_2018100
for (i in 1:nrow(f_2018100)) {
  # Calculate the range of columns to extract based on start_month and end_month
  column_range <- (f_2018100$start_month[i]):(f_2018100$end_month[i])
  # Extract the row with the specified column range
  extracted_row <- f_2018100[i, column_range, drop = FALSE]
  # Set the names of the extracted_row to match those of new_data
  names(extracted_row) <- names(new_data)[1:ncol(extracted_row)]
  # Append the extracted row to new_data
  if (ncol(extracted_row) == 12) {
    new_data <- rbind(new_data, extracted_row)
  } else {
    # Optionally handle cases where extracted rows do not match the expected column count
    warning("Row ", i, " does not match the expected number of columns and will be skipped.")
  }
}


# Add schoolid and survey_month
new_data2014 <- new_data %>%
  mutate(schoolid = f_2018100$schoolid,
         survey_month = f_2018100$date)

new_data2018 <- new_data %>%
  mutate(schoolid = f_2018100$schoolid,
         survey_month = f_2018100$date)

# Merge school data and new_data

school2018$survey_month <- as.character(school2018$survey_month)
school2018 <- school2018 %>%
  select(-"X")

flood_2018 <- school2018 %>%
  left_join(new_data2018, by = c("schoolid", "survey_month")) %>%
  replace(is.na(.), 0)

school2014$survey_month <- as.character(school2014$survey_month)
school2014 <- school2014 %>%
  select(-"X") %>%
  rename(schoolid = id2)

flood_2014 <- school2014 %>%
  left_join(new_data2014, by = c("schoolid", "survey_month")) %>%
  replace(is.na(.), 0)

write.csv(flood_2018, "flood_2018.csv")

write.csv(flood_2014, "flood_2014.csv")
