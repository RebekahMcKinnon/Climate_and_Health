library(sf)
library(dplyr)
library(tidyr)
library(lubridate)
setwd("C:\\Users\\jayde\\Documents\\climate data")

# Load and subset fire data
fire2018 <- st_read("NFDB_poly/NFDB_poly_20210707.shp")
fire2018 <- fire2018 %>% filter(YEAR == 2012 | YEAR == 2013 | YEAR == 2014 | YEAR == 2015)

# Create fire month column
fire2018 <- fire2018 %>%
  mutate(fire_month = as.Date(paste(YEAR, MONTH, "01", sep = "-"), format = "%Y-%m-%d"))

largefire<- fire2018 %>%
  filter(SIZE_HA > 200)

# Load and clean school data
school2018 <- read.csv("C:\\Users\\jayde\\Documents\\climate data\\pccf_nov2014_JCgeocoded\\pccf_nov2014_fccp\\data\\pccf_school2014_20240612.csv") %>%
  filter(!is.na(LONG) & !is.na(LAT)) %>%
  st_as_sf(coords = c("LONG", "LAT"), crs = 4269) %>%
  st_transform(crs = st_crs(fire2018))


# Add survey_month column
school2018 <- school2018 %>%
  mutate(survey_month = as.Date(paste(year, month, "01", sep = "-"), format = "%Y-%m-%d"),
         daterange = survey_month %m-% months(12)) # daterange is basically end_month
school2018 <- school2018 %>%
  select(-"X") %>%
  rename(schoolid = id2)

# Buffering operations
buffer2018_100 <- st_buffer(school2018, dist = 25000)


# Join fire and school data
# join2018100 <- st_intersection(buffer2018_100, fire2018)
# largefire
join2018100 <- st_intersection(buffer2018_100, largefire)

# Filter based on date range (fires) and drop geometry
filterjoin <- join2018100 %>%
  filter(REP_DATE >= daterange & REP_DATE <= survey_month) %>%
  st_drop_geometry(filterjoin)

# all the fire events filtered out base on the date range are in 2013 and 2014
# Group and count fire events by school and month
# if firemonth is 2017 extract month number, if 2018 +12
fire_count_2018100 <- filterjoin %>%
  group_by(schoolid, survey_month, fire_month) %>%  # Group by schoolid, survey_month, and fire_month
  summarise(count = n(), .groups = "drop") %>% # Summarize to get counts and drop grouping afterwards
  mutate(firemonth = ifelse(grepl("2013", fire_month), substr(fire_month, 6, 7),
                            ifelse(grepl("2014", fire_month), as.character(as.numeric(substr(fire_month, 6, 7)) + 12), NA)))


# remove leading zeros from firemonth
fire_count_2018100$firemonth <- sub("^0+", "", fire_count_2018100$firemonth)


# Pivot data to wide format
f_2018100 <- fire_count_2018100 %>%
  pivot_wider(names_from = firemonth, values_from = count, values_fill = 0)
# f_2018100 <- f_2018100 %>%
# select(order(as.numeric(colnames(.))))

# after this point we dont need firemonth anymore 
# we need to add values of fires vertically of data entries with the same school id and survey month
# get rid of fire_month, and if a row has same schoolid and survey month, add the values of all columns numerically for those rows 
f_2018100 <- f_2018100 %>% select(-fire_month)

f_2018100 <- f_2018100 %>%
  group_by(schoolid, survey_month) %>%
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
  mutate(end_month = as.numeric(sub("^0+", "", substr(survey_month, 6, 7))) + 12,
         start_month = end_month - 11)
# Ensure the columns are ordered numerically
f_2018100 <- f_2018100 %>%
  select(order(as.numeric(colnames(.))))

# Create an empty data frame with 12 columns
new_data <- as.data.frame(matrix(0, nrow = 0, ncol = 12))
names(new_data) <- paste("LARGE25_", 11:0, sep = "")


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
new_data <- new_data %>%
  mutate(schoolid = f_2018100$schoolid,
         survey_month = f_2018100$survey_month)

# Merge school data and new_data
school201825merge <- school2018 %>%
  st_drop_geometry() %>%
  select(schoolid, survey_month) %>%
  left_join(new_data, by = c("schoolid", "survey_month")) %>%
  replace(is.na(.), 0)

combined_data <- school2018 %>%
  full_join(school201825merge, by = c("schoolid", "survey_month")) %>%
  full_join(school201850merge, by = c("schoolid", "survey_month")) %>%
  full_join(school2018100merge, by = c("schoolid", "survey_month")) %>%
  full_join(school2018200merge, by = c("schoolid", "survey_month"))

## Replace "large" with "fire" in column names
# names(combined_data) <- gsub("LARGE", "FIRE", names(combined_data))

#combined_data$LargeExposure <- apply(combined_data, 1, function(x) if(any(x > 0)) "Y" else "N")

write.csv(combined_data, "schoollargefire2014_20240613.csv")

write.csv(new_data, "test.csv")