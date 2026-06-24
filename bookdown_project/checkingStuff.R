pa_fire <- read.csv(r"(./data/fires_presence_absence.csv)", row.names=1)
pa_hurricanes <- read.csv(r"(./data/hurricane_presence_absence.csv)", row.names=1)
pa_flood <- read.csv(r"(./data/flood_presence_absence.csv)", row.names=1)
pa_heat_eccc <- read.csv(r"(./data/heat_matrix_eccc.csv)", row.names=1)
pa_heat_threshold <- read.csv(r"(./data/heat_matrix_dayCriteria.csv)", row.names=1)

get_hospital_ids_with_1 <- function(df) {
  month_cols <- paste0("month_", 0:24)
  
  df %>%
    filter(rowSums(across(all_of(month_cols), ~ .x == 1), na.rm = TRUE) > 0) %>%
    distinct(hospital_id)
}

fire_hospital_ids <- get_hospital_ids_with_1(pa_fire)
#hurricane_hospital_ids <- get_hospital_ids_with_1(pa_hurricanes)
flood_hospital_ids <- get_hospital_ids_with_1(pa_flood)
heat_eccc_hospital_ids <- get_hospital_ids_with_1(pa_heat_eccc)
heat_head_hospital_ids <- get_hospital_ids_with_1(pa_heat_threshold)

common_ids <- Reduce(intersect, list(
  fire_hospital_ids$hospital_id,
  hurricane_hospital_ids$survey_id,
  flood_hospital_ids$hospital_id,
  heat_eccc_hospital_ids$hospital_id,
  heat_head_hospital_ids$hospital_id
))







library(dplyr)
library(tidyr)

get_hospital_ids_with_1 <- function(df) {
  month_cols <- paste0("month_", 0:24)
  
  df %>%
    filter(rowSums(across(all_of(month_cols), ~ .x == 1), na.rm = TRUE) > 0) %>%
    distinct(hospital_id)
}

fire_hospital_ids <- get_hospital_ids_with_1(pa_fire)$hospital_id
hurricane_hospital_ids <- get_hospital_ids_with_1(pa_hurricanes)$hospital_id
flood_hospital_ids <- get_hospital_ids_with_1(pa_flood)$hospital_id
heat_eccc_hospital_ids <- get_hospital_ids_with_1(pa_heat_eccc)$hospital_id
heat_head_hospital_ids <- get_hospital_ids_with_1(pa_heat_threshold)$hospital_id

# Put all IDs in one table with dataset labels
all_hospitals <- bind_rows(
  tibble(hospital_id = fire_hospital_ids, dataset = "fire"),
  tibble(hospital_id = hurricane_hospital_ids, dataset = "hurricane"),
  tibble(hospital_id = flood_hospital_ids, dataset = "flood"),
  tibble(hospital_id = heat_eccc_hospital_ids, dataset = "heat_eccc"),
  tibble(hospital_id = heat_head_hospital_ids, dataset = "heat_head")
)

# Count how many datasets each hospital_id appears in, and which datasets
hospitals_in_4 <- all_hospitals %>%
  distinct(hospital_id, dataset) %>%
  group_by(hospital_id) %>%
  summarise(
    n_datasets = n(),
    datasets_list = list(dataset),  # which datasets
    datasets_str = paste(dataset, collapse = ", "),
    .groups = "drop"
  ) %>%
  filter(n_datasets == 4)

hospitals_in_4







library(dplyr)

get_hospital_ids_with_1 <- function(df) {
  month_cols <- paste0("month_", 0:24)
  
  df %>%
    filter(rowSums(across(all_of(month_cols), ~ .x == 1), na.rm = TRUE) > 0) %>%
    distinct(hospital_id)
}

fire_hospital_ids <- get_hospital_ids_with_1(pa_fire)$hospital_id
hurricane_hospital_ids <- get_hospital_ids_with_1(pa_hurricanes)$hospital_id
flood_hospital_ids <- get_hospital_ids_with_1(pa_flood)$hospital_id
heat_eccc_hospital_ids <- get_hospital_ids_with_1(pa_heat_eccc)$hospital_id
heat_head_hospital_ids <- get_hospital_ids_with_1(pa_heat_threshold)$hospital_id

# All IDs with dataset labels
all_hospitals <- bind_rows(
  tibble(hospital_id = fire_hospital_ids, dataset = "fire"),
  tibble(hospital_id = hurricane_hospital_ids, dataset = "hurricane"),
  tibble(hospital_id = flood_hospital_ids, dataset = "flood"),
  tibble(hospital_id = heat_eccc_hospital_ids, dataset = "heat_eccc"),
  tibble(hospital_id = heat_head_hospital_ids, dataset = "heat_head")
)

# Hospitals in hurricanes plus at least one other dataset
hosp_in_hurricanes_plus_other <- all_hospitals %>%
  filter(dataset == "hurricane") %>%
  mutate(hospital_id = as.character(hospital_id)) %>%
  group_by(hospital_id) %>%
  summarise(
    n_datasets = n(),
    datasets = paste(dataset, collapse = ", "),
    .groups = "drop"
  ) %>%
  filter(n_datasets >= 2)

hosp_in_hurricanes_plus_other