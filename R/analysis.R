library(tidyverse)
library(sf)
library(readxl)
library(pool)
library(odbc)
library(DBI)

# Read in shape files -----------------------------------------------------

# localities - from https://cityobservatory.birmingham.gov.uk/explore/dataset/boundaries-birmingham-and-solihull-nhs-proxy-locality/information
locality_boundaries <- read_sf("data/boundaries-locality-bsol.geojson") |> 
  filter(locality != "Solihull")

# wards - from https://cityobservatory.birmingham.gov.uk/explore/dataset/boundaries-wards-2025-wmca/information
ward_boundaries <- read_sf("data/boundaries-wards-2025-wmca.geojson") |> 
  filter(lad25nm == "Birmingham")


# Read in DWP data --------------------------------------------------------

# first define col names - sheet ones are messy
dwp_col_names = c("ward", "local_authority", "claimant_count", "claimant_pct_16_64",
                  "claimant_count_18_24", "health_condition_count", "health_condition_count_18_24",
                  "inactive_claimant_count", "inactive_claimant_count_18_24", "claimant_count_50plus",
                  "long_term_unemployed_count", "long_term_unemployed_count_18_24", "pip_count_16_64",
                  "pip_count_16_64_mh", "pip_count_16_24_mh", "ward_imd")

# import data with new col names and filter to just Birmingham
dwp_data <- read_excel("data/Priority wards WMCA - March 2026.xlsx",
                       skip = 5,
                       col_names = dwp_col_names) |> 
  filter(local_authority == "Birmingham")


# Get 2024 population estimates from SQL ---------------------------------------

# create connection to SQL server
conpool <- pool::dbPool(drv = odbc::odbc(), Driver="SQL Server", Server="SVWVSQ016\\SVWVSQ016", Database="PH_Evelyn", Trusted_Connection="True")           # ie setting up an ODBC connection to SQL Server.

# Create a string variable containing SQL string
str_SQL_Code <- "SELECT [MidYear] AS mid_year
      ,[2018 Ward Code] AS ward_code
      ,[2018 Ward Name] AS ward_name
      ,[Sex] AS sex
      ,[Age Group] AS age_group
      ,[2018WardPopulation] AS population
  FROM [PH_LookUps].[dbo].[tblONS_MidYear_PopulationEstimates_2018Wards_SingleAges_2011_to_2024]
  WHERE MidYear = '2024' AND Sex = '3'"

# get data
pop_est <- DBI::dbGetQuery(conpool, str_SQL_Code)

# filter out 'all ages' and convert age to number
pop_est <- pop_est |> 
  filter(age_group != "All Ages") |> 
  mutate(age_group = as.numeric(age_group))

# summarise to get relevant age bandings - 16-64, 16-24 18-24, 50-64 --------

# set up empty df
pop_est_grouped <- data.frame()

# use for loop to iterate through age bands
for(age_band in c("16-64", "16-24", "18-24", "50-64")) {
  # get upper and lower limits of age band
  age_range <- str_split(age_band, pattern = "-")
  age_lower <- as.numeric(age_range[[1]][1])
  age_upper <- as.numeric(age_range[[1]][2])
  
  # filter and summarise
  temp <- pop_est |> 
    filter(between(age_group, age_lower, age_upper)) |> 
    group_by(ward_code, ward_name) |> 
    summarise(population = sum(population)) |> 
    mutate(age_band = age_band)
  
  # append to prepared df
  pop_est_grouped <- rbind(pop_est_grouped,
                           temp)
}

# pivot wider so there is one ward per row
pop_est_grouped <- pop_est_grouped |> 
  mutate(age_band = paste("count", age_band,
                          sep = "_")) |> 
  pivot_wider(names_from = "age_band",
              values_from = "population")

# trim white space at end of ward names
pop_est_grouped <- pop_est_grouped |> 
  mutate(ward_name = str_trim(ward_name))

# Get best fit of wards to localities -------------------------------------

locality_wards_full <- data.frame()
locality_wards <- data.frame()

sf_use_s2(FALSE)

localities <- locality_boundaries$locality

for (l in localities) {
  # clip wards inside district boundary
  temp_locality_wards <- locality_boundaries |> 
    filter(locality == l)|> 
    st_intersection(ward_boundaries) |> 
    mutate(locality = l)
  
  # find out the area of each clipped ward
  temp_locality_wards$area <- st_area(temp_locality_wards)
  
  # pull out unclipped wards for this locality
  temp_locality_wards_full <- ward_boundaries |> 
    filter(wd25cd %in% temp_locality_wards$wd25cd) |> 
    mutate(locality = l)
  
  # find area of unclipped ward and add to clipped ward sf
  temp_locality_wards$full_area <- st_area(temp_locality_wards_full)
  
  # calulate what % of each ward is inside the boundary
  temp_locality_wards <- temp_locality_wards |> 
    mutate(pct = area/full_area*100)
  
  # # recalculate unclipped wards now wards with <1% are excluded
  # temp_locality_wards_full <- ward_boundaries |> 
  #   filter(wd25cd %in% temp_locality_wards$wd25cd) |> 
  #   mutate(locality = l)
  
  # append temp dfs to main df
  locality_wards_full <- rbind(locality_wards_full,
                               temp_locality_wards_full)
  
  locality_wards <- rbind(locality_wards,
                          temp_locality_wards)
}

# use this to see which district a ward "belongs" to in terms of greatest percent inside that district
locality_wards_best_fit <- locality_wards |> 
  as.data.frame() |> 
  select(wd25nm, locality, pct) |> 
  arrange(desc(pct)) |> 
  distinct(wd25nm,
           .keep_all = T)


# Join DWP data, pop ests and locality best fit ---------------------------

dwp_data <- dwp_data |> 
  left_join(pop_est_grouped,
            by = join_by("ward" == "ward_name")) |> 
  left_join(locality_wards_best_fit,
            by = join_by("ward" == "wd25nm"))


# Summarise by locality ---------------------------------------------------

# use across() to make it quicker to summarise multiple cols
dwp_data_locality <- dwp_data |> 
  group_by(locality) |> 
  summarise(across(contains("count"), sum, .names = "{.col}"))

# # then pivot DWP counts longer to make them easier to work with
# dwp_data_locality <- dwp_data_locality |> 
#   pivot_longer(cols = claimant_count:pip_count_16_24_mh,
#                names_to = "metric",
#                values_to = "count")

# calculate rates and pcts
dwp_data_locality |> 
  mutate(claimant_rate = claimant_count/`count_16-64`*1000,
         claimant_rate_18_24 = claimant_count_18_24/`count_18-24`*1000,
         health_condition_rate = health_condition_count/`count_16-64`*1000,
         health_condition_pct = health_condition_count/claimant_count*100,
         health_condition_rate_18_24 = health_condition_count_18_24/`count_18-24`*1000
         health_condition_pct_18_24 = health_condition_count_18_24/claimant_count_18_24*100) |> 
  select(locality, claimant_rate)
