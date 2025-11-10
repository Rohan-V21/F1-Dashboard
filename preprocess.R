base_dir <- "C:/Users/rohan/Downloads"
constructors_dir <- file.path(base_dir, "constructors")
races_dir       <- file.path(base_dir, "races")
processed_dir   <- file.path(base_dir, "processed")

if(!dir.exists(processed_dir)) dir.create(processed_dir, recursive = TRUE)

circuit_path        <- file.path(base_dir, "circuit.csv")
driver_record_path  <- file.path(base_dir, "driver_record.csv")
team_record_path    <- file.path(base_dir, "team_record.csv")

pkgs <- c(
  "tidyverse","readr","lubridate","janitor",
  "tidygeocoder","leaflet","sf",
  "DT","plotly","shiny","bslib","shinyWidgets"
)
new_pkgs <- pkgs[!(pkgs %in% installed.packages()[,"Package"])]
if(length(new_pkgs)) install.packages(new_pkgs, dependencies = TRUE)

library(tidyverse)
library(readr)
library(lubridate)
library(janitor)
library(tidygeocoder)

read_yearly_csvs <- function(dirpath){
  files <- list.files(dirpath, pattern = "^[0-9]{4}\\.csv$", full.names = TRUE)
  tibble(file = files) |>
    mutate(year = as.integer(gsub("^(.*)([0-9]{4})\\.csv$","\\2", file))) |>
    arrange(year)
}

constructors_index <- read_yearly_csvs(constructors_dir)
races_index        <- read_yearly_csvs(races_dir)

constructors_all <- purrr::map2_dfr(
  constructors_index$file, constructors_index$year,
  ~ read_csv(.x, show_col_types = FALSE) |>
    clean_names() |>
    mutate(year = .y) |>
    mutate(
      team   = as.character(team),
      win    = suppressWarnings(as.numeric(win)),
      podium = suppressWarnings(as.numeric(podium))
    ) |>
    select(year, team, win, podium)
)

races_all <- purrr::map2_dfr(
  races_index$file, races_index$year,
  ~ read_csv(.x, show_col_types = FALSE) |>
    clean_names() |>
    rename(winner_driver = win, pole_driver = pole) |>
    mutate(year = .y)
)

races_master <- races_all |>
  mutate(
    date_str    = paste0(date, "-", year),  # "DD-Mon-YYYY"
    date_parsed = suppressWarnings(parse_date_time(date_str, orders = "d-b-Y", tz = "UTC"))
  ) |>
  transmute(
    year, date, date_parsed, title, circuit, winner_driver, pole_driver,
    winner_team = "Not listed" 
  )

write_csv(races_master,     file.path(processed_dir, "races_master.csv"))
write_csv(constructors_all, file.path(processed_dir, "constructors_all.csv"))

if(file.exists(driver_record_path)) {
  driver_record <- read_csv(driver_record_path, show_col_types = FALSE) |> clean_names()
  write_csv(driver_record, file.path(processed_dir, "driver_record_clean.csv"))
}
if(file.exists(team_record_path)) {
  team_record <- read_csv(team_record_path, show_col_types = FALSE) |> clean_names()
  write_csv(team_record, file.path(processed_dir, "team_record_clean.csv"))
}

if(file.exists(circuit_path)) {
  circuits_raw <- read_csv(circuit_path, show_col_types = FALSE) |>
    clean_names()
  
  if(!("location" %in% names(circuits_raw))) stop("Add a 'location' column to circuit.csv")
  
  circuits_unique <- circuits_raw |>
    distinct(circuit, location)
  
  circuits_geocoded <- circuits_unique |>
    geocode(address = location, method = "osm", lat = latitude, long = longitude, limit = 1)
  
  race_counts <- races_master |>
    count(circuit, name = "race_count")
  
  circuits_map_ready <- circuits_geocoded |>
    left_join(race_counts, by = "circuit") |>
    mutate(race_count = replace_na(race_count, 0L)) |>
    rename(lat = latitude, lon = longitude)
  
  write_csv(circuits_map_ready, file.path(processed_dir, "circuits_map_ready.csv"))
}

top25_drivers <- races_master |>
  count(winner_driver, name = "wins") |>
  arrange(desc(wins)) |>
  slice_head(n = 25)

write_csv(top25_drivers, file.path(processed_dir, "top25_drivers.csv"))

dir(processed_dir, full.names = TRUE)

if(!requireNamespace("DBI", quietly = TRUE)) install.packages("DBI", dependencies = TRUE)
if(!requireNamespace("RSQLite", quietly = TRUE)) install.packages("RSQLite", dependencies = TRUE)
library(DBI)
library(RSQLite)

db_path <- file.path(processed_dir, "f1.sqlite")
con <- dbConnect(SQLite(), db_path)

dbExecute(con, "CREATE TABLE IF NOT EXISTS races_master (
  year INTEGER,
  date TEXT,
  date_parsed TEXT,
  title TEXT,
  circuit TEXT,
  winner_driver TEXT,
  pole_driver TEXT,
  winner_team TEXT
)")

dbExecute(con, "CREATE TABLE IF NOT EXISTS constructors_all (
  year INTEGER,
  team TEXT,
  win REAL,
  podium REAL
)")

dbExecute(con, "CREATE TABLE IF NOT EXISTS circuits_map_ready (
  circuit TEXT,
  location TEXT,
  lat REAL,
  lon REAL,
  race_count INTEGER
)")

dbExecute(con, "CREATE TABLE IF NOT EXISTS top25_drivers (
  winner_driver TEXT,
  wins INTEGER
)")

dbExecute(con, "DELETE FROM races_master")
dbExecute(con, "DELETE FROM constructors_all")
dbExecute(con, "DELETE FROM circuits_map_ready")
dbExecute(con, "DELETE FROM top25_drivers")

rm_df <- races_master %>%
  mutate(
    year = as.integer(year),
    date = as.character(date),
    date_parsed = as.character(date_parsed),
    title = as.character(title),
    circuit = as.character(circuit),
    winner_driver = as.character(winner_driver),
    pole_driver = as.character(pole_driver),
    winner_team = as.character(winner_team)
  )

ca_df <- constructors_all %>%
  mutate(
    year = as.integer(year),
    team = as.character(team),
    win = as.numeric(win),
    podium = as.numeric(podium)
  )

cmr_df <- if (exists("circuits_map_ready") && !is.null(circuits_map_ready)) {
  circuits_map_ready %>%
    transmute(
      circuit = as.character(circuit),
      location = as.character(location),
      lat = as.numeric(lat),
      lon = as.numeric(lon),
      race_count = as.integer(race_count)
    )
} else {
  tibble(circuit = character(), location = character(), lat = numeric(), lon = numeric(), race_count = integer())
}

t25_df <- top25_drivers %>%
  transmute(winner_driver = as.character(winner_driver), wins = as.integer(wins))

dbWriteTable(con, "races_master", rm_df, append = TRUE)
dbWriteTable(con, "constructors_all", ca_df, append = TRUE)
dbWriteTable(con, "circuits_map_ready", cmr_df, append = TRUE)
dbWriteTable(con, "top25_drivers", t25_df, append = TRUE)
# write driver/team records into SQLite if available
drv_csv <- file.path(processed_dir, "driver_record_clean.csv")
tm_csv  <- file.path(processed_dir, "team_record_clean.csv")

drv_df <- if (file.exists(drv_csv)) readr::read_csv(drv_csv, show_col_types = FALSE) %>% janitor::clean_names() else tibble::tibble()
tm_df  <- if (file.exists(tm_csv))  readr::read_csv(tm_csv,  show_col_types = FALSE) %>% janitor::clean_names() else tibble::tibble()

if (nrow(drv_df) > 0) DBI::dbWriteTable(con, "driver_record", drv_df, overwrite = TRUE)
if (nrow(tm_df)  > 0) DBI::dbWriteTable(con, "team_record",  tm_df,  overwrite = TRUE)

dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_races_year ON races_master(year)")
dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_races_circuit ON races_master(circuit)")
dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_cons_year ON constructors_all(year)")
dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_cons_team ON constructors_all(team)")

demo_counts <- dbGetQuery(con, "SELECT 
  (SELECT COUNT(*) FROM races_master) AS races_n,
  (SELECT COUNT(*) FROM constructors_all) AS constructors_n,
  (SELECT COUNT(*) FROM circuits_map_ready) AS circuits_n,
  (SELECT COUNT(*) FROM top25_drivers) AS top25_n
")

demo_sample_races <- dbGetQuery(con, "SELECT year, date_parsed, circuit, winner_driver FROM races_master ORDER BY year, date_parsed LIMIT 5")

demo_top_teams <- dbGetQuery(con, "SELECT team, SUM(win) AS wins FROM constructors_all GROUP BY team ORDER BY wins DESC LIMIT 10")

print(demo_counts)
print(demo_sample_races)
print(demo_top_teams)

dbDisconnect(con)
#------------------------------------------------------------
#Data storage and retrieval demonstration
library(DBI)
library(RSQLite)
library(dplyr)

db_path <- "C:/Users/rohan/Downloads/processed/f1.sqlite"
con <- dbConnect(SQLite(), db_path)

dbExecute(con, "
  CREATE TABLE IF NOT EXISTS races_master (
    year INTEGER,
    date TEXT,
    circuit TEXT,
    winner_driver TEXT,
    pole_driver TEXT
  )
")
dbExecute(con, "DELETE FROM races_master")   

races_master_clean <- races_master %>%
  mutate(
    year = as.integer(year),
    date = as.character(date),
    circuit = as.character(circuit),
    winner_driver = as.character(winner_driver),
    pole_driver = as.character(pole_driver)
  )

dbWriteTable(con, "races_master", races_master_clean, append = TRUE)
all_races <- dbGetQuery(con, "SELECT * FROM races_master")
head(all_races)

races_2022 <- dbGetQuery(con, "
  SELECT year, circuit, winner_driver
  FROM races_master
  WHERE year = 2022
")
races_2022
dbDisconnect(con)

library(DBI)
library(RSQLite)

con <- dbConnect(SQLite(), "C:/Users/rohan/Downloads/processed/f1.sqlite")

View(dbReadTable(con, "constructors_all"))   


