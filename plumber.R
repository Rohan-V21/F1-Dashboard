# plumber.R
# Run with: plumber::pr("plumber.R") |> pr_run(port = 8000)

if (!requireNamespace("plumber", quietly = TRUE)) install.packages("plumber")
if (!requireNamespace("DBI", quietly = TRUE)) install.packages("DBI")
if (!requireNamespace("RSQLite", quietly = TRUE)) install.packages("RSQLite")

library(plumber)
library(DBI)
library(RSQLite)
library(jsonlite)

# Use env var if set, otherwise default to your processed path
db_path <- Sys.getenv("F1_DB_PATH",
                      unset = "C:/Users/rohan/Downloads/processed/f1.sqlite")

# Small helper to run a read query safely
db_read <- function(sql, params = list()) {
  con <- dbConnect(SQLite(), db_path)
  on.exit(dbDisconnect(con), add = TRUE)
  if (length(params)) {
    rs <- dbSendQuery(con, sql)
    on.exit(dbClearResult(rs), add = TRUE)
    dbBind(rs, params)
    out <- dbFetch(rs)
  } else {
    out <- dbGetQuery(con, sql)
  }
  out
}

# Small helper to run a write/DDL safely
db_exec <- function(sql, params = list()) {
  con <- dbConnect(SQLite(), db_path)
  on.exit(dbDisconnect(con), add = TRUE)
  if (length(params)) DBI::dbExecute(con, sql, params = params) else DBI::dbExecute(con, sql)
}

# --- OPTIONAL: a tiny version table to help Shiny detect changes ---
db_exec("CREATE TABLE IF NOT EXISTS _version (id INTEGER PRIMARY KEY, ts TEXT)")
if (nrow(db_read("SELECT COUNT(*) as n FROM _version")) == 0) {
  db_exec("INSERT INTO _version(ts) VALUES(datetime('now'))")
}

#* Health check
#* @get /ping
function() list(status = "ok", db = normalizePath(db_path, winslash = "/", mustWork = FALSE))

#* Get data from a table
#* @param table:string Table name (e.g., races_master, constructors_all, circuits_map_ready, top25_drivers)
#* @get /getData
function(table = "races_master") {
  allowed <- c("races_master","constructors_all","circuits_map_ready","top25_drivers",
               "driver_record","team_record","_version")
  if (!table %in% allowed) {
    return(list(error = paste0("Invalid table. Allowed: ", paste(allowed, collapse = ", "))))
  }
  db_read(paste0("SELECT * FROM ", table))
}

#* Insert a new race row (minimal example)
#* @param year:int
#* @param date:text
#* @param title:text
#* @param circuit:text
#* @param winner_driver:text
#* @param pole_driver:text
#* @post /addData/race
function(year, date, title, circuit, winner_driver, pole_driver) {
  sql <- "INSERT INTO races_master(year,date,date_parsed,title,circuit,winner_driver,pole_driver,winner_team)
          VALUES(?, ?, ?, ?, ?, ?, ?, ?)"
  # date_parsed as ISO (YYYY-MM-DD) if you pass one; else NULL
  date_parsed <- NA
  winner_team <- "Not listed"
  db_exec(sql, params = list(as.integer(year), as.character(date), as.character(date_parsed),
                             as.character(title), as.character(circuit),
                             as.character(winner_driver), as.character(pole_driver),
                             as.character(winner_team)))
  db_exec("INSERT INTO _version(ts) VALUES(datetime('now'))")
  list(status = "inserted")
}

#* Delete a race by simple key (year + circuit + date)
#* @param year:int
#* @param circuit:text
#* @param date:text
#* @delete /deleteData/race
function(year, circuit, date) {
  sql <- "DELETE FROM races_master WHERE year = ? AND circuit = ? AND date = ?"
  n <- db_exec(sql, params = list(as.integer(year), as.character(circuit), as.character(date)))
  db_exec("INSERT INTO _version(ts) VALUES(datetime('now'))")
  list(status = "deleted", rows = n)
}

#* Bump version (force Shiny to refresh)
#* @post /refresh
function(){
  db_exec("INSERT INTO _version(ts) VALUES(datetime('now'))")
  list(status = "ok")
}
