pkgs <- c("plumber","DBI","RSQLite","jsonlite","dplyr","ggplot2")
new_pkgs <- pkgs[!(pkgs %in% installed.packages()[,"Package"])]
if(length(new_pkgs)) install.packages(new_pkgs, dependencies = TRUE)

library(plumber)
library(DBI)
library(RSQLite)
library(jsonlite)
library(dplyr)
library(ggplot2)

base_dir <- "C:/Users/rohan/Downloads"
processed_dir <- file.path(base_dir,"processed")
db_path <- file.path(processed_dir,"f1.sqlite")

get_con <- function(){ dbConnect(SQLite(), db_path) }
close_con <- function(con){ try(dbDisconnect(con), silent = TRUE) }

valid_tables <- c("races_master","constructors_all","circuits_map_ready","top25_drivers")

parse_filters <- function(req){
  q <- req$args
  keep <- setdiff(names(q), c("table","limit","offset","type","year","n"))
  if(length(keep)==0) return(list(sql = "", params = list()))
  sql <- paste(paste0(keep," = ?"), collapse = " AND ")
  params <- unname(unlist(q[keep], use.names = FALSE))
  list(sql = paste0(" WHERE ", sql), params = params)
}

#* @apiTitle F1 SQLite API
#* @apiDescription REST endpoints for adding, deleting, retrieving, and visualizing F1 data.

#* Health
#* @get /ping
function(){ list(ok=TRUE, db=file.exists(db_path)) }

#* Insert rows
#* @post /addData
function(req, res, table){
  if(missing(table) || !(table %in% valid_tables)){ res$status <- 400; return(list(error="invalid table")) }
  body <- req$postBody
  if(is.null(body) || nchar(body)==0){ res$status <- 400; return(list(error="empty body")) }
  dat <- tryCatch(jsonlite::fromJSON(body, simplifyDataFrame = TRUE), error=function(e) NULL)
  if(is.null(dat) || !is.data.frame(dat)){ res$status <- 400; return(list(error="invalid json")) }
  con <- get_con()
  on.exit(close_con(con))
  cols_db <- dbListFields(con, table)
  dat <- tibble::as_tibble(dat)
  dat <- dat[, intersect(names(dat), cols_db), drop = FALSE]
  if(nrow(dat)==0 || ncol(dat)==0){ res$status <- 400; return(list(error="no matching columns or no rows")) }
  dbWriteTable(con, table, dat, append = TRUE)
  list(inserted=nrow(dat), table=table)
}

#* Delete with filters
#* @delete /deleteData
function(req, res, table){
  if(missing(table) || !(table %in% valid_tables)){ res$status <- 400; return(list(error="invalid table")) }
  body <- req$postBody
  if(is.null(body) || nchar(body)==0){ res$status <- 400; return(list(error="filters required")) }
  flt <- tryCatch(jsonlite::fromJSON(body, simplifyVector = TRUE), error=function(e) NULL)
  if(is.null(flt) || !is.list(flt) || length(flt)==0){ res$status <- 400; return(list(error="invalid filters")) }
  keys <- names(flt)
  sql_where <- paste(paste0(keys," = ?"), collapse = " AND ")
  params <- unname(unlist(flt, use.names = FALSE))
  con <- get_con()
  on.exit(close_con(con))
  stmt <- paste0("DELETE FROM ", DBI::dbQuoteIdentifier(con, table), " WHERE ", sql_where)
  res$n <- dbExecute(con, stmt, params = params)
  list(deleted=res$n, table=table)
}

#* Get data
#* @serializer unboxedJSON
#* @get /getData
function(req, res, table, limit=1000, offset=0){
  if(missing(table) || !(table %in% valid_tables)){ res$status <- 400; return(list(error="invalid table")) }
  limit <- suppressWarnings(as.integer(limit)); if(is.na(limit) || limit<=0) limit <- 1000
  offset <- suppressWarnings(as.integer(offset)); if(is.na(offset) || offset<0) offset <- 0
  con <- get_con()
  on.exit(close_con(con))
  f <- parse_filters(req)
  sql <- paste0("SELECT * FROM ", DBI::dbQuoteIdentifier(con, table), f$sql, " LIMIT ? OFFSET ?")
  params <- c(f$params, limit, offset)
  out <- dbGetQuery(con, sql, params = params)
  out
}

#* Visualize constructors top-N by year
#* @serializer png
#* @get /visualize
function(type="constructors_top", year=NULL, n=10){
  if(type!="constructors_top") plumber::raise(400, "unsupported type")
  year <- suppressWarnings(as.integer(year))
  n <- suppressWarnings(as.integer(n)); if(is.na(n) || n<=0) n <- 10
  con <- get_con()
  on.exit(close_con(con))
  if(is.na(year)) plumber::raise(400, "year required")
  q <- "SELECT team, win FROM constructors_all WHERE year = ? ORDER BY win DESC"
  df <- dbGetQuery(con, q, params = list(year))
  df <- dplyr::as_tibble(df) |> dplyr::arrange(dplyr::desc(win)) |> dplyr::slice_head(n=n)
  if(nrow(df)==0){
    p <- ggplot() + theme_void() + ggtitle(paste0("No data for year ", year))
  } else {
    df$team <- factor(df$team, levels = rev(df$team))
    p <- ggplot(df, aes(x = win, y = team)) + geom_col() + labs(x="Wins", y=NULL, title=paste0("Constructors Top ", n, " — ", year)) + theme_minimal(base_size = 12) + theme(legend.position="none")
  }
  print(p)
}
