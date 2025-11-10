pkgs <- c(
  "tidyverse","readr","lubridate","janitor",
  "leaflet","sf","DT","plotly","shiny","bslib","shinyWidgets",
  "ggalluvial","RColorBrewer","rnaturalearth", "scales","rnaturalearthdata"
)
new_pkgs <- pkgs[!(pkgs %in% installed.packages()[,"Package"])]
if(length(new_pkgs)) install.packages(new_pkgs, dependencies = TRUE)

parse_numeric <- function(x){
  x <- as.character(x)
  x <- gsub("\\s+", "", x)
  x <- gsub("[\u2212\u2010-\u2015]", "-", x, perl = TRUE)
  neg_paren <- grepl("^\\(.*\\)$", x)
  x <- gsub("[,%]", "", x)
  x <- gsub("[^0-9\\.-]", "", x)
  val <- suppressWarnings(as.numeric(x))
  val[neg_paren & !is.na(val)] <- -abs(val[neg_paren & !is.na(val)])
  val
}

library(shiny)
library(bslib)
library(shinyWidgets)
library(tidyverse)
library(readr)
library(DT)
library(leaflet)
library(plotly)
library(scales)
library(ggalluvial)
library(RColorBrewer)
library(rnaturalearth)
library(sf)
library(janitor)

if(!requireNamespace("DBI", quietly = TRUE)) install.packages("DBI", dependencies = TRUE)
if(!requireNamespace("RSQLite", quietly = TRUE)) install.packages("RSQLite", dependencies = TRUE)
library(DBI); library(RSQLite)

base_dir <- "C:/Users/rohan/Downloads"
processed_dir <- file.path(base_dir,"processed")
db_path <- file.path(processed_dir, "f1.sqlite")

initial_load <- function(){
  if (file.exists(db_path)) {
    con <- dbConnect(SQLite(), db_path)
    on.exit(try(dbDisconnect(con), silent = TRUE), add = TRUE)
    safe_tbl <- function(tbl, default_df = tibble(), null_ok = FALSE){
      if (!dbExistsTable(con, tbl)) return(if (null_ok) NULL else default_df)
      DBI::dbReadTable(con, tbl) |> janitor::clean_names()
    }
    list(
      races_master      = safe_tbl("races_master"),
      constructors_all  = safe_tbl("constructors_all"),
      circuits_map_ready= safe_tbl("circuits_map_ready", null_ok = TRUE),
      driver_record     = safe_tbl("driver_record", default_df = tibble()),
      team_record       = safe_tbl("team_record",   default_df = tibble()),
      top25_drivers     = safe_tbl("top25_drivers", default_df = tibble())
    )
  } else {
    races_master <- read_csv(file.path(processed_dir, "races_master.csv"), show_col_types = FALSE)
    constructors_all <- read_csv(file.path(processed_dir, "constructors_all.csv"), show_col_types = FALSE)
    circuits_map_ready_path <- file.path(processed_dir, "circuits_map_ready.csv")
    circuits_map_ready <- if(file.exists(circuits_map_ready_path)) read_csv(circuits_map_ready_path, show_col_types = FALSE) else NULL
    driver_record_path <- file.path(processed_dir, "driver_record_clean.csv")
    team_record_path <- file.path(processed_dir, "team_record_clean.csv")
    driver_record <- if(file.exists(driver_record_path)) read_csv(driver_record_path, show_col_types = FALSE) else tibble()
    team_record <- if(file.exists(team_record_path)) read_csv(team_record_path, show_col_types = FALSE) else tibble()
    top25_path <- file.path(processed_dir, "top25_drivers.csv")
    top25_drivers <- if(file.exists(top25_path)) read_csv(top25_path, show_col_types = FALSE) else tibble()
    list(
      races_master=races_master,
      constructors_all=constructors_all,
      circuits_map_ready=circuits_map_ready,
      driver_record=driver_record,
      team_record=team_record,
      top25_drivers=top25_drivers
    )
  }
}

viewers_path <- "C:/Users/rohan/Downloads/races/F1_viewers.csv"
viewers_raw <- NULL
viewers_error <- NULL
if (file.exists(viewers_path)) {
  viewers_raw <- tryCatch(read_csv(viewers_path, show_col_types = FALSE), error = function(e) { viewers_error <<- e$message; NULL })
} else {
  viewers_error <- paste0("File not found: ", viewers_path)
}
viewers_df <- if (!is.null(viewers_raw)) viewers_raw %>% clean_names() else tibble()

pillars_path <- file.path(base_dir, "races", "F1_Pillars.csv")
pillars_df <- if (file.exists(pillars_path)) read_csv(pillars_path, show_col_types = FALSE) else tibble()
if (nrow(pillars_df) > 0 && all(c("Pillar","Description") %in% names(pillars_df))) {
  pillars_df <- pillars_df %>% mutate(Pillar = as.character(Pillar), Description = as.character(Description))
} else {
  pillars_df <- tibble(Pillar = character(), Description = character())
}

init <- initial_load()
years_init <- sort(unique(init$races_master$year))
drivers_init <- sort(unique(na.omit(init$races_master$winner_driver)))
teams_init <- sort(unique(init$constructors_all$team))

pal_drivers <- colorRampPalette(brewer.pal(8, "Set2"))(max(1, nrow(init$top25_drivers)))
pal_teams <- colorRampPalette(brewer.pal(8, "Paired"))(max(1, length(teams_init)))

ui <- page_fluid(
  theme = bs_theme(version = 5, bootswatch = "flatly"),
  fluidRow(
    column(
      width = 4,
      pickerInput(
        inputId = "page_select",
        label = NULL,
        choices = c("Home","World Map","Records","Race Results","Analytics","Audience"),
        selected = "Home",
        options = list(title = "Select option")
      )
    )
  ),
  uiOutput("page_body")
)

server <- function(input, output, session){
  check_ver <- function(){
    if (!file.exists(db_path)) return(as.character(file.info(file.path(processed_dir,"races_master.csv"))$mtime))
    con <- dbConnect(SQLite(), db_path)
    on.exit(dbDisconnect(con), add = TRUE)
    if (!dbExistsTable(con,"_version")) return(as.character(file.info(db_path)$mtime))
    v <- dbGetQuery(con,"select coalesce(max(ts),'') as ts from _version")
    as.character(v$ts[1])
  }
  read_all <- function(){
    if (file.exists(db_path)) {
      con <- dbConnect(SQLite(), db_path)
      on.exit(dbDisconnect(con), add = TRUE)
      safe_tbl <- function(tbl, default_df = tibble(), null_ok = FALSE){
        if (!dbExistsTable(con, tbl)) return(if (null_ok) NULL else default_df)
        DBI::dbReadTable(con, tbl) |> janitor::clean_names()
      }
      list(
        races_master      = safe_tbl("races_master"),
        constructors_all  = safe_tbl("constructors_all"),
        circuits_map_ready= safe_tbl("circuits_map_ready", null_ok = TRUE),
        driver_record     = safe_tbl("driver_record", default_df = tibble()),
        team_record       = safe_tbl("team_record",   default_df = tibble()),
        top25_drivers     = safe_tbl("top25_drivers", default_df = tibble())
      )
    } else {
      initial_load()
    }
  }
  data_all <- reactivePoll(3000, session, check_ver, read_all)
  
  observe({
    rm <- data_all()$races_master
    ca <- data_all()$constructors_all
    years <- sort(unique(rm$year))
    drivers <- sort(unique(na.omit(rm$winner_driver)))
    teams <- sort(unique(ca$team))
    if (!is.null(input$year_rr)) updatePickerInput(session,"year_rr", choices = years, selected = intersect(isolate(input$year_rr), years))
    if (!is.null(input$conv_year)) updatePickerInput(session,"conv_year", choices = years, selected = intersect(isolate(input$conv_year), years))
    if (!is.null(input$trend_driver)) updatePickerInput(session,"trend_driver", choices = drivers, selected = intersect(isolate(input$trend_driver), drivers))
    if (!is.null(input$driver_pole)) updatePickerInput(session,"driver_pole", choices = drivers, selected = intersect(isolate(input$driver_pole), drivers))
    if (!is.null(input$driver_home)) updatePickerInput(session,"driver_home", choices = drivers, selected = intersect(isolate(input$driver_home), drivers))
    if (!is.null(input$year_const)) updatePickerInput(session,"year_const", choices = sort(unique(ca$year)), selected = intersect(isolate(input$year_const), unique(ca$year)))
  })
  
  output$pillars <- renderUI({
    if (nrow(pillars_df) == 0) return(NULL)
    needed <- paste0("Pillar ", 1:5)
    cards <- lapply(needed, function(p){
      desc <- pillars_df %>% filter(Pillar == p) %>% pull(Description)
      desc <- if (length(desc) == 0 || is.na(desc[1])) "" else desc[1]
      column(
        width = 2,
        bslib::card(
          class = "shadow-sm",
          bslib::card_header(tags$b(p)),
          bslib::card_body(tags$div(style="min-height:70px;", desc))
        )
      )
    })
    tagList(h3("Pillars of F1"), fluidRow(!!!cards))
  })
  
  output$page_body <- renderUI({
    req(input$page_select)
    switch(input$page_select,
           "Home" = fluidPage(
             br(),
             tags$h2("F1 Racing Dashboard", class = "text-center"),
             br(),
             uiOutput("pillars"),
             br(),
             h3("Driver Profile Radar"),
             fluidRow(
               column(
                 width = 6,
                 pickerInput("driver_home", label = NULL, choices = drivers_init, options = list(title = "Select option")),
                 br(),
                 plotlyOutput("radar_profile", height = 600)
               ),
               column(
                 width = 6,
                 uiOutput("driver_photo")
               )
             )
           ),
           "World Map" = fluidPage(br(), leafletOutput("map_circuits", height = 600)),
           "Records" = fluidPage(br(), h4("Driver Records"), DTOutput("driver_records_tbl"), br(), h4("Team Records"), DTOutput("team_records_tbl")),
           "Race Results" = fluidPage(br(), pickerInput(inputId = "year_rr", label = NULL, choices = years_init, options = list(title = "Select option")), br(), DTOutput("race_results_tbl")),
           "Analytics" = fluidPage(
             br(), h4("Pole → Wins"),
             fluidRow(column(4, pickerInput("conv_year", label = NULL, choices = years_init, options = list(title = "Select option")))),
             plotlyOutput("conv_donut", height = 420), br(), hr(), br(),
             h4("Top Constructor Wins"),
             fluidRow(column(4, pickerInput("alluvial_topn", choices = c(5,10,15), selected = 10))),
             plotlyOutput("alluvial_plot", height = 500), br(), hr(), br(),
             h4("Driver Wins Trend Across Years"),
             fluidRow(column(4, pickerInput("trend_driver", label = NULL, choices = drivers_init, options = list(title = "Select option")))),
             plotlyOutput("driver_trend_plot", height = 400), br(), hr(), br(),
             h4("Pole vs Win Impact"),
             fluidRow(column(4, pickerInput(inputId = "driver_pole", label = NULL, choices = drivers_init, options = list(title = "Select option")))),
             plotlyOutput("pole_win_plot", height = 350), br(), hr(), br(),
             h4("Top 25 Drivers by Wins"), plotlyOutput("top25_plot", height = 450), br(), hr(), br(),
             h4("Constructors: Top 10 Wins in Selected Year"),
             pickerInput(inputId = "year_const", label = NULL, choices = sort(unique(init$constructors_all$year)), options = list(title = "Select option")),
             plotlyOutput("constructors_top10_plot", height = 450)
           ),
           "Audience" = fluidPage(
             br(),
             tags$h2("F1 Viewership", class = "text-center"),
             br(),
             fluidRow(column(width = 12, uiOutput("viewers_status"))),
             br(),
             fluidRow(
               column(width = 6, plotlyOutput("slope_plot", height = 600)),
               column(width = 6, leafletOutput("audience_map", height = 600))
             )
           )
    )
  })
  
  output$viewers_status <- renderUI({
    if (!is.null(viewers_error)) {
      tags$div(style="color:#a94442; padding:8px; border:1px solid #f2dede; background:#f9eaea;", tags$b("Viewers file issue: "), tags$pre(viewers_error))
    } else if (nrow(viewers_df) == 0) {
      tags$div(style="color:#8a6d3b; padding:8px; border:1px solid #faebcc; background:#fff7e6;", paste0("Loaded 0 rows from: ", ifelse(is.na(viewers_path), "N/A", viewers_path), ". Check file contents/headers."))
    } else {
      tags$div(style="color:#31708f; padding:8px; border:1px solid #d9edf7; background:#f2f9fc;")
    }
  })
  
  output$map_circuits <- renderLeaflet({
    cmr <- data_all()$circuits_map_ready
    req(!is.null(cmr))
    leaflet(cmr) |>
      addTiles() |>
      addCircleMarkers(lng = ~lon, lat = ~lat, radius = 5,
                       popup = ~paste0("<b>Circuit:</b> ", circuit, "<br><b>Location:</b> ", location, "<br><b>Races held:</b> ", race_count))
  })
  
  output$driver_records_tbl <- renderDT({
    datatable(data_all()$driver_record, rownames = FALSE, options = list(pageLength = 10, scrollX = TRUE))
  })
  output$team_records_tbl <- renderDT({
    datatable(data_all()$team_record, rownames = FALSE, options = list(pageLength = 10, scrollX = TRUE))
  })
  
  output$race_results_tbl <- renderDT({
    req(input$year_rr)
    rm <- data_all()$races_master
    df <- rm |> filter(year == input$year_rr) |> select(date, title, circuit, winner_driver, pole_driver)
    datatable(df, rownames = FALSE, options = list(pageLength = 15, scrollX = TRUE))
  })
  
  output$radar_profile <- renderPlotly({
    req(input$driver_home)
    rm <- data_all()$races_master
    drivers <- sort(unique(na.omit(rm$winner_driver)))
    drv <- input$driver_home
    total_wins <- sum(rm$winner_driver == drv, na.rm = TRUE)
    total_poles <- sum(rm$pole_driver == drv, na.rm = TRUE)
    distinct_circuits <- n_distinct(rm$circuit[rm$winner_driver == drv])
    years_active <- n_distinct(rm$year[rm$winner_driver == drv])
    wins_max <- max(table(rm$winner_driver), na.rm = TRUE)
    poles_max <- max(table(rm$pole_driver), na.rm = TRUE)
    circuits_max <- max(rm %>% filter(winner_driver %in% drivers) %>% group_by(winner_driver) %>% summarize(n = n_distinct(circuit)) %>% pull(n), na.rm = TRUE)
    years_max <- max(rm %>% group_by(winner_driver) %>% summarize(n = n_distinct(year)) %>% pull(n), na.rm = TRUE)
    wins_max <- ifelse(is.na(wins_max) || wins_max==0, 1, wins_max)
    poles_max <- ifelse(is.na(poles_max) || poles_max==0, 1, poles_max)
    circuits_max <- ifelse(is.na(circuits_max) || circuits_max==0, 1, circuits_max)
    years_max <- ifelse(is.na(years_max) || years_max==0, 1, years_max)
    vec_raw <- c(total_wins, total_poles, distinct_circuits, years_active)
    vec_norm <- c(total_wins/wins_max, total_poles/poles_max, distinct_circuits/circuits_max, years_active/years_max)
    categories <- c("Wins","Poles","Distinct Circuits Won","Years with Wins")
    plot_ly(type = 'scatterpolar', r = c(vec_norm, vec_norm[1]), theta = c(categories, categories[1]), fill = 'toself',
            hoverinfo = 'text', text = paste0(categories, ": ", c(vec_raw, vec_raw[1]))) %>%
      layout(margin = list(t = 80, r = 40, b = 40, l = 40),
             polar = list(radialaxis = list(visible = TRUE, range = c(0,1), tickmode = "array", tickvals = seq(0,1,0.25), ticktext = c("0%","25%","50%","75%","100%"))),
             showlegend = FALSE, title = paste("Profile: ", drv))
  })
  
  output$driver_trend_plot <- renderPlotly({
    req(input$trend_driver)
    rm <- data_all()$races_master
    df <- rm %>% filter(winner_driver == input$trend_driver) %>% count(year, name = "wins") %>% arrange(year)
    if (nrow(df) == 0) df <- tibble(year = sort(unique(rm$year)), wins = 0)
    df <- df %>% mutate(year = as.integer(year), wins = as.numeric(replace_na(wins, 0)), hover = paste0("Year: ", year, "<br>Wins: ", wins))
    p <- ggplot(df, aes(x = year, y = wins, group = 1, text = hover)) +
      geom_line(size = 1.2) +
      geom_point(size = 2.5) +
      scale_x_continuous(breaks = sort(unique(df$year))) +
      scale_y_continuous(breaks = function(lims) seq(0, ceiling(lims[2]), by = 1)) +
      labs(x = "Year", y = "Wins") +
      theme_minimal(base_size = 13)
    ggplotly(p, tooltip = "text")
  })
  
  output$driver_photo <- renderUI({
    req(input$driver_home)
    drv <- input$driver_home
    exts <- c("png","jpg","jpeg","webp")
    slug <- tolower(gsub("[^a-z0-9]+","_", drv))
    candidates <- unique(c(file.path("drivers", paste0(drv, ".", exts)), file.path("drivers", paste0(slug, ".", exts))))
    existing <- Filter(function(rel) file.exists(file.path("www", rel)), candidates)
    img_rel <- if (length(existing) > 0) existing[[1]] else { if (file.exists("www/drivers/placeholder.png")) "drivers/placeholder.png" else NULL }
    bslib::card(class = "shadow-sm", bslib::card_header(tags$b(drv)),
                bslib::card_body(if (is.null(img_rel)) tags$div("No photo available", style="opacity:0.6; padding:8px;") else tags$img(src = img_rel, style = "width:100%; height:auto; border-radius:12px; object-fit:cover;")))
  })
  
  output$conv_donut <- renderPlotly({
    req(input$conv_year)
    rm <- data_all()$races_master
    df <- rm |>
      filter(year == input$conv_year) |>
      mutate(converted = pole_driver == winner_driver) |>
      count(converted, name = "n") |>
      tidyr::complete(converted = c(TRUE, FALSE), fill = list(n = 0)) |>
      mutate(label = if_else(converted, "Pole converted to win", "Pole not converted"),
             pct = if (sum(n) == 0) 0 else round(100 * n / sum(n), 1),
             hover = paste0(label, "<br>Races: ", n, "<br>", pct, "%"))
    plot_ly(data = df, labels = ~label, values = ~n, type = "pie", hole = 0.55, textinfo = "label+percent", hoverinfo = "text", textposition = "inside", hovertext = ~hover, sort = FALSE) |>
      layout(showlegend = FALSE, annotations = list(list(x = 0.5, y = 0.5, xref = "paper", yref = "paper", text = paste0(dplyr::first(df$pct[df$label=="Pole converted to win"]), "%"), showarrow = FALSE, font = list(size = 24))), margin = list(t = 30, r = 30, b = 30, l = 30))
  })
  
  output$alluvial_plot <- renderPlotly({
    req(input$alluvial_topn)
    ca <- data_all()$constructors_all
    topn <- as.integer(input$alluvial_topn)
    df <- ca %>% group_by(year, team) %>% summarise(wins = sum(win, na.rm = TRUE), .groups = "drop")
    top_teams <- df %>% group_by(team) %>% summarise(total = sum(wins), .groups = "drop") %>% arrange(desc(total)) %>% slice_head(n = topn) %>% pull(team)
    df2 <- df %>% mutate(team2 = if_else(team %in% top_teams, team, "Other")) %>% group_by(year, team2) %>% summarise(wins = sum(wins, na.rm = TRUE), .groups = "drop") %>% mutate(year = as.factor(year))
    p <- ggplot(df2, aes(x = year, stratum = team2, alluvium = team2, y = wins, fill = team2, label = team2)) +
      geom_flow(stat = "alluvium", lode.guidance = "frontback", color = "grey40", alpha = 0.9) +
      geom_stratum(width = 0.2) +
      scale_fill_manual(values = colorRampPalette(brewer.pal(8, "Set3"))(length(unique(df2$team2)))) +
      theme_minimal() +
      labs(x = "Year", y = "Wins", fill = "Team") +
      theme(legend.position = "right")
    ggplotly(p, tooltip = c("y","fill"))
  })
  
  output$pole_win_plot <- renderPlotly({
    req(input$driver_pole)
    rm <- data_all()$races_master
    base_cats <- tibble(on_pole = c(TRUE, FALSE))
    df <- rm |>
      mutate(on_pole = pole_driver == winner_driver) |>
      filter(winner_driver == input$driver_pole) |>
      count(on_pole, name = "wins") |>
      right_join(base_cats, by = "on_pole") |>
      mutate(wins = replace_na(wins, 0L)) |>
      mutate(category = if_else(on_pole, "Wins when on pole", "Wins when not on pole"))
    p <- ggplot(df, aes(x = category, y = wins, fill = category, text = paste0(category, "<br>Wins: ", wins))) +
      geom_col() +
      scale_y_continuous(breaks = function(lims) seq(0, ceiling(lims[2]), by = 1)) +
      scale_fill_manual(values = c("Wins when on pole" = "#1f78b4", "Wins when not on pole" = "#33a02c")) +
      labs(x = NULL, y = "Wins") +
      theme_minimal(base_size = 13) +
      theme(legend.position = "none")
    ggplotly(p, tooltip = "text")
  })
  
  output$top25_plot <- renderPlotly({
    t25 <- data_all()$top25_drivers
    req(nrow(t25) > 0)
    df <- t25 |> arrange(wins) |> mutate(winner_driver = factor(winner_driver, levels = winner_driver))
    pal <- colorRampPalette(brewer.pal(8, "Set2"))(nrow(df))
    p <- ggplot(df, aes(x = wins, y = winner_driver, fill = winner_driver, text = paste0(winner_driver, "<br>Wins: ", wins))) +
      geom_col() +
      scale_fill_manual(values = pal) +
      labs(x = "Wins", y = NULL) +
      theme_minimal(base_size = 13) +
      theme(legend.position = "none")
    ggplotly(p, tooltip = "text")
  })
  
  output$constructors_top10_plot <- renderPlotly({
    req(input$year_const)
    ca <- data_all()$constructors_all
    df <- ca |> filter(year == input$year_const) |> arrange(desc(win)) |> slice_head(n = 10) |> mutate(team = factor(team, levels = rev(unique(team))))
    pal <- colorRampPalette(brewer.pal(8, "Paired"))(nrow(df))
    p <- ggplot(df, aes(x = win, y = team, fill = team, text = paste0(team, "<br>Wins: ", win))) +
      geom_col() +
      scale_fill_manual(values = pal) +
      labs(x = "Wins", y = NULL) +
      theme_minimal(base_size = 13) +
      theme(legend.position = "none")
    ggplotly(p, tooltip = "text")
  })
  
  output$slope_plot <- renderPlotly({
    validate(need(!is.null(viewers_error) == FALSE, paste0("Viewers CSV error: ", viewers_error)))
    validate(need(nrow(viewers_df) > 0, "Viewers CSV not loaded or empty. See status above."))
    col_2018 <- names(viewers_df)[grepl("2018",  names(viewers_df))]
    col_2019 <- names(viewers_df)[grepl("2019",  names(viewers_df))]
    col_pct  <- names(viewers_df)[grepl("change", names(viewers_df))]
    col_market <- names(viewers_df)[grepl("market", names(viewers_df))]
    validate(need(length(col_market) > 0 && length(col_2018) > 0 && length(col_2019) > 0, "Could not detect 2018/2019/market columns. Check CSV headers."))
    col_2018 <- col_2018[1]; col_2019 <- col_2019[1]
    col_market <- col_market[1]; col_pct <- if (length(col_pct)) col_pct[1] else NA_character_
    df <- viewers_df %>%
      rename(market = all_of(col_market), reach_2018 = all_of(col_2018), reach_2019 = all_of(col_2019)) %>%
      mutate(reach_2018 = parse_numeric(reach_2018), reach_2019 = parse_numeric(reach_2019), pct_file = if (!is.na(col_pct)) parse_numeric(.data[[col_pct]]) else NA_real_, pct_calc = ifelse(is.finite(reach_2018) & reach_2018 != 0,(reach_2019 - reach_2018)/reach_2018 * 100, NA_real_), pct_final = dplyr::coalesce(pct_calc, pct_file), market = as.character(market)) %>%
      arrange(desc(reach_2019))
    df_long <- df %>% select(market, reach_2018, reach_2019, pct_final) %>% pivot_longer(cols = c(reach_2018, reach_2019), names_to = "year", values_to = "reach") %>%
      mutate(year = if_else(year == "reach_2018", 2018L, 2019L), reach_lab = scales::comma(reach, accuracy = 1)) %>%
      group_by(market) %>%
      mutate(trend = dplyr::case_when(first(pct_final) > 0 ~ "up", first(pct_final) < 0 ~ "down", TRUE ~ "flat")) %>% ungroup()
    p <- ggplot(df_long, aes(x = year, y = reach, group = market, text = paste0("Market: ", market,"<br>Year: ", year,"<br>Reach: ", reach_lab,"<br>% change: ", round(pct_final, 1), "%"))) +
      geom_line(aes(color = trend), size = 0.9, alpha = 0.9) +
      geom_point(size = 2) +
      scale_x_continuous(breaks = c(2018, 2019)) +
      scale_y_continuous(labels = scales::label_comma(accuracy = 1)) +
      scale_color_manual(values = c(up = "#2ca02c", flat = "#9e9e9e", down = "#d62728"), guide = "none") +
      theme_minimal(base_size = 12) +
      labs(x = NULL, y = "Reach")
    ggplotly(p, tooltip = "text") %>% layout(yaxis = list(tickformat = ","))
  })
  
  output$audience_map <- renderLeaflet({
    validate(need(!is.null(viewers_error) == FALSE, paste0("Viewers CSV error: ", viewers_error)))
    validate(need(nrow(viewers_df) > 0, "Viewers CSV not loaded or empty. See status above."))
    col_2018 <- names(viewers_df)[grepl("2018",  names(viewers_df))]
    col_2019 <- names(viewers_df)[grepl("2019",  names(viewers_df))]
    col_pct  <- names(viewers_df)[grepl("change", names(viewers_df))]
    col_market <- names(viewers_df)[grepl("market", names(viewers_df))]
    validate(need(length(col_market) > 0 && length(col_2018) > 0 && length(col_2019) > 0, "Could not detect 2018/2019/market columns. Check CSV headers."))
    col_2018 <- col_2018[1]; col_2019 <- col_2019[1]
    col_market <- col_market[1]; col_pct <- if (length(col_pct)) col_pct[1] else NA_character_
    v <- viewers_df %>%
      rename(market = all_of(col_market), reach_2018 = all_of(col_2018), reach_2019 = all_of(col_2019)) %>%
      mutate(reach_2018 = parse_numeric(reach_2018), reach_2019 = parse_numeric(reach_2019), pct_file = if (!is.na(col_pct)) parse_numeric(.data[[col_pct]]) else NA_real_, pct_calc = ifelse(is.finite(reach_2018) & reach_2018 != 0,(reach_2019 - reach_2018)/reach_2018 * 100, NA_real_), pct_final = dplyr::coalesce(pct_calc, pct_file), market_clean = tolower(as.character(market)))
    world <- rnaturalearth::ne_countries(returnclass = "sf", scale = "medium") %>% mutate(name_long_clean = tolower(name_long), name_clean = tolower(name))
    vm <- v %>% select(market, market_clean, reach_2018, reach_2019, pct_final)
    idx <- match(world$name_long_clean, vm$market_clean)
    na_idx <- which(is.na(idx))
    if(length(na_idx) > 0) idx[na_idx] <- match(world$name_clean[na_idx], vm$market_clean)
    world$reach_2018 <- ifelse(!is.na(idx), vm$reach_2018[idx], NA)
    world$reach_2019 <- ifelse(!is.na(idx), vm$reach_2019[idx], NA)
    world$pct_final <- ifelse(!is.na(idx), vm$pct_final[idx], NA)
    world$market_label <- ifelse(!is.na(idx), vm$market[idx], NA)
    world$fillHex <- ifelse(is.na(world$pct_final), "#d9d9d9", ifelse(world$pct_final > 0, "#2ca02c", ifelse(world$pct_final < 0, "#d62728", "#9e9e9e")))
    world$hover <- paste0(
      "<b>", dplyr::coalesce(world$market_label, world$name), "</b><br/>",
      "2018: ", ifelse(is.na(world$reach_2018), "N/A", formatC(world$reach_2018, format = "f", digits = 0, big.mark=",")), "<br/>",
      "2019: ", ifelse(is.na(world$reach_2019), "N/A", formatC(world$reach_2019, format = "f", digits = 0, big.mark=",")), "<br/>",
      "% change: ", ifelse(is.na(world$pct_final), "N/A", paste0(round(world$pct_final,1), "%"))
    )
    leaflet(world) %>%
      addTiles() %>%
      setView(lng = 0, lat = 20, zoom = 1.5) %>%
      addPolygons(fillColor = ~fillHex, fillOpacity = 0.8, color = "#444444", weight = 0.2, label = lapply(world$hover, htmltools::HTML), highlightOptions = highlightOptions(weight = 2, color = "#666", bringToFront = TRUE)) %>%
      addLegend(position = "bottomright", colors = c("#2ca02c","#d62728","#9e9e9e","#d9d9d9"), labels = c("Positive","Negative","Flat","No data"), title  = "Viewership change")
  })
}

shinyApp(ui, server)
