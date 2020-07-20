library(DBI)
library(shiny)
library(shinyWidgets)
library(tidyverse)
library(DT)
library(jsonlite)
library(igraph)
library(visNetwork)

blacklist_features <- c("seq_id", "app_id", "duration_ms", "project_count", "active_month_year", "total_number_of_days_active_current_month",
                        "story_points", "length", "created_at", "description", "updatedts", "insertedts", "instance_id", "id", "id_stk")

ui <- fluidPage(
  tabsetPanel(
    tabPanel(
      title = "Explore Tables",
      sidebarLayout(
        sidebarPanel(
          wellPanel(
            fileInput(
              inputId = "credentials", label = "Select JSON file with credentials:"
            )
          ),
          wellPanel(
            uiOutput("ui_select_schema_explore")
            ),
          wellPanel(
            uiOutput("ui_select_table")
            ),
          wellPanel(
            downloadButton(outputId = "download_table", label = "Download Table"),
            )
          ),
        mainPanel(
          DT::dataTableOutput(outputId = "db_table"),
          br(),
          verbatimTextOutput(outputId = "console_output")
          )
        )
      ),
    tabPanel(
      title = "Data Relationships",
      sidebarLayout(
        sidebarPanel(
          wellPanel(
            uiOutput("ui_select_schema_rels"),
            uiOutput("ui_select_tables_vis")
            ),
          wellPanel(
            actionButton(inputId = "download_graph_vis", label = "Download graph as html:")
          )
          ),
        mainPanel(
          visNetworkOutput("vis_cats", height = "1200px")
          )
        )
      )
    )
  )


server <- function(input, output, session) {
  
  get_user_db_credentials <- eventReactive(input$credentials, {
    print("Calling: get_user_db_credentials")
    
    input_files <- input$credentials
    data_paths <- input_files$datapath
    creds <- fromJSON(data_paths)
  })
  
  
  create_db_connection <- reactive({
    print("Calling: create_db_connection")
  
    creds <- get_user_db_credentials()
    con <- dbConnect(RPostgres::Postgres(), 
                     dbname = creds$DB_NAME, 
                     host = creds$DB_HOST, 
                     port = creds$DB_PORT, 
                     user = creds$DB_USER, 
                     password = creds$DB_PWD)
    
    
  })
  
  
  get_db_schemas <- reactive({
    print("Calling: get_db_schemas")
    
    con <- create_db_connection()
    schema <- dbGetQuery(con, "SELECT schema_name FROM information_schema.schemata")
  })
  
  
  get_schema_tables_explore <- eventReactive(input$select_schema_explore, {
    print("Calling: get_schema_tables_explore")
    schema_name <- input$select_schema_explore
    con <- create_db_connection()
    tables <- dbGetQuery(con, 
                         paste0("SELECT * FROM information_schema.tables WHERE table_schema = '", schema_name, "'")
                         )
    return(tables)
  })
  
  
  get_schema_tables_rels <- eventReactive(input$select_schema_rels, {
    print("Calling: get_schema_tables_rels")
    schema_name <- input$select_schema_rels
    con <- create_db_connection()
    tables <- tibble()
    for (h in 1:length(schema_name)) {
      temp_tables <- dbGetQuery(con, 
                                paste0("SELECT * FROM information_schema.tables WHERE table_schema = '", schema_name[h], "'")
      ) %>%
        mutate(schema = schema_name[h])
      tables <- bind_rows(tables, temp_tables)
    }
    return(tables)
  })
  
  get_table <- eventReactive(input$go_query_table, {
    print("Calling: get_table")
    schema_name <- input$select_schema_explore
    table_name <- input$select_table
    limit_rows <- input$limit_rows
    con <- create_db_connection()
    table <- dbGetQuery(con, 
                        paste0("select * FROM ", schema_name, ".", table_name, 
                               if_else(limit_rows == 0, "", paste0(" LIMIT ", limit_rows)))
                        )
    return(table)
  })
  
  
  output$ui_select_schema_explore = renderUI({
    selectInput(inputId = "select_schema_explore", label = "Select a schema to analyse:", get_db_schemas()$schema_name)
  })

  
  output$ui_select_table = renderUI({
    tagList(
      selectInput(inputId = "select_table", label = "Select a table to analyse:", get_schema_tables_explore()$table_name),
      numericInput(inputId = "limit_rows", label = "Limit number of rows to: (select zero for all rows)", value = 10, min = 0),
      actionButton(inputId = "go_query_table", label = "Retrieve rows from table:")
    )
  })
  
  
  output$db_table <- DT::renderDataTable({
    get_table()
    })
  
  
  output$console_output <- renderPrint({
    })
  
  
  output$download_table <- downloadHandler(
    filename = function(){
      paste0(input$select_schema_explore, "_", 
             input$select_table, "_limit_", 
             ifelse(input$limit_rows == 0, "all", input$limit_rows), "_", 
             format(Sys.time(), "%Y-%b-%d"),
             ".csv")
    },
    content = function(file) {
      data <- get_table()
      write.csv(data, file, row.names = FALSE, fileEncoding = "UTF-8")
    }
  )
  
  #####################################
  ##### SHARED COLS VISUALIZATION #####
  #####################################
  
  
  output$ui_select_schema_rels = renderUI({
    selectInput(inputId = "select_schema_rels", label = "Select one or more schemas to analyse:", get_db_schemas()$schema_name, multiple = TRUE)
  })
  
  
  output$ui_select_tables_vis = renderUI({
    tagList(
      pickerInput(inputId = "tables_selected", label = "Select tables for visualisation:", 
                  choices = get_schema_tables_rels()$table_name, 
                  options = list(`actions-box` = TRUE),
                  multiple = TRUE),
      numericInput(inputId = "limit_rows_multiple_tables", label = "Limit number of rows to: (select zero for all rows)", value = 10, min = 0),
      actionButton(inputId = "go_query_multiple_tables", label = "Retrieve tables:")
    )
  })
  
  
  get_multiple_tables <- eventReactive(input$go_query_multiple_tables, {
    print("Calling: get_multiple_tables")
    
    tables <- get_schema_tables_rels()
    schema_name <- input$select_schema_rels
    
    selected_tables <- input$tables_selected
    limit_rows <- input$limit_rows_multiple_tables
    
    con <- create_db_connection()
  
    schema_list <- list()
    for (h in 1:length(schema_name)) {
      temp_schema_name <- schema_name[h]
      table_names <- tables %>%
        filter(schema == temp_schema_name) %>%
        filter(table_name %in% selected_tables) %>%
        select(table_name) %>%
        pull() %>%
        unique()
      
      for (i in 1:length(table_names)) {
        temp_table_name <- table_names[i]
        temp_table <- dbGetQuery(con, 
                            paste0("select * FROM ", temp_schema_name, ".", temp_table_name, 
                                   if_else(limit_rows == 0, "", paste0(" LIMIT ", limit_rows)))
                            )
        schema_list[[temp_schema_name]][[temp_table_name]] <- temp_table
      }
    }
    return(schema_list)
  })
  
  
  get_edge_list_columns <- reactive({
    print("Calling: get_edge_list_columns")
    
    schema_list <- get_multiple_tables()
    
    edge_list <- tibble()
    for (h in 1:length(schema_list)) {
      temp_schema_name <- names(schema_list[h])
      temp_schema <- schema_list[[h]]
      temp_table_names <- names(temp_schema)
      temp_df_schema <- tibble(from = temp_schema_name, to = temp_table_names)
      edge_list <- bind_rows(edge_list, temp_df_schema)
      for (i in 1:length(temp_schema)){
        temp_table_name <- names(temp_schema[i])
        temp_columns <- names(temp_schema[[i]]) %>% 
          .[! . %in% blacklist_features]
        temp_df_tables <- tibble(from = temp_table_name, to = temp_columns)
        edge_list <- bind_rows(edge_list, temp_df_tables)
      }
    }
    return(edge_list)
  })
  
  
  output$db_table_II <- DT::renderDataTable({
    get_edge_list_columns()
  })
  
  
  ###########################################
  ##### SHARED CATEGORIES VISUALIZATION #####
  ###########################################
  
  get_schema_data <- reactive({
    print("Calling: get_schema_data")
    
    ###### HELPER FUNCTIONS - EXTRACT FEATURES #####
    get_table_data <- function(df) {
      table_name <- names(df)
      column_names <- names(df[[1]])
      unique_vals <- get_column_unique_vals(df[[1]])
      table_summary <- list(table_name = table_name,
                            column_names = column_names,
                            unique_values = unique_vals)
    }
    
    # Get unique features from every column:
    get_column_unique_vals <- function(df) {
      unique_vals <- map(df, unique)
    }
    
    schema_list <- get_multiple_tables()
    schema_names <- names(schema_list)  
    schema_summary <- list()
    
    for (i in 1:length(schema_list)) {
      tables_list <- schema_list[[i]]
      schema_name <- schema_names[i]
      for (j in 1:length(tables_list)) {
        
        if (nrow(pluck(tables_list[j], 1)) == 0) {
          print(paste0("Table with zero rows: ", names(tables_list[j])))
          next
        }
        temp_tabel_data <- get_table_data(tables_list[j])
        schema_summary[[schema_name]][[temp_tabel_data$table_name]] <- list(column_names = temp_tabel_data$column_names,
                                                             unique_values = temp_tabel_data$unique_values)
      }
    }
    return(schema_summary)
  })
  
  
  get_unique_vars_list <- reactive({
    print("Calling: get_unique_vars_list")
    # blacklist_features <- c("seq_id", "app_id", "duration_ms", "project_count", "active_month_year", "total_number_of_days_active_current_month",
    #                         "story_points", "length", "created_at", "description", "updatedts", "insertedts", "instance_id", "id", "id_stk")
    blacklist_classes <- c("POSIXct", "POSIXt", "Date", "logical", "numeric", "integer64", "integer")
    blacklist_values <- c(NA, 0, "0", "NA", "")
    
    schema_data <- get_schema_data()
    
    unique_vars_list <- list()
    schema_data_names <- names(schema_data)
    for (h in 1:length(schema_data_names)) {
      schema_data_tables <- schema_data[[h]]
      schema_data_tables_names <- names(schema_data_tables)
      for (i in 1:length(schema_data_tables)) {
        temp_table <- schema_data_tables[[i]]$unique_values
        temp_table_cols <- schema_data_tables[[i]]$column_names
        for (j in 1: length(temp_table_cols)) {
          if (temp_table_cols[j] %in% blacklist_features) {
            next
          }
          temp_class <- temp_table[j] %>%
            first() %>% 
            class()
          if (temp_class %in% c("numeric", "double", "integer64") && grepl(pattern = "by_user|created_by|instance_id", x = temp_table_cols[j])) {
            temp_table[[j]] <- as.character(temp_table[[j]])
            temp_class <- "character"
          }
          if (length(intersect(temp_class, blacklist_classes)) > 0) {
            next
          }
          temp_table_col_unique <- paste0(schema_data_names[h], "_zzz_", schema_data_tables_names[i], "_var_", temp_table_cols[j])
          unique_vars_list[[temp_table_col_unique]] <- temp_table[j] %>%
            first() %>%
            .[! . %in% blacklist_values]
        }
      }
    }
    return(unique_vars_list)
  })
  
  
  create_adjacency_matrix <- reactive({
    print("Calling: create_adjacency_matrix")
    
    unique_vars_list <- get_unique_vars_list()
    matrix_intersects <- matrix(nrow = length(unique_vars_list), ncol = length(unique_vars_list), 
                                dimnames = list(names(unique_vars_list), names(unique_vars_list)))
    
    for (k in 1:length(unique_vars_list)) {
      for (l in 1:length(unique_vars_list)) {
        if (k == l) {
          matrix_intersects[k, l] <- 0
        } else {
          temp_intersect <- base::intersect(unique_vars_list[k] %>% 
                                              first(),
                                            unique_vars_list[l] %>% 
                                              first()) %>%
            length()
          if (temp_intersect == 0) {
            matrix_intersects[k, l] <- 0
            next
          }
          
          temp_union <- base::union(unique_vars_list[k] %>% 
                                      first(),
                                    unique_vars_list[l] %>% 
                                      first()) %>%
            length()
          
          i_o_u <- temp_intersect / temp_union
          
          if (i_o_u <= 0.05 || is.nan(i_o_u)) {
            matrix_intersects[k, l] <- 0
            next
          }
          
          matrix_intersects[k, l] <- i_o_u
        }
      }
    }
    
    return(matrix_intersects)
  })
  
  
  create_edgelist_from_adjacency <- reactive({
    print("Calling: create_edgelist_from_adjacency")
    
    matrix_intersects <- create_adjacency_matrix()
    edge_list_from_adjacency <- tibble()
    
    for (k in 1:dim(matrix_intersects)[1]) {
      for (l in 1:dim(matrix_intersects)[2]) {
        if (k == l) {
          next
        } else {
          temp_edge <- matrix_intersects[k,l]
          if (is.nan(temp_edge) || temp_edge == 0) {
            next
          } else {
            
            row_node_name_nested <- rownames(matrix_intersects)[k]  
            col_node_name_nested <- colnames(matrix_intersects)[l]
            row_nodes_names <- str_split(row_node_name_nested, pattern = "_zzz_|_var_", n = 3) %>% first()
            col_nodes_names <- str_split(col_node_name_nested, pattern = "_zzz_|_var_", n = 3) %>% first()
            
            if (!identical(row_nodes_names[3], col_nodes_names[3])) {
              temp_edge_list_bridge <- tibble(from = row_nodes_names[3], to = col_nodes_names[3], weight = matrix_intersects[k,l])
              edge_list_from_adjacency <- bind_rows(edge_list_from_adjacency, 
                                                    temp_edge_list_bridge)
              }
            }
          }
        }
      }
    return(edge_list_from_adjacency)
  })
  
  
  build_categories_graph <- reactive({
    print("Calling: build_categories_graph")
    
    edge_list_cols <- get_edge_list_columns() %>%
      mutate(weight = 1, type = "col")
    
    edge_list_cats <- create_edgelist_from_adjacency() %>% 
      group_by(from, to) %>%
      filter(weight == max(weight)) %>%
      ungroup() %>%
      mutate(type = "cat")
    
    edge_list <- bind_rows(edge_list_cols, edge_list_cats)
    
    g <- graph_from_data_frame(edge_list, directed = TRUE)
    deg_in <- degree(g, mode = "in")
    
    vis_nodes <- data.frame(id=V(g)$name, label=V(g)$name, stringsAsFactors = FALSE)
    vis_nodes$size <- if_else(V(g)$name %in% unique(edge_list$from), 30, 10) + ifelse(deg_in > 100, 100, deg_in)
    vis_nodes$size <- if_else(V(g)$name %in% input$select_schema_rels, 80, vis_nodes$size)
    vis_nodes$color.background <- heat.colors(length(levels(factor(deg_in))), alpha = 0.75, rev = TRUE)[factor(deg_in)]
    vis_nodes$color.background <- if_else(V(g)$name %in% input$select_schema_rels, "#BCF79C", vis_nodes$color.background)
    vis_nodes$color.border <- "#E6E6E6"
    vis_nodes$color.highlight.background <- "orange"
    vis_nodes$color.highlight.border <- "darkred"
    
    vis_edges <- data.frame(from=edge_list$from, to=edge_list$to)
    vis_edges$color.color <- c("limegreen", "gray")[factor(edge_list$type)]
    vis_edges$color.highlight <- c("limegreen", "gray")[factor(edge_list$type)]
    vis_edges$label = ifelse(edge_list$weight == 1, yes = "", no = round(edge_list$weight, digits = 2))
    vis_edges$arrows <- "to"
    
    schema_name <- input$select_schema_rels
    visNetwork(vis_nodes, vis_edges,
               main = paste0("Shared Categories in Schema: ",paste0(schema_name, collapse = ", "))) %>%
      visIgraphLayout() %>%
      visEdges(smooth = FALSE) %>%
      visExport() %>%
      visOptions(
        highlightNearest = TRUE,
        nodesIdSelection = list(enabled = TRUE),
        height="1000px", width = "1800px")
  })
  
  
  observeEvent(input$download_graph_vis, {
    visSave(build_categories_graph(), file = "network.html", background = "white")
  })
  
  
  output$vis_cats <- renderVisNetwork({
    build_categories_graph()
    })

}

shinyApp(ui, server)