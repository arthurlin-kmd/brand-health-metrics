library(tidyverse)
library(keyring)
library(dbplyr)
library(lubridate)
library(DBI)
library(kmdr)
con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "BIDW", 
                      Database = "data_warehouse", UID = keyring::key_get("email_address"), 
                      PWD = keyring::key_get("kmd-password"), Trusted_Connection = "TRUE", 
                      Port = 1433)


# params ------------------------------------------------------------------

tbl_database <- set_source_tables(con)

date_start <- as.Date("2021-01-01")

date_end <- date_start + months(3)



# data extraction ---------------------------------------------------------

daily_sales <- kmdr::base_txn_query(con, period_start = date_start, period_end = date_end) %>%
    filter(customer_type == "Summit Club") %>% 
    
    group_by(full_date) %>% 
    summarise(
        revenue = sum(sale_amount_excl_gst, na.rm =  TRUE),
        txn     = n_distinct(sale_transaction)
    ) %>% 
    collect() %>% 
    ungroup() %>% 
    glimpse()

new_member_tbl <- base_member_query(con) %>% 
    filter(join_week_start >= date_start & join_week_start <= date_end & country %in% c("Australia", "New Zealand")) %>% 
    
    #how do you perfrom left join to get customer age?
    group_by(country, join_week_start) %>% 
    summarise(
        new_members = n_distinct(customer_number)
    ) %>% 
    collect() %>% 
    glimpse()

daily_sales %>% recode_dates() %>% 
    ggplot(aes(x = full_date, y = n))+
    geom_line() +
    scale_y_continuous(labels = scales::label_comma())+
    theme_minimal() +
    theme(plot.title.position = "plot")+
    labs(title = "daily sc transactions",
         subtitle = glue::glue("{date_start} to {date_end}"),
         x = "date",
         y = "sc transactions"
    )
    
members_tbl <- base_member_query(con) %>%
    head(1000) %>% glimpse()
    collect()

