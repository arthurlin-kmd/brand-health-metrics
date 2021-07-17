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
    
    dplyr::inner_join(dplyr::select(tbl_database$dim_customer, 
                              dim_customer_key, research_age), by = c("dim_customer_key")) %>% 
        
    select(customer_number, country, join_week_start, research_age) %>% 
    mutate(
        research_age_recode = case_when(
            research_age <= "0" ~ "00. Undefined",
            research_age < "18" ~ "01. 0 - 17 yo",
            research_age < "25" ~ "02. 18 - 24 yo",
            research_age < "35" ~ "03. 25 - 34 yo",
            research_age < "40" ~ "04. 35 - 39 yo",
            research_age < "50" ~ "05. 40 - 49 yo",
            research_age < "60" ~ "06. 50 - 59 yo",
            research_age >= "60" ~ "07. 60+ yo"
        )
    ) %>%
    group_by(country, join_week_start, research_age_recode) %>%
    summarise(
        new_members = n_distinct(customer_number)
    ) %>% 
    collect()


new_member_tbl %>% recode_dates() %>% 
    ggplot(aes(x = research_age_recode, y = new_members))+
    geom_col() +
    scale_y_continuous(labels = scales::label_comma())+
    theme_minimal() +
    theme(plot.title.position = "plot")+
    labs(title = "Weekly SC acquisition",
         subtitle = glue::glue("{date_start} to {date_end}"),
         y = "New Members"
    )
    
