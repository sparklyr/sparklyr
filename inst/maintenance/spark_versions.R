library(curl)
library(rvest)

main_page_curl <- curl("https://dlcdn.apache.org/spark/")

main_page_content <- read_html(main_page_curl)

main_page_content %>%
  html_elements("a") %>%
  html_attr("href")
