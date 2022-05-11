library(curl)
library(rvest)
library(stringr)
library(purrr)

apache_url <- "https://dlcdn.apache.org/spark/"

main_page_curl <- curl(apache_url)

main_page_links <- main_page_curl %>%
  read_html() %>%
  html_elements("a") %>%
  html_attr("href")

main_spark_folders <- main_page_links[str_starts(main_page_links, "spark")]

get_spark_files <- function(curr_folder) {
  spark_folder <- paste0(apache_url, curr_folder)

  spark_page_curl <- curl(spark_folder)

  spark_page_links <- spark_page_curl %>%
    read_html() %>%
    html_elements("a") %>%
    html_attr("href")

  valid_spark <- str_starts(spark_page_links, "spark") &
    str_ends(spark_page_links, ".tgz") &
    str_detect(spark_page_links, "hadoop")

  sfs <- spark_page_links[valid_spark & !str_detect(spark_page_links, "without")]

  map(sfs, ~ {
    list(
      main = apache_url,
      folder = curr_folder,
      file = .x
    )
  })
}

all_files <- main_spark_folders %>%
  map(get_spark_files) %>%
  flatten()



parse_file <- function(x) {
  xfp <- fs::path_ext_remove(x)
  xf_split <- str_split(xfp, "-")[[1]]
  list(
    spark = xf_split[2],
    hadoop = str_sub(xf_split[4], 7),
    scala = ifelse(length(xf_split) > 4, str_sub(xf_split[5], 6), NA)
  )
}

all_files %>%
  map(~ c(.x, parse_file(.x$file)))



