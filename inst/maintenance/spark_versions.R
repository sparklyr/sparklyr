library(curl)
library(rvest)
library(stringr)
library(purrr)
library(fs)
library(readr)

# ---------------------- Functions ----------------

## -------- Gets links from main page
get_main_page <- function(url) {
  main_page_curl <- curl(url)

  main_page_links <- main_page_curl %>%
    read_html() %>%
    html_elements("a") %>%
    html_attr("href")

  main_page_links[str_starts(main_page_links, "spark")]
}

## ---- Gets file names form folders in the links from main page
get_spark_files <- function(curr_folder, url) {
  spark_folder <- paste0(url, curr_folder)

  spark_page_curl <- curl(spark_folder)

  spark_page_links <- spark_page_curl %>%
    read_html() %>%
    html_elements("a") %>%
    html_attr("href")

  valid_spark <- str_starts(spark_page_links, "spark") &
    str_ends(spark_page_links, ".tgz") &
    str_detect(spark_page_links, "hadoop")

  sfs <- spark_page_links[valid_spark]

  map(sfs, ~ {
    list(
      main = url,
      folder = curr_folder,
      file = .x
    )
  })
}

## ---- Parses the file name to extract Spark, Hadoop and Scala version
parse_file <- function(x) {

  if(str_sub(x, 1, 6) != "spark-") stop("Invalid file name")
  if(str_sub(x, 12, 22) != "-bin-hadoop") stop(str_sub(x, 10, 21))

  xfp <- path_ext_remove(x)

  xf_split <- str_split(xfp, "-")[[1]]

  scala <- ""

  if(length(xf_split) > 4) {
    if(str_detect(xf_split[5], "scala")) {
      scala <- str_sub(xf_split[5], 6)
    }
  }

  list(
    spark = xf_split[2],
    hadoop = str_sub(xf_split[4], 7),
    scala = scala
  )
}

# ---------------------- Read / create versions.rds file ----------------

versions_rds <- path("inst", "maintenance", "versions.rds")

if(!file_exists(versions_rds)) {
  c("https://dlcdn.apache.org/spark/",
    "https://archive.apache.org/dist/spark/"
  ) %>%
    map(~ get_main_page(.x) %>%
          map(get_spark_files, .x) %>%
          flatten()
    ) %>%
    flatten() %>%
    write_rds(versions_rds)
}

all_files <- read_rds(versions_rds)

# -------------------- Data Wrangling -------------

all_files %>%
  #head(1) %>%
  discard(~str_detect(.x$file, "incubating")) %>%
  discard(~str_detect(.x$file, "without")) %>%
  discard(~str_detect(.x$file, "preview")) %>%
  map(~ c(.x, parse_file(.x$file)))



library(jsonlite)
current_versions <- read_json("inst/extdata/versions.json")

cdn_entries <- keep(current_versions, ~.x$base == "https://d3kbcqa49mib13.cloudfront.net/")




