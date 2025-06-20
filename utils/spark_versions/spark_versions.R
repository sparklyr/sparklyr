library(curl)
library(rvest)
library(stringr)
library(purrr)
library(fs)
library(readr)
library(jsonlite)
library(dplyr)

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
parse_file <- function(x, main, folder) {
  print(x)
  if(str_sub(x, 1, 6) != "spark-") stop("Invalid file name")
  #if(str_sub(x, 12, 22) != "-bin-hadoop") stop("Invalid file name")
  if(str_detect(x, "scala")) stop("No Scala version files")


  xfp <- path_ext_remove(x)

  xf_split <- str_split(xfp, "-")[[1]]

  if(any(str_detect(xf_split, "preview"))) {
    hadoop_no <- 5
    pattern <- paste0(xf_split[1], "-%s-", xf_split[3], "-", xf_split[4], "-hadoop%s.tgz")
  } else {
    hadoop_no <- 4
    pattern <- "spark-%s-bin-hadoop%s.tgz"
  }

  list(
    spark = xf_split[2],
    hadoop = str_sub(xf_split[hadoop_no], 7),
    base = paste0(main, folder),
    pattern = pattern
  )
}

# ---------------------- Read / create versions.rds file ----------------

versions_rds <- path("utils", "spark_versions", "versions.rds")
if(file_exists(versions_rds)) {
  rds_info <- file_info(versions_rds)
  rds_days <- as.integer(Sys.Date() - as.Date(rds_info$modification_time))
  if(rds_days > 8) {
    file_delete(versions_rds)
  }
}

if(!file_exists(versions_rds)) {
  c("https://dlcdn.apache.org/spark/",
    "https://archive.apache.org/dist/spark/"
  ) %>%
    map(~ get_main_page(.x) %>%
          map(get_spark_files, .x) %>%
          purrr::flatten()
    ) %>%
    purrr::flatten() %>%
    write_rds(versions_rds)
}

all_files <- read_rds(versions_rds)

# -------------------- Combine files -------------

apache_entries <- all_files %>%
  discard(~str_detect(.x$file, "incubating")) %>%
  discard(~str_detect(.x$file, "without")) %>%
  discard(~str_detect(.x$file, "scala")) %>%
  discard(~str_detect(.x$file, "connect")) %>%
  discard(~str_detect(.x$file, "hive")) %>%
  map(~ parse_file(.x$file, .x$main, .x$folder))

versions_json <- path("inst/extdata/versions.json")
future_json <- path("inst/extdata/versions-next.json")

# -------------------- Create new list -------------

final_tbl <- apache_entries %>%
  map(~{
    x <- .x
    x$priority <- 0
    if(str_detect(x$base, "dlcdn.apache.org")) x$priority <- 1
    if(str_detect(x$base, "archive.apache.org")) x$priority <- 2
    x
  }) %>%
  discard(~str_detect(.x$base, "archive.apache.org") && str_detect(.x$base, "preview")) %>%
  map_dfr(~.x) %>%
  arrange(spark, hadoop, priority) %>%
  filter(spark >= "2.4.0") %>% # Matching minimum version to the original file
  group_by(spark, hadoop) %>%
  filter(priority == min(priority)) %>%
  select(-priority) %>%
  ungroup()

if(any(str_detect(final_tbl$base, "preview"))) {
  previews <- final_tbl %>%
    filter(str_detect(base, "preview")) %>%
    filter(base != max(base)) %>%
    pull(base)

  future_tbl <- final_tbl %>%
    filter(str_detect(base, "preview")) %>%
    filter(!base %in% previews)
} else {
  future_tbl <- data.frame()
}

final_tbl <- final_tbl %>%
  filter(!str_detect(base, "preview"))

final_tbl %>%
  mutate(base = str_sub(base, 1, 25)) %>%
  count(base)

# ---------------- Save new versions.json to inst folder --------------

final_tbl %>%
  transpose() %>%
  write_json(versions_json, pretty = TRUE, auto_unbox = TRUE)

future_tbl %>%
  transpose() %>%
  write_json(future_json, pretty = TRUE, auto_unbox = TRUE)
