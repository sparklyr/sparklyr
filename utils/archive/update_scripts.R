library(fs)
library(purrr)
library(stringr)
library(glue)
devtools::load_all()

r_files <- test_path() %>%
  dir_ls(glob = "*.R")


test_files <-  r_files[str_detect(r_files, "test-")]

test_files %>%
  map(~{
    contents <- readLines(.x)

    tf <- .x %>%
      path_file() %>%
      str_remove("test-") %>%
      str_remove(".R")

    line <- paste0("skip_connection(\"", tf, "\")")

    contents <- c(line, contents)

    writeLines(contents, .x)

  })


