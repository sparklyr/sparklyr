# Databricks notebook source
library(stringi)
file_name <- paste("/tmp/log_", stri_rand_strings(1, 10), ".txt", sep = "")
file_name

# COMMAND ----------

test_pass <- TRUE
capture.output(
  {
    tryCatch(
      {
        dbutils.widgets.removeAll()
        dbutils.widgets.text("Github repo", "sparklyr/sparklyr")
        dbutils.widgets.text("Commit ref", "main")

        print(dbutils.widgets.get("Github repo"))
        print(dbutils.widgets.get("Commit ref"))

        retryFunc <- function(func, maxTries = 5, init = 0) {
          suppressWarnings(tryCatch(
            {
              if (init < maxTries) func
            },
            error = function(e) {
              retryFunc(func, maxTries, init = init + 1)
            }
          ))
        }

        # Must set upgrade = "never". If you do not, packages like rlang may get upgraded,
        # and then sparklyr cannot be imported until the notebook is detached and reattached
        # (can't do that in a job)
        retryFunc(devtools::install_github(dbutils.widgets.get("Github repo"), ref = dbutils.widgets.get("Commit ref"), upgrade = "never"))

        library(sparklyr)
        library(dplyr)

        sc <- spark_connect(method = "databricks")

        copy_to(sc, iris, overwrite = TRUE) %>% count()
      },
      warning = function(w) {
        print("WARNING in trycatch:")
        print(w)
      },
      error = function(e) {
        g <- globalenv()
        g$test_pass <- FALSE
        print("ERROR in trycatch:")
        print(e)
      }
    )
  },
  file = file_name
)

# COMMAND ----------

test_output <- readChar(file_name, file.info(file_name)$size)
if (test_pass) {
  dbutils.notebook.exit(paste("NOTEBOOK TEST PASS with output:\n", test_output))
} else {
  dbutils.notebook.exit(paste("NOTEBOOK TEST FAIL with output:\n", test_output))
}
