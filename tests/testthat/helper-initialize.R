testthat_spark_connection <- function(version = NULL) {

  # generate connection if none yet exists
  if (!exists(".testthat_spark_connection", envir = .GlobalEnv)) {
    version <- version %||% Sys.getenv("SPARK_VERSION", unset = "2.0.0")
    setwd(tempdir())
    sc <- spark_connect(master = "local", version = version)
    assign(".testthat_spark_connection", sc, envir = .GlobalEnv)
  }

  # retrieve spark connection
  get(".testthat_spark_connection", envir = .GlobalEnv)
}

testthat_tbl <- function(name) {
  sc <- testthat_spark_connection()
  tryCatch(
    expr  = dplyr::tbl(sc, name),
    error = function(e) {
      data <- eval(as.name(name), envir = .GlobalEnv)
      dplyr::copy_to(sc, data, name = name)
    }
  )
}

skip_unless_verbose <- function(message = NULL) {
  message <- message %||% "Verbose test skipped"
  verbose <- Sys.getenv("SPARKLYR_TESTS_VERBOSE", unset = NA)
  if (is.na(verbose)) skip(message)
  TRUE
}
