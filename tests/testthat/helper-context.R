testthat_context <- new.env()

## --------------------------- Connection --------------------------------------

testthat_spark_connection_open <- function(set_to = NULL) {
  cs <- testthat_context$spark$connection$open
  if(is.null(cs)) cs <- FALSE
  if(!is.null(set_to)) cs <- set_to
  testthat_context$spark$connection$open <- cs
  cs
}

testthat_spark_connection_object <- function(con = NULL) {
  co <- testthat_spark_connection_open()

  if(co && !is.null(con)) stop("There is a connection already open")

  if(co) return(testthat_context$spark$connection$object)

  if(!is.null(con)) {
    testthat_spark_connection_open(TRUE)
    testthat_context$spark$connection$object <- con
  }
}

testthat_spark_connection_type <- function() {
  ct <- testthat_context$spark$type
  if(is.null(ct)) {
    ct <- "local"
    lv <- using_livy()
    db <- using_databricks()
    if(lv && db) stop("Databricks and Livy cannot be tested simultaneously")
    if(lv) ct <- "livy"
    if(db) ct <- "databricks"
  }
  ct
}

testthat_spark_env_version <- function(set_to = NULL) {
  cv <- testthat_context$spark$version
  if(is.null(cv)) {
    esv <- Sys.getenv("SPARK_VERSION")
    sv <- NULL
    if(esv != "") sv <- esv
    if(!is.null(cv)) sv <- cv
    if(is.null(sv)) {
      mv <- max(spark_installed_versions()$spark)
      sv <- mv
    }
  } else {
    sv <- cv
  }
  if(!is.null(set_to)) sv <- set_to
  testthat_context$spark$version <- sv
  if(is.null(set_to)) sv
}

## ----------------------------- Using -----------------------------------------

using_livy_version <- function() {
  lv <- Sys.getenv("LIVY_VERSION")
  testthat_context$spark$livy$version <- lv
  lv
}

using_livy <- function() {
  lt <- FALSE
  if(using_livy_version() != "") lt <- TRUE
  lt
}

using_arrow_version <- function() {
  lv <- Sys.getenv("ARROW_VERSION")
  testthat_context$spark$arrow$version <- lv
  lv
}

using_arrow_devel <- function() {
  if(using_arrow_version == "devel") {
    TRUE
  } else {
    FALSE
  }
}

using_arrow <- function() {
  lt <- FALSE
  if(using_arrow_version() != "") lt <- TRUE
  lt
}

using_databricks <- function() {
  dcs <- FALSE
  dc <- Sys.getenv("TEST_DATABRICKS_CONNECT")
  if(dc == 'true') dcs <- TRUE
  testthat_context$databricks <- dcs
  dcs
}
