compile_jars <- function() {

  # skip on CRAN
  if (is.na(Sys.getenv("NOT_CRAN", unset = NA))) {
    message("** Skipping Scala compilation on CRAN")
    return(FALSE)
  }

  # skip on Travis
  if (!is.na(Sys.getenv("TRAVIS", unset = NA))) {
    message("** Skipping Scala compilation on Travis")
    return(FALSE)
  }

  # skip if no 'scalac' or 'jar' available
  if (!nzchar(Sys.which("scalac"))) {
    message("** Skipping Scala compilation: 'scalac' not on PATH")
    return(FALSE)
  }

  if (!nzchar(Sys.which("jar"))) {
    message("** Skipping Scala compilation: 'jar' not on PATH")
    return(FALSE)
  }

  message("** Building 'rspark_utils.jar'...")

  # rspark won't be available during configure stage, so just source
  # the pieces necessary for determing spark versions + install paths
  source("R/install_spark.R")
  source("R/install_spark_versions.R")

  tryCatch(
    source("inst/tools/compile-scala.R"),
    error = function(e) message(e$message)
  )

}

invisible(compile_jars())
