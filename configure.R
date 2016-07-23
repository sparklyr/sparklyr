compile_jars <- function() {
  verbose <- !is.na(Sys.getenv("NOT_CRAN", unset = NA))

  # skip on Travis
  if (!is.na(Sys.getenv("TRAVIS", unset = NA))) {
    if (verbose)
      message("** skipping Scala compilation on Travis")
    return(FALSE)
  }

  # skip if no 'scalac' available
  if (!nzchar(Sys.which("scalac"))) {
    if (verbose)
      message("** skipping Scala compilation: 'scalac' not on PATH")
    return(FALSE)
  }

  # skip if no 'jar' available
  if (!nzchar(Sys.which("jar"))) {
    if (verbose)
      message("** skipping Scala compilation: 'jar' not on PATH")
    return(FALSE)
  }

  # sparklyr won't be available during configure stage, so just source
  # the pieces necessary for determing spark versions + install paths
  source("R/install_spark.R")
  source("R/install_spark_versions.R")

  tryCatch(
    source("inst/tools/compile-scala.R"),
    error = function(e) {
      if (nzchar(e$message)) {
        message(e$message)
      }
    }
  )

}

invisible(compile_jars())
