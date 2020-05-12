#' @include precondition.R
#' @include spark_compile.R
#' @include spark_gen_embedded_sources.R
#' @include utils.R

spark_verify_embedded_sources <- function() {
  output <- tempfile(pattern = 'spark_embedded_sources_', fileext = ".R")
  spark_gen_embedded_sources(output = output)
  expected_content <- readLines(output)
  jar <- find_jar() %||% path_program("jar", fmt = "Unable to locate '%s' binary")

  pkg_root <- normalizePath(".")
  on.exit(setwd(pkg_root))

  sparklyr_jars <- list_sparklyr_jars()

  for (sparklyr_jar in sparklyr_jars) {
    wd <- tempdir()
    ensure_directory(wd)
    setwd(wd)

    system2(
      "jar",
      args = c("xf", sparklyr_jar, file.path("sparklyr", "embedded_sources.R"))
    )
    embedded_srcs <- dir(file.path(".", "sparklyr"), "^embedded_sources\\.R$")

    if (length(embedded_srcs) > 0) {
      message("verifying embedded sources from '", sparklyr_jar, "'")
      stopifnot(identical(expected_content, readLines(file.path(".", "sparklyr", embedded_srcs[[1]]))))
    } else {
      message("no embedded source found within '", sparklyr_jar, "'")
    }
  }
}
