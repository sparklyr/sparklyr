#' @include precondition.R
#' @include spark_compile.R
#' @include spark_gen_embedded_sources.R
#' @include utils.R

spark_verify_embedded_sources <- function() {
  expected <- tempfile(pattern = 'spark_embedded_sources_', fileext = ".R")
  spark_gen_embedded_sources(output = expected)
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
      actual <- file.path(".", "sparklyr", embedded_srcs[[1]])
      diff <- diffobj::diffFile(expected, actual)
      if (any(diff)) {
        print(diff)
        stop(c(
          "Embedded sources from '", sparklyr_jar, "' is not up-to-date!\n\n",
          "Please run 'Rscript update_embedded_sources.R' to fix this error ",
          "(also see ",
          "https://github.com/sparklyr/sparklyr/wiki/Development#updating-embedded-sources",
          " for mode detail)"
        ))
      }
    } else {
      message("no embedded source found within '", sparklyr_jar, "'")
    }
  }
}
