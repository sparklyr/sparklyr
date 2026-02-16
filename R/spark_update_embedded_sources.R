#' @include precondition.R
#' @include spark_compile.R
#' @include spark_gen_embedded_sources.R

spark_update_embedded_sources <- function(jars_to_skip = c()) {
  pkg_root <- normalizePath(".")
  on.exit(setwd(pkg_root))

  parent_dir <- tempdir()
  srcs_dir <- file.path(parent_dir, "sparklyr")
  ensure_directory(srcs_dir)
  srcs <- file.path(srcs_dir, "embedded_sources.R")
  spark_gen_embedded_sources(output = srcs)
  # apply the same update to all copies in sparklyr-*.jar and also to the copy
  # inside java/ to avoid confusion
  file.copy(from = srcs, to = file.path(pkg_root, "java"), overwrite = TRUE)

  sparklyr_jars <- list_sparklyr_jars()

  setwd(parent_dir)

  for (sparklyr_jar in sparklyr_jars) {
    if (!basename(sparklyr_jar) %in% jars_to_skip) {
      rlang::inform(c("*" = "Updating embedded sources in '", sparklyr_jar, "'"))
      system2(
        "jar",
        args = c("uf", sparklyr_jar, file.path("sparklyr", "embedded_sources.R"))
      )
    }
  }
}
