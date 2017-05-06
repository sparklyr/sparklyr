#' @export
compile_sparkworker_jars <- function() {
  spec <- sparklyr::spark_default_compilation_spec()
  spec <- spec[[4]]
  spec$jar_dep <- system.file("java/sparklyr-2.1-2.11.jar", package = "sparklyr")
  update_sources_class()

  javaopts <- Sys.getenv("CLASSPATH")
  Sys.setenv(JAVA_OPTS = "-Xss256m -Xmx4096m")
  on.exit(Sys.setenv(JAVA_OPTS = javaopts), add = TRUE)

  sparklyr::compile_package_jars(spec = spec)
}
