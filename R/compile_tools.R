#' @export
compile_sparkworker_jars <- function() {
  spec <- sparklyr::spark_default_compilation_spec()
  spec <- spec[[4]]
  spec$jar_dep <- system.file("java/sparklyr-2.1-2.11.jar", package = "sparklyr")
  update_sources_class()
  sparklyr::compile_package_jars(spec = spec)
}
