.globals <- new.env(parent = emptyenv())

.gls_env <- new.env(parent = emptyenv())

.gls_env$extension_packages <-  character()
.gls_env$spark_versions_json <- NULL
.gls_env$param_mapping_s_to_r <- NULL
.gls_env$param_mapping_r_to_s <- NULL
.gls_env$ml_class_mapping <- NULL
.gls_env$ml_package_mapping <- NULL

genv_get_extension_packages <- function() {
  .gls_env$extension_packages
}

genv_set_extension_packages <- function(x) {
  .gls_env$extension_packages <- x
  invisible()
}

genv_get_spark_versions_json <- function() {
  .gls_env$spark_versions_json
}

genv_set_spark_versions_json <- function(x) {
  .gls_env$spark_versions_json <- x
  invisible()
}

genv_get_param_mapping_s_to_r <- function() {
  .gls_env$param_mapping_s_to_r
}

genv_set_param_mapping_s_to_r <- function(x) {
  .gls_env$param_mapping_s_to_r <- x
  invisible()
}

genv_get_param_mapping_r_to_s <- function() {
  .gls_env$param_mapping_r_to_s
}

genv_set_param_mapping_r_to_s <- function(x) {
  .gls_env$param_mapping_r_to_s <- x
  invisible()
}

genv_get_ml_class_mapping <- function() {
  .gls_env$ml_class_mapping
}

genv_set_ml_class_mapping <- function(x) {
  .gls_env$ml_class_mapping <- x
  invisible()
}

genv_get_ml_package_mapping <- function() {
  .gls_env$ml_package_mapping
}

genv_set_ml_package_mapping <- function(x) {
  .gls_env$ml_package_mapping <- x
  invisible()
}


