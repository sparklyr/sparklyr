# ------------ Init session level variables ----------
.gls_env <- new.env(parent = emptyenv())

.gls_env$extension_packages <-  character()
.gls_env$spark_versions_json <- NULL
.gls_env$param_mapping_s_to_r <- NULL
.gls_env$param_mapping_r_to_s <- NULL
.gls_env$ml_class_mapping <- NULL
.gls_env$ml_package_mapping <- NULL
.gls_env$avail_package_cache <- NULL
.gls_env$do_spark <- NULL
.gls_env$last_error <- NULL

# ---------- Manage session level variables ----------
genv_get_last_error <- function() {
  .gls_env$last_error
}

genv_set_last_error <- function(x) {
  .gls_env$last_error <- x
  invisible()
}

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

genv_get_avail_package_cache <- function() {
  .gls_env$avail_package_cache
}

genv_set_avail_package_cache <- function(x) {
  .gls_env$avail_package_cache <- x
  invisible()
}

genv_get_do_spark <- function(element = NULL) {
  if(!is.null(element)) {
    .gls_env$do_spark[[element]]
  } else {
    .gls_env$do_spark
  }
}

genv_set_do_spark <- function(x) {
  .gls_env$do_spark <- x
  invisible()
}

genv_clear_do_spark_options <- function() {
  remove(list = ls(.gls_env$do_spark$options), pos = .gls_env$do_spark$options)
}

genv_set_do_spark_options <- function(optnames, opts) {
  for (i in seq(along = opts)) {
    assign(optnames[i], opts[[i]], pos = .gls_env$do_spark$options)
  }
}
