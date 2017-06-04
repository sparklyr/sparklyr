#' @export
debug_backend <- function(version = "2.1.0") {
  sparklyr:::start_shell(master = "local",
                         spark_home = spark_home_dir(version),
                         app_name = "sparklyr_worker",
                         service = TRUE)
}
