spark_debug_backend <- function(version = "2.3.0") {
  start_shell(master = "local",
              spark_home = spark_home_dir(version),
              app_name = "sparklyr_worker",
              service = TRUE)
}
