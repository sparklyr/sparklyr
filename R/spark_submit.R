#' @name spark-connections
#'
#' @param file Path to R source file to submit for batch execution.
#'
#' @export
spark_submit <- function(master,
                         file,
                         spark_home = Sys.getenv("SPARK_HOME"),
                         app_name = "sparklyr",
                         version = NULL,
                         hadoop_version = NULL,
                         config = spark_config(),
                         extensions = sparklyr::registered_extensions(),
                         ...) {

  master <- spark_master_local_cores(master)
  shell_args <- spark_config_shell_args(config, master)
  if (is.null(spark_home) || !nzchar(spark_home)) spark_home <- spark_config_value(config, "spark.home", "")

  temp_path <- tempfile()
  dir.create(temp_path)
  batch_fie <- file.path(temp_path, "sparklyr-batch.R")
  file.copy(file, batch_fie)
  config$sparklyr.shell.files <- c(batch_fie, config$sparklyr.shell.files)

  shell_connection(master = master,
                   spark_home = spark_home,
                   app_name = app_name,
                   version = version,
                   hadoop_version = hadoop_version,
                   shell_args = shell_args,
                   config = config,
                   service = FALSE,
                   remote = FALSE,
                   extensions = extensions,
                   batch = TRUE)

  spark_disconnect(sc)
}
