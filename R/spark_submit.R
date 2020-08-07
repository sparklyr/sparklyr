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
                         config = spark_config(),
                         extensions = sparklyr::registered_extensions(),
                         scala_version = NULL,
                         ...) {
  hadoop_version <- list(...)$hadoop_version
  temp_path <- tempfile()
  dir.create(temp_path)
  batch_file <- file.path(temp_path, "sparklyr-batch.R")
  file.copy(file, batch_file)
  config$sparklyr.shell.files <- c(batch_file, config$sparklyr.shell.files)

  # spark_submit() is designed for non-interactive jobs, so we can log to console
  if (is.null(spark_config_value(config, "sparklyr.verbose", NULL))) config$sparklyr.verbose <- TRUE
  if (is.null(spark_config_value(config, "sparklyr.log.console", NULL))) config$sparklyr.log.console <- TRUE

  master <- spark_master_local_cores(master, config)
  shell_args <- spark_config_shell_args(config, master)
  if (is.null(spark_home) || !nzchar(spark_home)) spark_home <- spark_config_value(config, "spark.home", "")

  shell_connection(
    master = master,
    spark_home = spark_home,
    app_name = app_name,
    version = version,
    hadoop_version = hadoop_version,
    shell_args = shell_args,
    config = config,
    service = FALSE,
    remote = FALSE,
    extensions = extensions,
    batch = file,
    scala_version = scala_version
  )

  invisible(NULL)
}
