#' Set/Get Spark checkpoint directory
#'
#' @name checkpoint_directory
#' @param sc A \code{spark_connection}.
#' @param dir checkpoint directory, must be HDFS path of running on cluster
#' @export
spark_set_checkpoint_dir <- function(sc, dir) {
  invisible(sc %>%
              spark_context() %>%
              invoke("setCheckpointDir",
                     spark_normalize_path(dir))
  )
}

#' @rdname checkpoint_directory
#' @export
spark_get_checkpoint_dir <- function(sc) {
  dir <- sc %>%
    spark_context() %>%
    invoke("getCheckpointDir")

  invoke_static(spark_connection(sc),
                "sparklyr.Utils",
                "unboxString",
                dir)
}
