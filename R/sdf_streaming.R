#' Spark DataFrame is Streaming
#'
#' Is the given Spark DataFrame a streaming data?
#'
#' @template roxlate-ml-x
#' @export
sdf_is_streaming <- function(x)
{
  sc <- spark_connection(x)
  spark_version(sc) >= "2.0.0" && invoke(x, "isStreaming")
}
