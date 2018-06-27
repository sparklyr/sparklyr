#' Spark DataFrame is Streaming
#'
#' Is the given Spark DataFrame a streaming data?
#'
#' @template roxlate-ml-x
#' @export
sdf_is_streaming <- function(x)
{
  invoke(x, "isStreaming")
}
