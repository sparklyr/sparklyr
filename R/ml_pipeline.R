#' @export
ml_stages <- function(x, ...) {
  sc <- spark_connection(x$.pipeline)
  dots <- list(...) %>%
    lapply(function(x) x$.pipeline)
  invoke_static(sc,
                "sparklyr.MLUtils",
                "composeStages",
                x$.pipeline, dots) %>%
    ml_pipeline()
}
