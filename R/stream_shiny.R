#' Reactive spark reader
#'
#' Given a spark object, returns a reactive data source for the contents
#' of the spark object. This function is most Useful to read Spark streams.
#'
#' @param x An object coercable to a Spark DataFrame.
#' @param intervalMillis Approximate number of milliseconds to wait to retrieve
#'   updated data frame. This can be a numeric value, or a function that returns
#'   a numeric value.
#'
#' @export
reactiveSpark <- function(x, intervalMillis = 100)
{
  name <- random_string("sparklyr_tmp")
  sdf <- tbl(sc, name) %>% spark_dataframe()
  stream <- x %>% stream_write_memory(name)

  shiny::reactivePoll(
    intervalMillis = intervalMillis,
    checkFunc = function() {
      TRUE
    },
    valueFunc = function() {
      sdf_collect(sdf)
    })
}
