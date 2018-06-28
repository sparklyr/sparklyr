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
#' @importFrom shiny reactivePoll
#' @export
reactiveSpark <- function(x, intervalMillis = 100)
{
  traceable <- x %>%
    mutate(reactive_timestamp = current_timestamp())

  name <- random_string("sparklyr_tmp")

  stream <- traceable %>% stream_write_memory(name)

  reactivePoll(
    intervalMillis = intervalMillis,
    checkFunc = function() {
      progress <- invoke(stream, "lastProgress")
      if (is.null(progress) || identical(invoke(progress, "numInputRows", 0L))) {
        ""
      } else {
        tbl(sc, name) %>%
          mutate(reactive_timestamp = current_timestamp()) %>%
          select(reactive_timestamp) %>%
          filter(reactive_timestamp == max(reactive_timestamp, na.rm = TRUE)) %>%
          distinct() %>%
          pull()
      }
    },
    valueFunc = function() {
      tbl(sc, name) %>%
        select(-reactive_timestamp) %>%
        spark_dataframe() %>%
        sdf_collect()
    })
}
