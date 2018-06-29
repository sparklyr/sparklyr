#' Reactive spark reader
#'
#' Given a spark object, returns a reactive data source for the contents
#' of the spark object. This function is most Useful to read Spark streams.
#'
#' @param x An object coercable to a Spark DataFrame.
#' @param intervalMillis Approximate number of milliseconds to wait to retrieve
#'   updated data frame. This can be a numeric value, or a function that returns
#'   a numeric value.
#' @param session The user session to associate this file reader with, or NULL if
#'   none. If non-null, the reader will automatically stop when the session ends.
#'
#' @importFrom shiny reactivePoll
#' @importFrom dplyr select
#' @importFrom dplyr filter
#' @importFrom dplyr distinct
#' @importFrom dplyr pull
#'
#' @export
reactiveSpark <- function(x,
                          intervalMillis = 1000,
                          session = NULL)
{
  sc <- spark_connection(x)

  traceable <- x %>%
    mutate(reactive_timestamp = current_timestamp())

  name <- random_string("sparklyr_tmp_")

  stream <- traceable %>% stream_write_memory(name, mode = "complete")

  reactivePoll(
    intervalMillis = intervalMillis,
    session = session,
    checkFunc = function() {
      progress <- invoke(stream, "lastProgress")
      if (is.null(progress) || identical(invoke(progress, "numInputRows"), 0L)) {
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
