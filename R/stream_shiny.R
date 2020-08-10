#' Reactive spark reader
#'
#' Given a spark object, returns a reactive data source for the contents
#' of the spark object. This function is most useful to read Spark streams.
#'
#' @param x An object coercable to a Spark DataFrame.
#' @param intervalMillis Approximate number of milliseconds to wait to retrieve
#'   updated data frame. This can be a numeric value, or a function that returns
#'   a numeric value.
#' @param session The user session to associate this file reader with, or NULL if
#'   none. If non-null, the reader will automatically stop when the session ends.
#'
#' @export
reactiveSpark <- function(x,
                          intervalMillis = 1000,
                          session = NULL) {
  if (!"shiny" %in% installed.packages()) stop("The 'shiny' package is required for this operation.")

  getDefaultReactiveDomain <- get("getDefaultReactiveDomain", envir = asNamespace("shiny"))
  reactivePoll <- get("reactivePoll", envir = asNamespace("shiny"))
  if (identical(session, NULL)) session <- getDefaultReactiveDomain()

  sc <- spark_connection(x)

  sdf <- spark_dataframe(x)
  traceable <- invoke(
    sdf,
    "withColumn",
    "reactive_timestamp",
    invoke_static(sc, "org.apache.spark.sql.functions", "expr", "current_timestamp()")
  )

  name <- random_string("sparklyr_tmp_")

  stream <- traceable %>% stream_write_memory(name)

  onStop <- get("onStop", envir = asNamespace("shiny"))
  onStop(function() {
    stream_stop(stream)
  }, session = session)

  reactivePoll(
    intervalMillis = intervalMillis,
    session = session,
    checkFunc = function() {
      spark_session(sc) %>%
        invoke("table", name) %>%
        invoke(
          "agg",
          invoke_static(
            sc,
            "org.apache.spark.sql.functions",
            "expr",
            "max(reactive_timestamp)"
          ),
          list()
        ) %>%
        sdf_collect()
    },
    valueFunc = function() {
      spark_session(sc) %>%
        invoke("table", name) %>%
        invoke("drop", "reactive_timestamp") %>%
        sdf_collect()
    }
  )
}
