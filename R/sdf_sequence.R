#' Create DataFrame for Length
#'
#' Creates a DataFrame for the given length.
#'
#' @param sc The associated Spark connection.
#' @param length The desired length of the sequence.
#' @param repartition The number of partitions to use when distributing the
#'   data across the Spark cluster.
#'
#' @export
sdf_len <- function(sc, length, repartition = NULL) {
  sdf_seq(sc, 1, length, repartition = repartition)
}

#' Create DataFrame for Range
#'
#' Creates a DataFrame for the given range
#'
#' @param sc The associated Spark connection.
#' @param from,to The start and end to use as a range
#' @param by The increment of the sequence.
#' @param repartition The number of partitions to use when distributing the
#'   data across the Spark cluster.
#'
#' @export
sdf_seq <- function(sc, from = 1L, to = 1L, by = 1L, repartition = NULL) {
  from <- ensure_scalar_integer(from)
  to <- ensure_scalar_integer(to + 1)
  by <- ensure_scalar_integer(by)

  if (is.null(repartition)) repartition <- invoke(spark_context(sc), "defaultMinPartitions")
  repartition <- ensure_scalar_integer(repartition)

  rdd <- invoke(spark_context(sc), "range", from, to, by, repartition)
  rdd <- invoke_static(sc, "sparklyr.WorkerUtils", "mapRddLongToRddRow", rdd)

  schema <- invoke_static(sc, "sparklyr.WorkerUtils", "buildStructTypeForLongField")
  sdf <- invoke(hive_context(sc), "createDataFrame", rdd, schema)

  sdf_register(sdf)
}

#' Create DataFrame for along Object
#'
#' Creates a DataFrame along the given object.
#'
#' @param sc The associated Spark connection.
#' @param along Takes the length from the length of this argument.
#' @param repartition The number of partitions to use when distributing the
#'   data across the Spark cluster.
#'
#' @export
sdf_along <- function(sc, along, repartition = NULL) {
  sdf_len(sc, length(along), repartition = repartition)
}
