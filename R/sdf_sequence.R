#' Create DataFrame for Length
#'
#' Creates a DataFrame for the given length.
#'
#' @param sc The associated Spark connection.
#' @param length The desired length of the sequence.
#' @param repartition The number of partitions to use when distributing the
#'   data across the Spark cluster.
#' @param type The data type to use for the index, either \code{"integer"} or \code{"integer64"}.
#'
#' @export
sdf_len <- function(sc, length, repartition = NULL, type = c("integer", "integer64")) {
  sdf_seq(sc, 1, length, repartition = repartition, type = type)
}

#' Create DataFrame for Range
#'
#' Creates a DataFrame for the given range
#'
#' @param sc The associated Spark connection.
#' @param from,to The start and end to use as a range
#' @param by The increment of the sequence.
#' @param repartition The number of partitions to use when distributing the
#'   data across the Spark cluster. Defaults to the minimum number of partitions.
#' @param type The data type to use for the index, either \code{"integer"} or \code{"integer64"}.
#'
#' @export
sdf_seq <- function(sc, from = 1L, to = 1L, by = 1L, repartition = NULL, type = c("integer", "integer64")) {
  from <- cast_scalar_integer(from)
  to <- cast_scalar_integer(to + 1)
  by <- cast_scalar_integer(by)

  # validate bits parameter
  type <- match.arg(type)

  type_map <- list(
    "integer" = "Integer",
    "integer64" = "Long"
  )
  type_name <- type_map[[type]]

  if (is.null(repartition)) repartition <- invoke(spark_context(sc), "defaultMinPartitions")
  repartition <- cast_scalar_integer(repartition)

  rdd <- invoke(spark_context(sc), "range", from, to, by, repartition)
  rdd <- invoke_static(sc, "sparklyr.Utils", paste0("mapRdd", type_name, "ToRddRow"), rdd)

  schema <- invoke_static(sc, "sparklyr.Utils", paste0("buildStructTypeFor", type_name, "Field"))
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
#' @param type The data type to use for the index, either \code{"integer"} or \code{"integer64"}.
#'
#' @export
sdf_along <- function(sc, along, repartition = NULL, type = c("integer", "integer64")) {
  sdf_len(sc, length(along), repartition = repartition, type = type)
}
