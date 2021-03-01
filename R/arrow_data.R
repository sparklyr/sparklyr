#' @include spark_data_build_types.R
NULL

arrow_enabled <- function(sc, object) {
  has_arrow <- "package:arrow" %in% search()
  spark_config_value(sc$config, "sparklyr.arrow", has_arrow) &&
    arrow_enabled_object(object)
}

#' Determine whether arrow is able to serialize the given R object
#'
#' If the given R object is not serializable by arrow due to some known
#' limitations of arrow, then return FALSE, otherwise return TRUE
#'
#' @param object The object to be serialized
#'
#' @keywords internal
#' @examples
#' \dontrun{
#'
#' df <- tibble::tibble(x = seq(5))
#' arrow_enabled_object(df)
#' }
#'
#' @export
arrow_enabled_object <- function(object) {
  UseMethod("arrow_enabled_object")
}

#' @export
arrow_enabled_object.default <- function(object) {
  TRUE
}

#' @export
arrow_enabled_object.tbl_spark <- function(object) {
  sdf <- spark_dataframe(object)
  arrow_enabled_object(sdf)
}

#' @export
arrow_enabled_object.spark_jobj <- function(object) {
  unsupported_expr <- ".Vector|StructType"

  if (packageVersion("arrow") < "0.15") {
    unsupported_expr <- ".Vector|ArrayType|StructType"
  }

  unsupported <- object %>%
    sdf_schema() %>%
    Filter(function(x) grepl(unsupported_expr, x$type), .)
  enabled <- length(unsupported) == 0
  if (!enabled) warning("Arrow disabled due to columns: ", paste(names(unsupported), collapse = ", "))

  enabled
}

#' @export
arrow_enabled_object.data.frame <- function(object) {
  unsupported <- NULL
  enabled <- TRUE
  for (column in colnames(object)) {
    if ("list" %in% class(object[[column]]) &&
      "raw" %in% lapply(object[[column]], class)) {
      unsupported <- c(unsupported, column)
      enabled <- FALSE
    }
  }
  if (!enabled) {
    warning(
      "Arrow disabled due to columns: ", paste(unsupported, collapse = ", ")
    )
  }

  enabled
}

#' @export
arrow_enabled_object.tbl_df <- arrow_enabled_object.data.frame

arrow_read_stream <- function(stream) {
  reader <- arrow_record_stream_reader(stream)
  record_entry <- arrow_read_record_batch(reader)

  entries <- list()
  while (!is.null(record_entry)) {
    entries[[length(entries) + 1]] <- tibble::as_tibble(record_entry)
    record_entry <- arrow_read_record_batch(reader)
  }

  entries
}

arrow_copy_to <- function(sc, df, parallelism) {
  # replace factors with characters
  if (any(sapply(df, is.factor))) {
    df <- dplyr::as_tibble(lapply(df, function(x) if (is.factor(x)) as.character(x) else x))
  }

  # serialize to arrow
  bytes <- arrow_write_record_batch(df, spark_version(sc))

  # create batches data frame
  batches <- list(bytes)

  # build schema
  schema <- spark_data_build_types(sc, lapply(df, class))

  # load arrow file in scala
  rdd <- invoke_static(sc, "sparklyr.ArrowHelper", "javaRddFromBinaryBatches", spark_context(sc), batches, parallelism)
  sdf <- invoke_static(sc, "sparklyr.ArrowConverters", "toDataFrame", rdd, schema, spark_session(sc))

  sdf
}

arrow_collect <- function(tbl, ...) {
  args <- list(...)

  sc <- spark_connection(tbl)
  sdf <- spark_dataframe(tbl)
  session <- spark_session(sc)

  time_zone <- spark_session(sc) %>%
    invoke("sessionState") %>%
    invoke("conf") %>%
    invoke("sessionLocalTimeZone")

  if (!identical(args$callback, NULL)) {
    cb <- args$callback
    if (is.language(cb)) cb <- rlang::as_closure(cb)

    arrow_df <- invoke_static(sc, "sparklyr.ArrowConverters", "toArrowDataset", sdf, session, time_zone)
    arrow_iter <- invoke(arrow_df, "toLocalIterator")

    iter <- 1
    while (invoke(arrow_iter, "hasNext")) {
      batches <- invoke_static(sc, "sparklyr.ArrowConverters", "toArrowStream", sdf, time_zone, invoke(arrow_iter, "underlying")) %>%
        arrow_read_stream()

      for (batch in batches) {
        if (length(formals(cb)) >= 2) cb(batch, iter) else cb(batch)
        iter <- iter + 1
      }
    }
  }
  else {
    invoke_static(sc, "sparklyr.ArrowConverters", "toArrowBatchRdd", sdf, session, time_zone) %>%
      arrow_read_stream() %>%
      dplyr::bind_rows()
  }
}
