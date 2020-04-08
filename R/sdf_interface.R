#' Copy an Object into Spark
#'
#' Copy an object into Spark, and return an \R object wrapping the
#' copied object (typically, a Spark DataFrame).
#'
#' @section Advanced Usage:
#'
#' \code{sdf_copy_to} is an S3 generic that, by default, dispatches to
#' \code{sdf_import}. Package authors that would like to implement
#' \code{sdf_copy_to} for a custom object type can accomplish this by
#' implementing the associated method on \code{sdf_import}.
#'
#' @param sc The associated Spark connection.
#' @param x An \R object from which a Spark DataFrame can be generated.
#' @param name The name to assign to the copied table in Spark.
#' @param memory Boolean; should the table be cached into memory?
#' @param repartition The number of partitions to use when distributing the
#'   table across the Spark cluster. The default (0) can be used to avoid
#'   partitioning.
#' @param overwrite Boolean; overwrite a pre-existing table with the name \code{name}
#'   if one already exists?
#' @param struct_columns (only supported with Spark 2.4.0 or higher) A list of
#'   columns from the source data frame that should be converted to Spark SQL
#'   StructType columns.
#'   The source columns can contain either json strings or nested lists.
#'   All rows within each source column should have identical schemas (because
#'   otherwise the conversion result will contain unexpected null values or
#'   missing values as Spark currently does not support schema discovery on
#'   individual rows within a struct column).
#' @param ... Optional arguments, passed to implementing methods.
#'
#' @family Spark data frames
#'
#' @examples
#'
#' sc <- spark_connect(master = "spark://HOST:PORT")
#' sdf_copy_to(sc, iris)
#'
#' @name sdf_copy_to
#' @export
sdf_copy_to <- function(sc,
                        x,
                        name,
                        memory,
                        repartition,
                        overwrite,
                        struct_columns,
                        ...) {
  UseMethod("sdf_copy_to")
}

#' @export
sdf_copy_to.default <- function(sc,
                                x,
                                name = spark_table_name(substitute(x)),
                                memory = TRUE,
                                repartition = 0L,
                                overwrite = FALSE,
                                struct_columns = list(),
                                ...
) {
  sdf_import(x, sc, name, memory, repartition, overwrite, struct_columns, ...)
}

#' @name sdf_copy_to
#' @export
sdf_import <- function(x,
                       sc,
                       name,
                       memory,
                       repartition,
                       overwrite,
                       struct_columns,
                       ...) {
  UseMethod("sdf_import")
}

sdf_prepare_dataframe <- function(x) {
  as.data.frame(
    x,
    stringsAsFactors = FALSE,
    row.names = NULL,
    optional = TRUE
  )
}

#' @export
#' @importFrom dplyr tbl
sdf_import.default <- function(x,
                               sc,
                               name = random_string("sparklyr_tmp_"),
                               memory = TRUE,
                               repartition = 0L,
                               overwrite = FALSE,
                               struct_columns = list(),
                               ...)
{
  if (overwrite)
    spark_remove_table_if_exists(sc, name)
  else if (name %in% src_tbls(sc))
    stop("table ", name, " already exists (pass overwrite = TRUE to overwrite)")

  dots <- list(...)
  serializer <- dots$serializer
  spark_data_copy(
    sc,
    x,
    name = name,
    repartition = repartition,
    serializer = serializer,
    struct_columns = struct_columns
  )

  if (memory && !class(x)[[1]] %in% c("iterator", "list"))
    tbl_cache(sc, name)

  on_connection_updated(sc, name)

  tbl(sc, name)
}

#' Register a Spark DataFrame
#'
#' Registers a Spark DataFrame (giving it a table name for the
#' Spark SQL context), and returns a \code{tbl_spark}.
#'
#' @template roxlate-sdf
#'
#' @param x A Spark DataFrame.
#' @param name A name to assign this table.
#'
#' @family Spark data frames
#' @export
sdf_register <- function(x, name = NULL) {
  UseMethod("sdf_register")
}

#' @export
sdf_register.tbl_spark <- function(x, name = NULL) {
  sdf_register(spark_dataframe(x), name)
}

#' @export
sdf_register.list <- function(x, name = NULL) {
  result <- lapply(seq_along(x), function(i) {
    sdf_register(x[[i]], name[[i]])
  })
  names(result) <- name
  result
}

#' @export
#' @importFrom dplyr tbl
sdf_register.spark_jobj <- function(x, name = NULL) {
  name <- name %||% random_string("sparklyr_tmp_")
  sc <- spark_connection(x)

  if (spark_version(sc) < "2.0.0")
    invoke(x, "registerTempTable", name)
  else
    invoke(x, "createOrReplaceTempView", name)

  on_connection_updated(sc, name)
  tbl(sc, name)
}

#' Randomly Sample Rows from a Spark DataFrame
#'
#' Draw a random sample of rows (with or without replacement)
#' from a Spark DataFrame.
#'
#' @template roxlate-sdf
#'
#' @param x An object coercable to a Spark DataFrame.
#' @param fraction The fraction to sample.
#' @param replacement Boolean; sample with replacement?
#' @param seed An (optional) integer seed.
#'
#' @family Spark data frames
#'
#' @export
sdf_sample <- function(x, fraction = 1, replacement = TRUE, seed = NULL)
{
  sdf <- spark_dataframe(x)

  sampled <- if (is.null(seed)) {
    sdf %>%
      invoke("sample", as.logical(replacement), as.double(fraction))
  } else {
    sdf %>%
      invoke("sample", as.logical(replacement), as.double(fraction), as.integer(seed))
  }

  sdf_register(sampled)
}

#' Sort a Spark DataFrame
#'
#' Sort a Spark DataFrame by one or more columns, with each column
#' sorted in ascending order.
#'
#' @template roxlate-sdf
#'
#' @param x An object coercable to a Spark DataFrame.
#' @param columns The column(s) to sort by.
#'
#' @family Spark data frames
#'
#' @export
sdf_sort <- function(x, columns) {
  df <- spark_dataframe(x)

  columns <- as.character(columns)
  n <- length(columns)
  if (n == 0)
    stop("must supply one or more column names")

  sorted <- if (n == 1) {
    invoke(df, "sort", columns, list())
  } else {
    invoke(df, "sort", columns[[1]], as.list(columns[-1]))
  }

  sdf_register(sorted)
}

#' Add a Unique ID Column to a Spark DataFrame
#'
#' Add a unique ID column to a Spark DataFrame. The Spark
#' \code{monotonicallyIncreasingId} function is used to produce these and is
#' guaranteed to produce unique, monotonically increasing ids; however, there
#' is no guarantee that these IDs will be sequential. The table is persisted
#' immediately after the column is generated, to ensure that the column is
#' stable -- otherwise, it can differ across new computations.
#'
#' @template roxlate-ml-x
#' @param id The name of the column to host the generated IDs.
#'
#' @export
sdf_with_unique_id <- function(x, id = "id") {
  sdf <- spark_dataframe(x)
  sc <- spark_connection(sdf)

  id <- cast_string(id)

  mii <- invoke_static(
    sc,
    "org.apache.spark.sql.functions",
    "monotonicallyIncreasingId"
  )

  mii <- invoke(mii, "cast", "double")

  transformed <- sdf %>%
    invoke("withColumn", id, mii) %>%
    sdf_persist(storage.level = "MEMORY_ONLY")

  sdf_register(transformed)
}

#' Add a Sequential ID Column to a Spark DataFrame
#'
#' Add a sequential ID column to a Spark DataFrame. The Spark
#' \code{zipWithIndex} function is used to produce these. This differs from
#' \code{sdf_with_unique_id} in that the IDs generated are independent of
#' partitioning.
#'
#' @template roxlate-ml-x
#' @param id The name of the column to host the generated IDs.
#' @param from The starting value of the id column
#'
#' @export
sdf_with_sequential_id <- function(x, id = "id", from = 1L) {

  sdf <- spark_dataframe(x)
  sc <- spark_connection(sdf)
  id <- cast_string(id)
  from <- cast_scalar_integer(from)

  transformed <- invoke_static(sc,
                               "sparklyr.Utils",
                               "addSequentialIndex",
                               sdf,
                               from,
                               id)

  sdf_register(transformed)
}

#' Returns the last index of a Spark DataFrame
#'
#' Returns the last index of a Spark DataFrame. The Spark
#' \code{mapPartitionsWithIndex} function is used to iterate
#' through the last nonempty partition of the RDD to find the last record.
#'
#' @template roxlate-ml-x
#' @param id The name of the index column.
#'
#' @export
#' @importFrom rlang sym
#' @importFrom rlang :=
sdf_last_index <- function(x, id = "id") {

  sdf <- x %>%
    dplyr::transmute(!!sym(id) := as.numeric(!!sym(id))) %>%
    spark_dataframe()
  sc <- spark_connection(sdf)
  id <- cast_string(id)

  invoke_static(sc,
                "sparklyr.Utils",
                "getLastIndex",
                sdf,
                id)
}

#' Compute (Approximate) Quantiles with a Spark DataFrame
#'
#' Given a numeric column within a Spark DataFrame, compute
#' approximate quantiles (to some relative error).
#'
#' @template roxlate-ml-x
#' @param column The column for which quantiles should be computed.
#' @param probabilities A numeric vector of probabilities, for
#'   which quantiles should be computed.
#' @param relative.error The relative error -- lower values imply more
#'   precision in the computed quantiles.
#'
#' @export
sdf_quantile <- function(x,
                         column,
                         probabilities = c(0.00, 0.25, 0.50, 0.75, 1.00),
                         relative.error = 1E-5)
{
  sdf <- spark_dataframe(x)

  nm <-
    names(probabilities) %||%
    paste(signif(probabilities * 100, 3), "%", sep = "")

  column <- cast_string(column)
  probabilities <- as.list(as.numeric(probabilities))
  relative.error <- cast_scalar_double(relative.error)

  stat <- invoke(sdf, "stat")
  quantiles <- invoke(stat, "approxQuantile", column, probabilities, relative.error)
  names(quantiles) <- nm

  quantiles
}

#' Persist a Spark DataFrame
#'
#' Persist a Spark DataFrame, forcing any pending computations and (optionally)
#' serializing the results to disk.
#'
#' Spark DataFrames invoke their operations lazily -- pending operations are
#' deferred until their results are actually needed. Persisting a Spark
#' DataFrame effectively 'forces' any pending computations, and then persists
#' the generated Spark DataFrame as requested (to memory, to disk, or
#' otherwise).
#'
#' Users of Spark should be careful to persist the results of any computations
#' which are non-deterministic -- otherwise, one might see that the values
#' within a column seem to 'change' as new operations are performed on that
#' data set.
#'
#' @template roxlate-ml-x
#' @param storage.level The storage level to be used. Please view the
#'   \href{http://spark.apache.org/docs/latest/programming-guide.html#rdd-persistence}{Spark Documentation}
#'   for information on what storage levels are accepted.
#' @export
sdf_persist <- function(x, storage.level = "MEMORY_AND_DISK") {
  sdf <- spark_dataframe(x)
  sc <- spark_connection(sdf)

  storage.level <- cast_string(storage.level)

  sl <- invoke_static(
    sc,
    "org.apache.spark.storage.StorageLevel",
    storage.level
  )

  sdf %>%
    invoke("persist", sl) %>%
    sdf_register()
}

#' Checkpoint a Spark DataFrame
#'
#' @param x an object coercible to a Spark DataFrame
#' @param eager whether to truncate the lineage of the DataFrame
#' @export
sdf_checkpoint <- function(x, eager = TRUE) {
  eager <- cast_scalar_logical(eager)

  x %>%
    spark_dataframe() %>%
    invoke("checkpoint", eager) %>%
    sdf_register()
}

#' Broadcast hint
#'
#' Used to force broadcast hash joins.
#'
#' @template roxlate-ml-x
#'
#' @export
sdf_broadcast <- function(x) {
  sdf <- spark_dataframe(x)
  sc <- spark_connection(sdf)

  invoke_static(sc,
                "org.apache.spark.sql.functions",
                "broadcast", sdf) %>%
    sdf_register()
}

#' Repartition a Spark DataFrame
#'
#' @template roxlate-ml-x
#'
#' @param partitions number of partitions
#' @param partition_by vector of column names used for partitioning, only supported for Spark 2.0+
#'
#' @export
sdf_repartition <- function(x, partitions = NULL, partition_by = NULL) {
  sdf <- spark_dataframe(x)
  sc <- spark_connection(sdf)

  partitions <- partitions %||% 0L %>%
    cast_scalar_integer()

  if (spark_version(sc) >= "2.0.0") {
    partition_by <- cast_nullable_character_list(partition_by) %||% list()

    return(
      invoke_static(sc, "sparklyr.Repartition", "repartition", sdf, partitions, partition_by) %>%
        sdf_register()
    )
  } else {
    if (!is.null(partition_by))
      stop("partitioning by columns only supported for Spark 2.0.0 and later")

    invoke(sdf, "repartition", partitions) %>%
      sdf_register()
  }
}

#' Gets number of partitions of a Spark DataFrame
#'
#' @template roxlate-ml-x
#' @export
sdf_num_partitions <- function(x) {
  x %>%
    spark_dataframe() %>%
    invoke("%>%", list("rdd"), list("getNumPartitions"))
}

#' Coalesces a Spark DataFrame
#'
#' @template roxlate-ml-x
#' @param partitions number of partitions
#' @export
sdf_coalesce <- function(x, partitions) {
  sdf <- spark_dataframe(x)
  sc <- spark_connection(sdf)

  partitions <- cast_scalar_integer(partitions)

  if (partitions < 1)
    stop("number of partitions must be positive")

  sdf %>%
    invoke("coalesce", partitions) %>%
    sdf_register()
}

#' Compute summary statistics for columns of a data frame
#'
#' @param x An object coercible to a Spark DataFrame
#' @param cols Columns to compute statistics for, given as a character vector
#' @export
sdf_describe <- function(x, cols = colnames(x)) {
  in_df <- cols %in% colnames(x)
  if (any(!in_df)) {
    msg <- paste0("The following columns are not in the data frame: ",
                  paste0(cols[which(!in_df)], collapse = ", "))
    stop(msg)
  }
  cols <- cast_character_list(cols)

  x %>%
    spark_dataframe() %>%
    invoke("describe", cols) %>%
    sdf_register()
}

#' Remove duplicates from a Spark DataFrame
#'
#' @param x An object coercible to a Spark DataFrame
#' @param cols Subset of Columns to consider, given as a character vector
#' @export
sdf_drop_duplicates <- function(x, cols = NULL) {
  in_df <- cols %in% colnames(x)

  if (any(!in_df)) {
    msg <- paste0("The following columns are not in the data frame: ",
                  paste0(cols[which(!in_df)], collapse = ", "))
    stop(msg)
  }

  cols <- cast_character_list(cols, allow_null=TRUE)
  sdf <- spark_dataframe(x)

  sdf_deduplicated <- if(is.null(cols)) {
    invoke(sdf, "dropDuplicates")
  } else {
    invoke(sdf, "dropDuplicates", cols)
  }

  sdf_register(sdf_deduplicated)
}
