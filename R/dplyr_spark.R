#' @include spark_dataframe.R
#' @include spark_sql.R
#' @include tables_spark.R
NULL

#' @export
spark_connection.tbl_spark <- function(x, ...) {
  spark_connection(x$src)
}

#' @export
spark_connection.src_spark <- function(x, ...) {

  # for development version of dplyr (>= 0.5.0.9000)
  if ("dplyr" %in% loadedNamespaces() &&
    exists("con_acquire", envir = asNamespace("dplyr"))) {
    acquire <- get("con_acquire", envir = asNamespace("dplyr"))
    return(acquire(x))
  }

  # older versions of dplyr (0.5.0 and below)
  x$con
}

#' @rawNamespace
#' if (utils::packageVersion("dbplyr") < "2") {
#'   importFrom(dplyr, db_desc)
#'   S3method(db_desc, src_spark)
#' } else {
#'   importFrom(dbplyr, db_connection_describe)
#'   S3method(db_connection_describe, src_spark)
#' }

db_desc.src_spark <- function(x) {
  spark_db_desc(x)
}

db_connection_describe.src_spark <- function(con) {
  spark_db_desc(con)
}

#' @rawNamespace
#' if (utils::packageVersion("dbplyr") < "2") {
#'   importFrom(dplyr, db_explain)
#'   S3method(db_explain, spark_connection)
#' } else {
#'   importFrom(dbplyr, sql_query_explain)
#'   S3method(sql_query_explain, spark_connection)
#' }

db_explain.spark_connection <- function(con, sql, ...) {
  explain_sql <- spark_sql_query_explain(con, sql, ...)
  explained <- DBI::dbGetQuery(con, explain_sql)

  message(explained$plan)
}

sql_query_explain.spark_connection <- function(con, sql, ...) {
  spark_sql_query_explain(con, sql, ...)
}

#' @export
#' @importFrom dplyr tbl_vars
tbl_vars.spark_jobj <- function(x) {
  spark_dataframe_cols(x)
}

#' @export
#' @importFrom dplyr tbl_vars
tbl_vars.tbl_spark <- function(x) {
  spark_dataframe_cols(spark_dataframe(x))
}

#' @export
#' @importFrom dbplyr op_vars
op_vars.tbl_spark <- function(x) {
  spark_dataframe_cols(spark_dataframe(x))
}

spark_dataframe_cols <- function(sdf) {
  as.character(invoke(sdf, "columns") %>% unlist())
}

#' @export
#' @importFrom dbplyr tbl_sql
tbl.src_spark <- function(src, from, ...) {
  tbl_sql("spark", src = src, from = process_tbl_name(from), ...)
}

#' @export
#' @importFrom dbplyr src_sql
#' @importFrom dbplyr tbl_sql
tbl.spark_connection <- function(src, from, ...) {
  src <- src_sql("spark", src)
  tbl_sql("spark", src = src, from = process_tbl_name(from), ...)
}

process_tbl_name <- function(x) {
  tbl_name <- Filter(function(str) nchar(str) > 0, strsplit(x, "\\s+")[[1]])
  if (length(tbl_name) != 1) {
    x
  } else {
    components <- strsplit(tbl_name, "\\.")[[1]]
    num_components <- length(components)

    if (identical(num_components, 1L)) {
      x
    } else if (identical(num_components, 2L)) {
      dbplyr::in_schema(components[[1]], components[[2]])
    } else {
      stop("expected input to be <table name> or <schema name>.<table name>")
    }
  }
}

#' @export
#' @importFrom dplyr src_tbls
src_tbls.spark_connection <- function(x, ...) {
  sql <- hive_context(x)
  tbls <- invoke(sql, "sql", "SHOW TABLES")
  tableNames <- sdf_read_column(tbls, "tableName")

  filtered <- grep("^sparklyr_tmp_", tableNames, invert = TRUE, value = TRUE)
  sort(filtered)
}

#' @export
#' @importFrom dplyr db_data_type
db_data_type.spark_connection <- function(...) {
}


#' Copy an R Data Frame to Spark
#'
#' Copy an R \code{data.frame} to Spark, and return a reference to the
#' generated Spark DataFrame as a \code{tbl_spark}. The returned object will
#' act as a \code{dplyr}-compatible interface to the underlying Spark table.
#'
#' @param dest A \code{spark_connection}.
#' @param df An \R \code{data.frame}.
#' @param name The name to assign to the copied table in Spark.
#' @param memory Boolean; should the table be cached into memory?
#' @param repartition The number of partitions to use when distributing the
#'   table across the Spark cluster. The default (0) can be used to avoid
#'   partitioning.
#' @param overwrite Boolean; overwrite a pre-existing table with the name \code{name}
#'   if one already exists?
#' @param ... Optional arguments; currently unused.
#'
#' @return A \code{tbl_spark}, representing a \code{dplyr}-compatible interface
#'   to a Spark DataFrame.
#'
#' @export
#' @importFrom dplyr copy_to
copy_to.spark_connection <- function(dest,
                                     df,
                                     name = spark_table_name(substitute(df)),
                                     overwrite = FALSE,
                                     memory = TRUE,
                                     repartition = 0L,
                                     ...) {
  sdf_copy_to(dest, df, name, memory, repartition, overwrite, ...)
}

#' @export
#' @importFrom dplyr copy_to
copy_to.src_spark <- function(dest, df, name, overwrite, ...) {
  copy_to(spark_connection(dest), df, name, ...)
}

#' @export
print.src_spark <- function(x, ...) {
  cat(spark_db_desc(x))
  cat("\n\n")

  spark_log(spark_connection(x))
}

#' @rawNamespace
#' if (utils::packageVersion("dbplyr") < "2") {
#'   importFrom(dplyr, db_save_query)
#'   S3method(db_save_query, spark_connection)
#' } else {
#'   importFrom(dbplyr, sql_query_save)
#'   S3method(sql_query_save, spark_connection)
#' }

db_save_query.spark_connection <- function(con, sql, name, temporary = TRUE, ...) {
  create_temp_view_sql <- spark_sql_query_save(con, sql, name, temporary, ...)
  DBI::dbGetQuery(con, create_temp_view_sql)
  tbl_cache_sql(con, name = name, force = TRUE)

  # dbplyr expects db_save_query to retrieve the table name
  name
}

sql_query_save.spark_connection <- function(con, sql, name, temporary = TRUE, ...) {
  spark_sql_query_save(con, sql, name, temporary, ...)
}

#' @rawNamespace
#' if (utils::packageVersion("dbplyr") < "2") {
#'   importFrom(dplyr, db_analyze)
#'   S3method(db_analyze, spark_connection)
#' } else {
#'   importFrom(dbplyr, sql_table_analyze)
#'   S3method(sql_table_analyze, spark_connection)
#' }

db_analyze.spark_connection <- function(con, table, ...) {
  spark_db_analyze(con, table, ...)
}

sql_table_analyze.spark_connection <- function(con, table, ...) {
  spark_db_analyze(con, table, ...)
}

#' @export
#' @importFrom dplyr same_src
same_src.src_spark <- function(x, y) {
  if (!inherits(y, "src_spark")) {
    return(FALSE)
  }

  # By default, dplyr uses identical(x$con, y$con); however,
  # sparklyr connection might slightly change due to the state
  # it manages.

  identical(x$con$master, y$con$master) &&
    identical(x$con$app_name, y$con$app_name) &&
    identical(x$con$method, y$con$method)
}
