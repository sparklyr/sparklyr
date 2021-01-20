#' @include tidyr_utils.R
NULL

#' Unnest longer
#'
#' Expand a struct column or an array column within a Spark dataframe into one
#' or more rows, similar what to tidyr::unnest_longer does to an R dataframe.
#' An index column, if included, will be 1-based if `col` is an array column.
#'
#' @param data The Spark dataframe to be unnested
#' @param col The struct column to extract components from
#' @param values_to Name of column to store vector values. Defaults to `col`.
#' @param indices_to A string giving the name of column which will contain the
#'   inner names or position (if not named) of the values. Defaults to `col`
#'   with `_id` suffix
#' @param include_indices Whether to include an index column. An index column
#'   will be included by default if `col` is a struct column. It will also be
#'   included if `indices_to` is not `NULL`.
#' @param names_repair Strategy for fixing duplicate column names (the semantic
#'   will be exactly identical to that of `.name_repair` option in
#'   \code{\link[tibble:tibble]{tibble}})
#' @param ptype Optionally, supply an R data frame prototype for the output.
#'   Each column of the unnested result will be casted based on the Spark
#'   equivalent of the type of the column with the same name within `ptype`,
#'   e.g., if `ptype` has a column `x` of type `character`, then column `x`
#'   of the unnested result will be casted from its original SQL type to
#'   StringType.
#' @param transform Optionally, a named list of transformation functions applied
#'
#' @examples
#' \dontrun{
#' library(sparklyr)
#' sc <- spark_connect(master = "local", version = "2.4.0")
#'
#' # unnesting a struct column
#' sdf <- copy_to(
#'   sc,
#'   tibble::tibble(
#'     x = 1:3,
#'     y = list(list(a = 1, b = 2), list(a = 3, b = 4), list(a = 5, b = 6))
#'   )
#' )
#'
#' unnested <- sdf %>% sdf_unnest_longer(y, indices_to = "attr")
#'
#' # unnesting an array column
#' sdf <- copy_to(
#'   sc,
#'   tibble::tibble(
#'     x = 1:3,
#'     y = list(1:10, 1:5, 1:2)
#'   )
#' )
#'
#' unnested <- sdf %>% sdf_unnest_longer(y, indices_to = "array_idx")
#' }
#'
#' @export
sdf_unnest_longer <- function(
                              data,
                              col,
                              values_to = NULL,
                              indices_to = NULL,
                              include_indices = NULL,
                              names_repair = "check_unique",
                              ptype = list(),
                              transform = list()) {
  sc <- spark_connection(data)
  if (spark_version(sc) < "2.2.0") {
    stop("`sdf_unnest_longer()` requires Spark 2.2.0 or above!")
  }
  check_present(col)
  col <- tidyselect::vars_pull(colnames(data), !!rlang::enquo(col))
  schema <- data %>%
    spark_dataframe() %>%
    invoke("schema")
  col_idx <- schema %>% invoke("fieldIndex", col)
  col_data_type <- schema %>%
    invoke("%>%", list("apply", col_idx), list("dataType"))
  col_sql_type <- col_data_type %>% invoke("sql")
  values_to <- values_to %||% col
  if (!is.null(indices_to)) {
    include_indices <- include_indices %||% TRUE
  } else {
    indices_to <- sprintf("%s_id", col)
  }
  col_sql_name <- quote_sql_name(col)

  srcs <- list()
  dsts <- NULL
  has_numeric_indices <- FALSE
  indices_col_idx <- NULL
  if (grepl("STRUCT<.*>", col_sql_type)) {
    include_indices <- include_indices %||% TRUE
    field_names <- col_data_type %>% invoke("fieldNames")
    for (src in colnames(data)) {
      if (!identical(src, col)) {
        dsts <- c(dsts, src)
        srcs <- append(
          srcs,
          list(structure(list(
            sql = sprintf("%s AS %%s", quote_sql_name(src)),
            dst_idxes = length(dsts)
          )))
        )
      } else {
        if (identical(include_indices, TRUE)) {
          sql <- sprintf(
            "STACK(%d, %s) AS (%%s, %%s)",
            length(field_names),
            field_names %>%
              lapply(
                function(x) {
                  sprintf(
                    "%s.%s, %s",
                    col_sql_name,
                    quote_sql_name(x),
                    dbplyr::escape(x, con = dbplyr::simulate_dbi())
                  )
                }
              ) %>%
              paste(collapse = ", ")
          )
          srcs <- append(
            srcs,
            list(structure(list(
              sql = sql,
              dst_idxes = length(dsts) + seq(2)
            )))
          )
          dsts <- c(dsts, values_to, indices_to)
        } else {
          sql <- sprintf(
            "STACK(%d, %s) AS %%s",
            length(field_names),
            field_names %>%
              lapply(
                function(x) {
                  sprintf("%s.%s", col_sql_name, quote_sql_name(x))
                }
              ) %>%
              paste(collapse = ", ")
          )
          dsts <- c(dsts, values_to)
          srcs <- append(
            srcs,
            list(structure(list(
              sql = sql,
              dst_idxes = length(dsts)
            )))
          )
        }
      }
    }
  } else if (grepl("ARRAY<.*>", col_sql_type)) {
    for (src in colnames(data)) {
      if (!identical(src, col)) {
        dsts <- c(dsts, src)
        srcs <- append(
          srcs,
          list(structure(list(
            sql = sprintf("%s AS %%s", quote_sql_name(src)),
            dst_idxes = length(dsts)
          )))
        )
      } else {
        if (identical(include_indices, TRUE)) {
          srcs <- append(
            srcs,
            list(structure(list(
              sql = sprintf("POSEXPLODE_OUTER(%s) AS (%%s, %%s)", col_sql_name),
              dst_idxes = length(dsts) + seq(2)
            )))
          )
          has_numeric_indices <- TRUE
          indices_col_idx <- length(dsts) + 1
          dsts <- c(dsts, indices_to, values_to)
        } else {
          dsts <- c(dsts, values_to)
          srcs <- append(
            srcs,
            list(structure(list(
              sql = sprintf("EXPLODE_OUTER(%s) AS %%s", col_sql_name),
              dst_idxes = length(dsts)
            )))
          )
        }
      }
    }
  } else {
    stop(sprintf("`%s` must be a struct column or an array column.", col))
  }

  dsts <- repair_names(dsts, names_repair)
  out <- DBI::dbSendQuery(
    sc,
    sprintf(
      "SELECT %s FROM (%s)",
      lapply(
        srcs,
        function(src) {
          do.call(
            sprintf,
            append(
              list(src$sql),
              lapply(dsts[src$dst_idxes], quote_sql_name)
            )
          )
        }
      ) %>%
        paste(collapse = ", "),
      dbplyr::remote_query(data)
    )
  )
  out <- out@sdf %>%
    sdf_register() %>%
    apply_ptype(ptype) %>%
    apply_transform(transform)

  if (has_numeric_indices) {
    # make numeric indices 1-based
    indices_col <- dsts[[indices_col_idx]]
    increment_indices_sql <- list(dplyr::sql(sprintf("%s + 1", quote_sql_name(indices_col))))
    names(increment_indices_sql) <- indices_col
    out <- out %>>% dplyr::mutate %@% increment_indices_sql
  }

  out
}
