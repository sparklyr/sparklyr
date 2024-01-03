#' @include mutation.R
#' @include sdf_wrapper.R
#' @include tidyr_utils.R
NULL

#' Unnest wider
#'
#' Flatten a struct column within a Spark dataframe into one or more columns,
#' similar what to tidyr::unnest_wider does to an R dataframe
#'
#' @param data The Spark dataframe to be unnested
#' @param col The struct column to extract components from
#' @param names_sep If `NULL`, the default, the names will be left as is.
#'   If a string, the inner and outer names will be pasted together using
#'   `names_sep` as the delimiter.
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
#'   to each component (e.g., list(`x = as.character`) to cast column `x` to
#'   String).
#'
#' @examples
#' \dontrun{
#' library(sparklyr)
#' sc <- spark_connect(master = "local", version = "2.4.0")
#'
#' sdf <- copy_to(
#'   sc,
#'   dplyr::tibble(
#'     x = 1:3,
#'     y = list(list(a = 1, b = 2), list(a = 3, b = 4), list(a = 5, b = 6))
#'   )
#' )
#'
#' # flatten struct column 'y' into two separate columns 'y_a' and 'y_b'
#' unnested <- sdf %>% sdf_unnest_wider(y, names_sep = "_")
#' }
#'
#' @export
sdf_unnest_wider <- function(
                             data,
                             col,
                             names_sep = NULL,
                             names_repair = "check_unique",
                             ptype = list(),
                             transform = list()) {
  if (data %>% spark_connection() %>% spark_version() < "2.0.0") {
    stop("`sdf_unnest_wider()` requires Spark 2.0.0 or above!")
  }
  check_present(col)
  col <- tidyselect::vars_pull(colnames(data), !!rlang::enquo(col))
  schema <- data %>%
    spark_dataframe() %>%
    invoke("schema")
  col_idx <- schema %>% invoke("fieldIndex", col)
  col_data_type <- schema %>%
    invoke("%>%", list("apply", col_idx), list("dataType"))
  if (!grepl("STRUCT<.*>", col_data_type %>% invoke("sql"))) {
    stop(sprintf("`%s` must be a struct column.", col))
  }
  field_names <- col_data_type %>% invoke("fieldNames")
  col_sql_name <- quote_sql_name(col)

  srcs <- list()
  dsts <- NULL
  for (src in colnames(data)) {
    if (!identical(src, col)) {
      srcs <- append(srcs, src %>% as.symbol() %>% list())
      dsts <- c(dsts, src)
    } else {
      for (field_name in field_names) {
        srcs <- append(
          srcs,
          sprintf("%s.%s", col_sql_name, quote_sql_name(field_name)) %>%
            dplyr::sql() %>%
            list()
        )
        dsts <- c(
          dsts,
          if (!is.null(names_sep)) {
            sprintf("%s%s%s", col, names_sep, field_name)
          } else {
            field_name
          }
        )
      }
    }
  }
  dsts <- repair_names(dsts, names_repair)
  names(srcs) <- dsts

  data %>>%
    dplyr::mutate %@% srcs %>>%
    dplyr::select %@% lapply(dsts, as.symbol) %>%
    apply_ptype(ptype) %>%
    apply_transform(transform)
}
