#' @include tidyr_utils.R
NULL

#' @importFrom tidyr unnest
#' @export
unnest.tbl_spark <- function(data,
                             cols,
                             ...,
                             keep_empty = FALSE,
                             ptype = NULL,
                             names_sep = NULL,
                             names_repair = "check_unique",
                             .drop = "DEPRECATED",
                             .id = "DEPRECATED",
                             .sep = "DEPRECATED",
                             .preserve = "DEPRECATED") {
  cols <- tidyselect::eval_select(
    rlang::enquo(cols), replicate_colnames(data)
  ) %>%
    names()
  if (length(cols) == 0) {
    return(data)
  }

  group_vars <- dplyr::group_vars(data)
  data <- data %>%
    dplyr::compute() %>%
    dplyr::ungroup()
  sc <- spark_connection(data)
  num_rows <- spark_dataframe(data) %>% invoke("count")
  schema <- data %>% sdf_schema(expand_nested_cols = TRUE)

  struct_fields <- list()
  for (col in cols) {
    if (!is.list(schema[[col]]$type)) {
      paste0(
        "`unnest.tbl_spark` is only supported for columns of type ",
        "`array<struct<.*>>` (i.e., a column produced by a previous ",
        "`nest.tbl_spark` operation. Column '", col, "' is of type ",
        schema[[col]]$type
      ) %>%
        rlang::abort()
    }

    nested_cols <- lapply(schema[[col]]$type, function(field) field$name) %>%
      list()
    names(nested_cols) <- col
    struct_fields <- append(struct_fields, nested_cols)
  }

  output_cols <- NULL
  for (col in colnames(data)) {
    if (col %in% cols) {
      for (nested_col in struct_fields[[col]]) {
        dest_col <- (
          if (is.null(names_sep)) {
            nested_col
          } else {
            paste0(col, names_sep, nested_col)
          }
        )
        output_cols <- append(output_cols, dest_col)
      }
    } else {
      output_cols <- append(output_cols, col)
    }
  }
  output_cols <- repair_names(output_cols, names_repair)

  output_cols_idx <- 1
  unnest_col_sqls <- list()
  other_col_sqls <- list()
  for (col in colnames(data)) {
    if (col %in% cols) {
      struct_name <- quote_sql_name(col)
      for (nested_col in struct_fields[[col]]) {
        unnest_col_sql <- sprintf(
          "EXPLODE_OUTER(%s.%s) AS %s",
          struct_name,
          quote_sql_name(nested_col),
          quote_sql_name(output_cols[[output_cols_idx]])
        )
        output_cols_idx <- output_cols_idx + 1
        unnest_col_sqls <- append(unnest_col_sqls, unnest_col_sql)
      }
    } else {
      other_col_sqls <- append(
        other_col_sqls,
        sprintf("%s AS %s", quote_sql_name(col), output_cols[[output_cols_idx]])
      )
      output_cols_idx <- output_cols_idx + 1
    }
  }
  unnest_col_sqls[[1]] <- paste(
    append(
      list(unnest_col_sqls[[1]]),
      other_col_sqls
    ),
    collapse = ", "
  )
  out <- data
  if (identical(keep_empty, FALSE)) {
    no_empty_value_sql <- paste(
      lapply(
        cols,
        function(col) {
          sprintf("ISNOTNULL(%s)", quote_sql_name(col))
        }
      ),
      collapse = " AND "
    ) %>%
      dplyr::sql()

    out <- dplyr::filter(out, no_empty_value_sql)
  }
  out <- out %>% dplyr::compute()
  out_tbl <- out %>%
    ensure_tmp_view() %>%
    quote_sql_name()

  unnested_cols <- lapply(
    unnest_col_sqls,
    function(sql) {
      tmp_view_name <- random_string("tidyr_unnest_tmp_")
      DBI::dbGetQuery(
        sc,
        sprintf(
          "CREATE TEMPORARY VIEW %s AS (SELECT %s FROM %s)",
          quote_sql_name(tmp_view_name),
          paste(sql, collapse = " ,"),
          out_tbl
        )
      )
      dplyr::tbl(sc, tmp_view_name)
    }
  )

  group_vars <- intersect(setdiff(group_vars, cols), output_cols)

  out <- do.call(sdf_fast_bind_cols, unnested_cols) %>>%
    dplyr::select %@% lapply(output_cols, as.symbol) %>>%
    dplyr::group_by %@% lapply(group_vars, as.symbol)

  apply_ptype(out, ptype)
}
