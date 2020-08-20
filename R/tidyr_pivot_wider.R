#' @include sql_utils.R
#' @include tidyr_utils.R
#' @include utils.R
NULL

#' @importFrom tidyr pivot_wider
#' @export
pivot_wider.tbl_spark <- function(data,
                                  id_cols = NULL,
                                  names_from = name,
                                  names_prefix = "",
                                  names_sep = "_",
                                  names_glue = NULL,
                                  names_sort = FALSE,
                                  names_repair = "check_unique",
                                  values_from = value,
                                  values_fill = NULL,
                                  values_fn = NULL,
                                  ...) {
  names_from <- rlang::enquo(names_from)
  values_from <- rlang::enquo(values_from)
  spec <- sdf_build_wider_spec(
    data,
    names_from = !!names_from,
    values_from = !!values_from,
    names_prefix = names_prefix,
    names_sep = names_sep,
    names_glue = names_glue,
    names_sort = names_sort
  )

  id_cols <- rlang::enquo(id_cols)
  sdf_pivot_wider(
    data,
    spec,
    !!id_cols,
    names_repair = names_repair,
    values_fill = values_fill,
    values_fn = values_fn
  )
}

sdf_build_wider_spec <- function(data,
                                 names_from = name,
                                 values_from = value,
                                 names_prefix = "",
                                 names_sep = "_",
                                 names_glue = NULL,
                                 names_sort = FALSE) {
  colnames_df <- replicate_colnames(data)
  names_from <- names(tidyselect::eval_select(rlang::enquo(names_from), colnames_df))
  values_from <- names(tidyselect::eval_select(rlang::enquo(values_from), colnames_df))

  row_ids <- do.call(
     dplyr::distinct,
     append(list(data), lapply(names_from, as.symbol))
   ) %>%
     collect()
  if (names_sort) {
    row_ids <- vctrs::vec_sort(row_ids)
  }

  row_names <- rlang::exec(paste, !!!row_ids, sep = names_sep)

  out <- tibble::tibble(.name = paste0(names_prefix, row_names))

  if (length(values_from) == 1) {
    out$.value <- values_from
  } else {
    out <- vctrs::vec_repeat(out, times = vctrs::vec_size(values_from))
    out$.value <- vctrs::vec_repeat(values_from, each = vctrs::vec_size(row_ids))
    out$.name <- paste0(out$.value, names_sep, out$.name)

    row_ids <- vctrs::vec_repeat(row_ids, times = vctrs::vec_size(values_from))
  }

  out <- vctrs::vec_cbind(out, tibble::as_tibble(row_ids), .name_repair = "minimal")
  if (!is.null(names_glue)) {
    out$.name <- as.character(glue::glue_data(out, names_glue))
  }

  out
}

sdf_pivot_wider <- function(data,
                            spec,
                            names_repair = "check_unique",
                            id_cols = NULL,
                            values_fill = NULL,
                            values_fn = NULL) {
  sc <- spark_connection(data)
  if (spark_version(sc) < "2.0.0") {
    rlang::abort("`pivot_wider.tbl_spark` requires Spark 2.0.0 or higher")
  }

  spec <- canonicalize_spec(spec)

  if (is.null(values_fill)) {
    values_fill <- rlang::rep_named(unique(spec$.value), list(NA))
  } else if (is_scalar(values_fill)) {
    values_fill <- rlang::rep_named(unique(spec$.value), list(values_fill))
  }
  if (!is.list(values_fill)) {
    rlang::abort("`values_fill` must be NULL, a scalar, or a named list")
  }

  if (is.null(values_fn) || is.function(values_fn)) {
    values_fn <- rlang::rep_named(unique(spec$.value), list(values_fn))
  }
  if (!is.list(values_fn)) {
    rlang::abort("`values_fn` must be a NULL, a function, or a named list")
  }

  group_vars <- dplyr::group_vars(data)

  names_from <- names(spec)[-(1:2)]
  values_from <- vctrs::vec_unique(spec$.value)
  spec_cols <- c(names_from, values_from)

  id_cols <- rlang::enquo(id_cols)
  colnames_df <- replicate_colnames(data)
  if (!rlang::quo_is_null(id_cols)) {
    key_vars <- names(tidyselect::eval_select(id_cols, colnames_df))
  } else {
    key_vars <- dplyr::tbl_vars(colnames_df)
  }
  key_vars <- setdiff(key_vars, spec_cols)
  data_schema <- data %>% sdf_schema()

  # apply summarizing function(s)
  grouped_data <- do.call(
    dplyr::group_by,
    append(list(data), lapply(union(key_vars, names_from), as.symbol))
  )
  summarizers <- list()
  for (idx in seq_along(values_fn)) {
    col <- names(values_fn)[[idx]]
    summarizers[[col]] <- (
      if (is.null(values_fn[[idx]])) {
        is_double_type <- identical(data_schema[[col]]$type, "DoubleType")
        col <- quote_sql_name(col)
        (
          if (is_double_type) {
            sprintf(
              "FIRST(IF(ISNULL(%s) OR ISNAN(%s), NULL, %s), TRUE)", col, col, col
            )
          } else {
            sprintf("FIRST(IF(ISNULL(%s), NULL, %s), TRUE)", col, col)
          }
        ) %>%
          dplyr::sql()
      } else {
        rlang::expr((!! values_fn[[col]])(!! rlang::sym(col)))
      }
    )
  }
  names(summarizers) <- names(values_fn)
  summarized_data <- do.call(
    dplyr::summarize, append(list(grouped_data), summarizers)
  )

  # perform any name repair if necessary
  other_cols <- colnames(summarized_data) %>%
    setdiff(key_vars) %>%
    setdiff(spec_cols)
  output_colnames <- c(key_vars, other_cols, spec$.name) %>%
    repair_names(names_repair = names_repair)
  key_vars_renamed <- head(output_colnames, length(key_vars))
  spec$.name <- tail(output_colnames, length(spec$.name))
  name_repair_args <- lapply(c(key_vars, other_cols), as.symbol)
  names(name_repair_args) <- head(output_colnames, length(key_vars) + length(other_cols))
  if (length(name_repair_args) > 0) {
    summarized_data <- do.call(
      dplyr::rename, append(list(summarized_data), name_repair_args)
    )
  }
  key_vars <- key_vars_renamed

  summarized_data_id_col <- random_string("sdf_pivot_id")
  summarized_data_id_col_args <- list(dplyr::sql("monotonically_increasing_id()"))
  names(summarized_data_id_col_args) <- summarized_data_id_col
  summarized_data <- do.call(
    dplyr::mutate,
    append(list(summarized_data), summarized_data_id_col_args)
  )

  summarized_data <- summarized_data %>% dplyr::compute()

  value_specs <- unname(split(spec, spec$.value))
  out <- NULL

  pivot_col <- random_string("sdf_pivot")
  for (value_spec in value_specs) {
    value <- value_spec$.value[[1]]

    lhs_cols <- union(summarized_data_id_col, key_vars) %>%
      union(names_from) %>%
      union(value)
    lhs <- do.call(
      dplyr::select,
      append(list(summarized_data), lapply(lhs_cols, as.symbol))
    )

    all_obvs <- do.call(
      dplyr::distinct,
      append(
        list(lhs %>% dplyr::ungroup()),
        lapply(union(summarized_data_id_col, key_vars), as.symbol)
      )
    )

    rhs_select_args = list(as.symbol(".name"))
    names(rhs_select_args) <- pivot_col
    rhs_select_args <- append(rhs_select_args, lapply(names_from, as.symbol))
    rhs <- do.call(dplyr::select, append(list(value_spec), rhs_select_args)) %>%
      copy_to(sc, ., name = random_string("pivot_wider_spec_sdf"))
    combined <- spark_dataframe(lhs) %>%
      invoke("join", spark_dataframe(rhs), as.list(names_from), "inner") %>%
      invoke("drop", as.list(names_from))
    all_vals <- invoke(spark_dataframe(rhs), "crossJoin", spark_dataframe(all_obvs))
    missing_vals <- invoke(
      all_vals,
      "join",
      spark_dataframe(lhs),
      as.list(union(key_vars, names_from)),
      "left_anti"
    ) %>%
      invoke("drop", as.list(names_from))
    combined_cols <- invoke(combined, "columns")
    missing_vals <- missing_vals %>% sdf_register()
    combined_schema_obj <- invoke(combined, "schema")
    val_field_idx <- invoke(combined_schema_obj, "fieldIndex", value)
    val_sql_type <- invoke(combined_schema_obj, "fields")[[val_field_idx]] %>%
      invoke("%>%", list("dataType"), list("sql"))
    fv <- values_fill[[value]]
    val_fill_sql <-
    # sprintf(
    #   "CAST((%s) AS %s)",
    #   dbplyr::translate_sql_(list(fv), con = dbplyr::simulate_dbi()),
    #   val_sql_type
    # ) %>%
    dbplyr::translate_sql_(list(fv), con = dbplyr::simulate_dbi()) %>%
      dplyr::sql() %>%
      list()
    names(val_fill_sql) <- value
    missing_vals <- do.call(dplyr::mutate, append(list(missing_vals), val_fill_sql))
    combined <- invoke(combined, "unionByName", spark_dataframe(missing_vals))
    combined_schema <- combined %>% sdf_schema()

    value_col <- invoke_new(sc, "org.apache.spark.sql.Column", value)
    value_is_invalid <- invoke_static(sc, "org.apache.spark.sql.functions", "isnull", value_col)
    if (identical(combined_schema[[value]]$type, "DoubleType")) {
      value_is_nan <- invoke_static(sc, "org.apache.spark.sql.functions", "isnan", value_col)
      value_is_invalid <- invoke(value_is_invalid, "or", value_is_nan)
    }
    first_valid_value <- value_is_invalid %>%
      invoke_static(sc, "org.apache.spark.sql.functions", "when", ., NULL) %>%
      invoke("otherwise", value_col) %>%
      invoke_static(sc, "org.apache.spark.sql.functions", "first", ., TRUE)
    group_by_cols <- lapply(
      union(summarized_data_id_col, key_vars),
      function(col) invoke_new(sc, "org.apache.spark.sql.Column", col)
    )
    pivoted <- combined %>%
      invoke("groupBy", group_by_cols) %>%
      invoke("pivot", pivot_col, as.list(value_spec$.name)) %>%
      invoke("agg", first_valid_value, list())


  # out_schema <- out %>% sdf_schema()
  # values_fill_args <- list()
  # for (col in names(values_fill)) {
  #   value <- values_fill[[col]]
  #   if (!is.null(value) && !is.na(value) && !is.nan(value)) {
  #     value_sql <- dbplyr::translate_sql_(
  #       list(value), con = dbplyr::simulate_dbi()
  #     )
  #     dest_cols <- spec %>%
  #       dplyr::filter(.value == col) %>%
  #       dplyr::pull(.name)
  #     for (dest_col in dest_cols) {
  #       is_double_type <- identical(out_schema[[dest_col]]$type, "DoubleType")
  #       dest_col_sql <- quote_sql_name(dest_col)
  #       args <- (
  #         if (is_double_type) {
  #           sprintf(
  #             "IF(ISNULL(%s) OR ISNAN(%s), %s, %s)",
  #             dest_col_sql, dest_col_sql, value_sql, dest_col_sql
  #           )
  #         } else {
  #           sprintf(
  #             "IF(ISNULL(%s), %s, %s)", dest_col_sql, value_sql, dest_col_sql
  #           )
  #         }
  #       ) %>%
  #         dplyr::sql() %>%
  #         list()
  #       names(args) <- dest_col
  #       values_fill_args <- append(values_fill_args, args)
  #     }
  #   }
  # }



















    if (is.null(out)) {
      out <- pivoted
    } else {
      pivoted <- pivoted %>% invoke("drop", as.list(key_vars))
      out <- invoke(out, "join", pivoted, as.list(summarized_data_id_col))
    }
  }

  if (!is.null(summarized_data_id_col)) {
    out <- out %>% invoke("drop", list(summarized_data_id_col))
  }

  # coalesce output columns based on key_vars after dropping `summarized_data_id_col`
  # (i.e., making sure each observation identified by `key_vars` will occupy 1 row
  # instead of multiple rows)
  out <- out %>% sdf_register()
  out <- do.call(dplyr::group_by, append(list(out), lapply(key_vars, as.symbol)))
  out_schema <- out %>% sdf_schema()
  coalesce_cols <- setdiff(colnames(out), key_vars)
  coalesce_args <- lapply(
    coalesce_cols,
    function(col) {
      is_double_type <- identical(out_schema[[col]]$type, "DoubleType")
      col <- quote_sql_name(col)
      (
        if (is_double_type) {
          sprintf(
            "FIRST(IF(ISNULL(%s) OR ISNAN(%s), NULL, %s), TRUE)", col, col, col
          )
        } else {
          sprintf("FIRST(IF(ISNULL(%s), NULL, %s), TRUE)", col, col)
        }
      ) %>%
        dplyr::sql()
    }
  )
  names(coalesce_args) <- coalesce_cols
  out <- do.call(dplyr::summarize, append(list(out), coalesce_args))

  # TODO:
  # fill missing values according to `values_fill`
  # out_schema <- out %>% sdf_schema()
  # values_fill_args <- list()
  # for (col in names(values_fill)) {
  #   value <- values_fill[[col]]
  #   if (!is.null(value) && !is.na(value) && !is.nan(value)) {
  #     value_sql <- dbplyr::translate_sql_(
  #       list(value), con = dbplyr::simulate_dbi()
  #     )
  #     dest_cols <- spec %>%
  #       dplyr::filter(.value == col) %>%
  #       dplyr::pull(.name)
  #     for (dest_col in dest_cols) {
  #       is_double_type <- identical(out_schema[[dest_col]]$type, "DoubleType")
  #       dest_col_sql <- quote_sql_name(dest_col)
  #       args <- (
  #         if (is_double_type) {
  #           sprintf(
  #             "IF(ISNULL(%s) OR ISNAN(%s), %s, %s)",
  #             dest_col_sql, dest_col_sql, value_sql, dest_col_sql
  #           )
  #         } else {
  #           sprintf(
  #             "IF(ISNULL(%s), %s, %s)", dest_col_sql, value_sql, dest_col_sql
  #           )
  #         }
  #       ) %>%
  #         dplyr::sql() %>%
  #         list()
  #       names(args) <- dest_col
  #       values_fill_args <- append(values_fill_args, args)
  #     }
  #   }
  # }
  # if (length(values_fill_args) > 0) {
  #   out <- do.call(dplyr::mutate, append(list(out), values_fill_args))
  # }

  out <- out %>% dplyr::ungroup()
  group_vars <- intersect(group_vars, colnames(out))
  do.call(dplyr::group_by, append(list(out), lapply(group_vars, as.symbol)))
}

is_scalar <- function(x) {
  if (is.null(x)) {
    return(FALSE)
  }

  if (is.list(x)) {
    (length(x) == 1) && !rlang::have_name(x)
  } else {
    length(x) == 1
  }
}
