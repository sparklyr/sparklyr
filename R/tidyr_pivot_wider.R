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
  spec <- build_wider_spec_for_sdf(
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

build_wider_spec_for_sdf <- function(data,
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
  spec <- canonicalize_spec(spec)

  if (!is.null(values_fill) && !is.list(values_fill)) {
    abort("`values_fill` must be NULL, a scalar, or a named list")
  }
  if (!is.null(values_fn) && !is.list(values_fn)) {
    abort("`values_fn` must be a NULL, a function, or a named list")
  }

  if (is_scalar(values_fill)) {
    values_fill <- rlang::rep_named(unique(spec$.value), list(values_fill))
  }
  if (!is.list(values_fn)) {
    values_fn <- rlang::rep_named(unique(spec$.value), list(values_fn))
  }

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

  grouped_data <- do.call(
    dplyr::group_by,
    append(list(data), lapply(union(key_vars, names_from), as.symbol))
  )
  summarizers <- lapply(
    seq_along(values_fn),
    function(idx) {
      col <- names(values_fn)[[idx]]
      if (is.null(values_fn[[idx]])) {
        sprintf("FIRST(%s)", col)
      } else {
        sprintf("values_fn$%s(%s)", col, col)
      }
    }
  )
  names(summarizers) <- names(values_fn)
  summarizers <- lapply(summarizers, rlang::parse_expr)
  summarized_data_id_col <- random_string("sdf_pivot_id")
  summarized_data <- do.call(
    dplyr::summarize,
    append(list(grouped_data), summarizers)
  )
  summarized_data_id_col_args <- list(dplyr::sql("monotonically_increasing_id()"))
  names(summarized_data_id_col_args) <- summarized_data_id_col
  summarized_data <- do.call(
    dplyr::mutate,
    append(list(summarized_data), summarized_data_id_col_args)
  ) %>%
    dplyr::compute()
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
    rhs_select_args = list(as.symbol(".name"))
    names(rhs_select_args) <- pivot_col
    rhs_select_args <- append(rhs_select_args, lapply(names_from, as.symbol))
    rhs <- do.call(dplyr::select, append(list(value_spec), rhs_select_args)) %>%
      copy_to(sc, ., name = random_string("pivot_wider_spec_sdf"))
    combined <- spark_dataframe(lhs) %>%
      invoke("join", spark_dataframe(rhs), as.list(names_from))
    agg_spec <- new.env(parent = emptyenv())
    assign(value, "FIRST", envir = agg_spec)
    sc <- spark_connection(data)
    group_by_cols <- union(summarized_data_id_col, key_vars)
    group_by_cols <- union(group_by_cols, names_from)
    group_by_cols <- lapply(
      group_by_cols,
      function(col) invoke_new(sc, "org.apache.spark.sql.Column", col)
    )
    pivoted <- combined %>%
      invoke("groupBy", group_by_cols) %>%
      invoke("pivot", pivot_col, as.list(value_spec$.name)) %>%
      invoke("agg", agg_spec)

    if (is.null(out)) {
      out <- pivoted %>% invoke("drop", as.list(names_from))
    } else {
      pivoted <- pivoted %>% invoke("drop", as.list(union(key_vars, names_from)))
      out <- invoke(out, "join", pivoted, as.list(summarized_data_id_col))
    }
  }

    # TODO: replace missing values in value column
    # TODO: need to handle NaN values if column is numeric
# if (is.list(values_fill)) {
# values_fill is a named list, so process each column accordingly
# }
  out %>%
    invoke("drop", list(summarized_data_id_col)) %>%
    sdf_register()
}

is_scalar <- function(x) {
  if (is.null(x)) {
    return(FALSE)
  }

  if (is.list(x)) {
    (length(x) == 1) && !have_name(x)
  } else {
    length(x) == 1
  }
}
