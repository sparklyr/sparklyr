#' @include sdf_interface.R
#' @include tidyr_pivot_utils.R
#' @include tidyr_utils.R
NULL

#' @importFrom tidyr pivot_wider
#' @export
pivot_wider.tbl_spark <- function(data,
                                  id_cols = NULL,
                                  id_expand = FALSE,
                                  names_from = !!as.name("name"),
                                  names_prefix = "",
                                  names_sep = "_",
                                  names_glue = NULL,
                                  names_sort = FALSE,
                                  names_vary = "fastest",
                                  names_expand = FALSE,
                                  names_repair = "check_unique",
                                  values_from = !!as.name("value"),
                                  values_fill = NULL,
                                  values_fn = NULL,
                                  unused_fn = NULL,
                                  ...) {
  if (!identical(id_expand, FALSE)) {
    rlang::abort("`id_expand` is not yet supported.")
  }
  if (!identical(names_vary, "fastest")) {
    rlang::abort("`names_vary` is not yet supported.")
  }
  if (!identical(names_expand, FALSE)) {
    rlang::abort("`names_expand` is not yet supported.")
  }
  if (!identical(unused_fn, NULL)) {
    rlang::abort("`unused_fn` is not yet supported.")
  }

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

#' @importFrom purrr reduce map transpose
#' @importFrom tidyselect eval_select
#' @importFrom tibble tibble
#' @importFrom rlang `!!` enquos enquo
#' @importFrom dplyr ungroup arrange
sdf_build_wider_spec <- function(data,
                                 names_from,
                                 values_from,
                                 names_prefix = "",
                                 names_sep = "_",
                                 names_glue = NULL,
                                 names_sort = FALSE) {
  colnames_df <- replicate_colnames(data)
  values_from <- names(eval_select(enquo(values_from), colnames_df))

  local_data <- data %>%
    head(1) %>%
    collect()

  row_ids <- data %>%
    select(!!enquo(names_from)) %>%
    distinct() %>%
    collect() %>%
    select(-dplyr::group_vars(data))

  if (dim(row_ids)[2] == 1) {
    row_names <- row_ids[1][[1]]
  } else {
    row_names <- transpose(row_ids) %>%
      map_chr(~ paste(.x, collapse = names_sep))
  }

  out <- tibble(.name = paste0(names_prefix, row_names))

  if (length(values_from) == 1) {
    out$.value <- values_from
  } else {
    out <- seq_along(values_from) %>%
      map(~out) %>%
      reduce(rbind)

    out$.value <- as.character(rep(values_from, each = nrow(row_ids)))
    out$.name <- paste0(out$.value, names_sep, out$.name)

    row_ids <- seq_along(values_from) %>%
      map(~row_ids) %>%
      reduce(rbind)
  }

  out <- cbind(out, row_ids)
  if (!is.null(names_glue)) {
    out$.name <- as.character(glue::glue_data(out, names_glue))
  }

  if (names_sort) {
    out <- arrange(out, !!rlang::parse_expr(".name"))
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
  if (spark_version(sc) < "2.3.0") {
    rlang::abort("`pivot_wider.tbl_spark` requires Spark 2.3.0 or higher")
  }

  list(spec, values_fill, values_fn) %<-% .canonicalize_args(
    spec, values_fill, values_fn
  )

  group_vars <- dplyr::group_vars(data)

  names_from <- names(spec)[-(1:2)]
  values_from <- unique(spec$.value)
  spec_cols <- c(names_from, values_from)

  id_cols <- rlang::enquo(id_cols)
  colnames_df <- replicate_colnames(data)
  if (!rlang::quo_is_null(id_cols)) {
    key_vars <- names(tidyselect::eval_select(id_cols, colnames_df))
  } else {
    key_vars <- as.character(dplyr::tbl_vars(colnames_df))
  }
  key_vars <- setdiff(key_vars, spec_cols)

  data <- data %>% .summarize_data(values_fn, key_vars, names_from)

  list(data, spec, key_vars) %<-% .apply_pivot_wider_names_repair(
    data = data,
    spec = spec,
    spec_cols = spec_cols,
    key_vars = key_vars,
    names_repair = names_repair
  )

  seq_col <- random_string("__seq")
  seq_col_args <- list(dplyr::sql("MONOTONICALLY_INCREASING_ID()"))
  names(seq_col_args) <- seq_col
  data <- data %>>% dplyr::mutate %@% seq_col_args %>% dplyr::compute()

  value_specs <- unname(split(spec, spec$.value))
  pvt_col <- random_string("__pvt")
  out <- NULL

  for (value_spec in value_specs) {
    out <- out %>%
      .process_value_spec(
        data,
        value_spec,
        key_vars,
        names_from,
        values_fill,
        pvt_col,
        seq_col
      )
  }

  out %>%
    invoke("drop", list(seq_col)) %>%
    .postprocess_pivot_wider_output(group_vars, key_vars)
}

.process_value_spec <- function(out,
                                data,
                                value_spec,
                                key_vars,
                                names_from,
                                values_fill,
                                pvt_col,
                                seq_col) {
  sc <- spark_connection(data)

  value <- value_spec$.value[[1]]
  lhs_cols <- c(seq_col, key_vars, names_from, value)
  lhs <- data %>>% dplyr::select %@% lapply(lhs_cols, as.symbol)
  all_obvs <- lhs %>%
    dplyr::ungroup() %>>%
    dplyr::distinct %@% lapply(union(seq_col, key_vars), as.symbol)

  rhs_select_args <- list(as.symbol(".name"))
  names(rhs_select_args) <- pvt_col
  rhs_select_args <- append(rhs_select_args, lapply(names_from, as.symbol))
  rhs <- value_spec %>>%
    dplyr::select %@% rhs_select_args %>%
    copy_to(sc, ., name = random_string("sparklyr_tmp_"))
  combined <- spark_dataframe(lhs) %>%
    invoke(
      "%>%",
      list("join", spark_dataframe(rhs), as.list(names_from), "inner"),
      list("drop", as.list(names_from))
    )
  all_vals <- invoke(spark_dataframe(rhs), "crossJoin", spark_dataframe(all_obvs))
  missing_vals <- all_vals %>%
    invoke(
      "%>%",
      list("join", spark_dataframe(lhs), as.list(union(key_vars, names_from)), "left_anti"),
      list("drop", as.list(names_from))
    ) %>%
    sdf_register()
  combined_cols <- invoke(combined, "columns")
  combined_schema_obj <- invoke(combined, "schema")
  val_field_idx <- invoke(combined_schema_obj, "fieldIndex", value)
  val_sql_type <- invoke(combined_schema_obj, "fields")[[val_field_idx]] %>%
    invoke("%>%", list("dataType"), list("sql"))
  fv <- values_fill[[value]]
  val_fill_sql <- dbplyr::translate_sql_(list(fv), con = dbplyr::simulate_dbi()) %>%
    dplyr::sql() %>%
    list()
  names(val_fill_sql) <- value
  missing_vals <- missing_vals %>>% dplyr::mutate %@% val_fill_sql
  combined <- invoke(combined, "unionByName", spark_dataframe(missing_vals))

  group_by_cols <- lapply(
    union(seq_col, key_vars),
    function(col) invoke_new(sc, "org.apache.spark.sql.Column", col)
  )
  agg <- .first_valid_value(sc, schema = combined %>% sdf_schema())(value)
  pvt <- combined %>%
    invoke(
      "%>%",
      list("groupBy", group_by_cols),
      list("pivot", pvt_col, as.list(value_spec$.name)),
      list("agg", agg, list())
    )
  if (is.null(out)) {
    out <- pvt
  } else {
    pvt <- pvt %>% invoke("drop", as.list(key_vars))
    out <- invoke(out, "join", pvt, as.list(seq_col))
  }

  out
}

.canonicalize_args <- function(spec, values_fill, values_fn) {
  spec <- canonicalize_spec(spec)

  if (is.null(values_fill)) {
    values_fill <- rlang::rep_named(unique(spec$.value), list(NA))
  } else if (.is_scalar(values_fill)) {
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

  list(spec, values_fill, values_fn)
}

.summarize_data <- function(data, values_fn, key_vars, names_from) {
  summarizers <- list()
  schema <- data %>% sdf_schema()
  for (idx in seq_along(values_fn)) {
    col <- names(values_fn)[[idx]]
    summarizers[[col]] <- (
      if (is.null(values_fn[[idx]])) {
        .build_coalesce_col_sql(schema)(col)
      } else {
        rlang::expr((!!values_fn[[col]])(!!rlang::sym(col)))
      })
  }
  names(summarizers) <- names(values_fn)

  # apply summarizing function(s)
  data %>>%
    dplyr::group_by %@% lapply(union(key_vars, names_from), as.symbol) %>>%
    dplyr::summarize %@% summarizers
}

.apply_pivot_wider_names_repair <- function(data,
                                            spec,
                                            spec_cols,
                                            key_vars,
                                            names_repair) {
  other_cols <- colnames(data) %>%
    setdiff(key_vars) %>%
    setdiff(spec_cols)
  output_colnames <- c(key_vars, other_cols, spec$.name) %>%
    repair_names(names_repair = names_repair)
  key_vars_renamed <- head(output_colnames, length(key_vars))
  spec$.name <- tail(output_colnames, length(spec$.name))
  name_repair_args <- lapply(c(key_vars, other_cols), as.symbol)
  names(name_repair_args) <- head(output_colnames, length(key_vars) + length(other_cols))
  if (length(name_repair_args) > 0) {
    data <- data %>>% dplyr::rename %@% name_repair_args
  }
  key_vars <- key_vars_renamed

  list(data, spec, key_vars)
}

.build_coalesce_col_sql <- function(schema) {
  impl <- function(col) {
    is_double_type <- identical(schema[[col]]$type, "DoubleType")
    col <- quote_sql_name(col)
    (
      if (is_double_type) {
        sprintf(
          "FIRST(IF(ISNULL(%s) OR ISNAN(%s), NULL, %s), TRUE)", col, col, col
        )
      } else {
        sprintf("FIRST(IF(ISNULL(%s), NULL, %s), TRUE)", col, col)
      }) %>%
      dplyr::sql()
  }

  impl
}

.first_valid_value <- function(sc, schema) {
  impl <- function(value) {
    value_col <- invoke_new(sc, "org.apache.spark.sql.Column", value)
    value_is_invalid <- invoke_static(
      sc, "org.apache.spark.sql.functions", "isnull", value_col
    )
    if (identical(schema[[value]]$type, "DoubleType")) {
      value_is_nan <- invoke_static(
        sc, "org.apache.spark.sql.functions", "isnan", value_col
      )
      value_is_invalid <- invoke(value_is_invalid, "or", value_is_nan)
    }

    value_is_invalid %>%
      invoke_static(sc, "org.apache.spark.sql.functions", "when", ., NULL) %>%
      invoke("otherwise", value_col) %>%
      invoke_static(sc, "org.apache.spark.sql.functions", "first", ., TRUE)
  }

  impl
}

.postprocess_pivot_wider_output <- function(out, group_vars, key_vars) {
  # coalesce output columns based on key_vars after dropping `seq_col`
  # (i.e., making sure each observation identified by `key_vars` will occupy 1 row
  # instead of multiple rows)
  out <- out %>%
    sdf_register() %>>%
    dplyr::group_by %@% lapply(key_vars, as.symbol)
  out_schema <- out %>% sdf_schema()
  coalesce_cols <- setdiff(colnames(out), key_vars)
  coalesce_args <- lapply(coalesce_cols, .build_coalesce_col_sql(out_schema))
  names(coalesce_args) <- coalesce_cols
  out <- out %>>%
    dplyr::summarize %@% coalesce_args %>%
    dplyr::ungroup()

  group_vars <- intersect(group_vars, colnames(out))

  out %>>% dplyr::group_by %@% lapply(group_vars, as.symbol)
}

.is_scalar <- function(x) {
  if (is.null(x)) {
    return(FALSE)
  }

  if (is.list(x)) {
    (length(x) == 1) && !rlang::have_name(x)
  } else {
    length(x) == 1
  }
}
