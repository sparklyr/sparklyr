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
    name_prefix = names_prefix,
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

  list(
    spec = out,
    names_cols = names_from,
    values_cols = values_from
  )
}

sdf_pivot_wider <- function(data,
                            spec,
                            names_repair = "check_unique",
                            id_cols = NULL,
                            values_fill = NULL,
                            values_fn = NULL) {
  names_cols <- spec$names_cols
  values_cols <- spec$values_cols
  spec <- canonicalize_spec(spec$spec)

  if (is.function(values_fn)) {
    values_fn <- rlang::rep_named(unique(spec$.value), list(values_fn))
  }
  if (!is.null(values_fn) && !is.list(values_fn)) {
    abort("`values_fn` must be a NULL, a function, or a named list")
  }

  if (is_scalar(values_fill)) {
    values_fill <- rlang::rep_named(unique(spec$.value), list(values_fill))
  }
  if (!is.null(values_fill) && !is.list(values_fill)) {
    abort("`values_fill` must be NULL, a scalar, or a named list")
  }

  spec_cols <- c(names(spec)[-(1:2)], values)

  id_cols <- rlang::enquo(id_cols)
  colnames_df <- replicate_colnames(data)
  if (!rlang::quo_is_null(id_cols)) {
    key_vars <- names(tidyselect::eval_select(id_cols, colnames_df))
  } else {
    key_vars <- dplyr::tbl_vars(colnames_df)
  }
  key_vars <- setdiff(key_vars, spec_cols)

  grouped <- do.call(
    dplyr::group_by,
    append(
      list(data),
      lapply(union(names_cols, key_vars), as.symbol)
    )
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
  summarizers <- lapply(summarizers, rlang::parse_expr)
  summarized <- do.call(
    dplyr::summarize,
    append(list(grouped), summarizers)
  )
  summarized <- sumamrized %>% dplyr::compute()

  value_specs <- unname(split(spec, spec$.value))
  value_out <- vctrs::vec_init(list(), length(value_specs))

  for (i in seq_along(value_out)) {
    spec_i <- value_specs[[i]]
    value <- spec_i$value[[1]]
# TODO: join by names_cols
  }
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
