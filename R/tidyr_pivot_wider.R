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
# TODO:
#   colnames_df <- replicate_colnames(data)
#   names_from <- names(tidyselect::eval_select(rlang::enquo(names_from), colnames_df))
#   values_from <- names(tidyselect::eval_select(rlang::enquo(values_from), colnames_df))
#
#   id_cols <- rlang::enquo(id_cols)
#   if (!rlang::quo_is_null(id_cols)) {
#     id_cols <- names(tidyselect::eval_select(rlang::enquo(id_cols), colnames_df))
#   } else {
#     id_cols <- dplyr::tbl_vars(colnames_df)
#   }
#   id_cols <- id_cols %>% setdiff(names_from) %>% setdiff(values_from)
#
#   unique_name_tuples <- do.call(
#     dplyr::distinct,
#     append(list(data), lapply(names_from, as.symbol))
#   ) %>%
#     collect()
#   unique_names <- lapply(
#     seq(nrow(unique_name_tuples)),
#     function(idx) {
#       if (!is.null(names_glue)) {
#         glue::glue_data(unique_name_tuples[idx, ], names_glue)
#       } else {
#         paste0(
#           names_prefix,
#           paste0(unique_name_tuples[idx, ], collapse = names_sep)
#         )
#       }
#     }
#   ) %>%
#     unlist()
#   name_col <- random_string("tidy_pivot_name
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
