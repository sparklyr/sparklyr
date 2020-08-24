#' @include tidyr_pivot_utils.R
#' @include tidyr_utils.R
NULL

#' @importFrom tidyr pivot_longer
#' @export
pivot_longer.tbl_spark <- function(data,
                                   cols,
                                   names_to = "name",
                                   names_prefix = NULL,
                                   names_sep = NULL,
                                   names_pattern = NULL,
                                   names_ptypes = list(),
                                   names_transform = list(),
                                   names_repair = "check_unique",
                                   values_to = "value",
                                   values_drop_na = FALSE,
                                   values_ptypes = list(),
                                   values_transform = list(),
                                   ...) {
  cols <- rlang::enquo(cols)
  spec <- build_longer_spec(
    data,
    !!cols,
    names_to = names_to,
    values_to = values_to,
    names_prefix = names_prefix,
    names_sep = names_sep,
    names_pattern = names_pattern,
    names_ptypes = names_ptypes,
    names_transform = names_transform
  )

  sdf_pivot_longer(
    data,
    spec,
    names_repair = names_repair,
    values_drop_na = values_drop_na,
    values_ptypes = values_ptypes,
    values_transform = values_transform
  )
}

build_longer_spec <- function(data,
                              cols,
                              names_to = "name",
                              values_to = "value",
                              names_prefix = NULL,
                              names_sep = NULL,
                              names_pattern = NULL,
                              names_ptypes = NULL,
                              names_transform = NULL) {
  colnames_df <- replicate_colnames(data)
  cols <- names(tidyselect::eval_select(rlang::enquo(cols), colnames_df))

  if (length(cols) == 0) {
    rlang::abort(glue::glue("`cols` must select at least one column."))
  }

  if (is.null(names_prefix)) {
    output_names <- cols
  } else {
    output_names <- gsub(paste0("^", names_prefix), "", cols)
  }

  if (length(names_to) > 1) {
    if (!xor(is.null(names_sep), is.null(names_pattern))) {
      rlang::abort(glue::glue(
        "If you supply multiple names in `names_to` you must also supply one",
        " of `names_sep` or `names_pattern`."
      ))
    }

    if (!is.null(names_sep)) {
      output_names <- .str_separate(output_names, names_to, sep = names_sep)
    } else {
      output_names <- stringr::str_extract(output_names, names_to, regex = names_pattern)
    }
  } else if (length(names_to) == 0) {
    output_names <- tibble::new_tibble(x = list(), nrow = length(output_names))
  } else {
    if (!is.null(names_sep)) {
      rlang::abort("`names_sep` can not be used with `names_to` of length 1")
    }
    if (!is.null(names_pattern)) {
      output_names <- stringr::str_extract(output_names, names_to, regex = names_pattern)[[1]]
    }

    output_names <- tibble::tibble(!!names_to := output_names)
  }

  if (".value" %in% names_to) {
    values_to <- NULL
  } else {
    vctrs::vec_assert(values_to, ptype = character(), size = 1)
  }

  # optionally, cast variables generated from columns
  cast_cols <- intersect(names(output_names), names(names_ptypes))
  for (col in cast_cols) {
    output_names[[col]] <- vctrs::vec_cast(output_names[[col]], names_ptypes[[col]])
  }

  # transform cols
  coerce_cols <- intersect(names(output_names), names(names_transform))
  for (col in coerce_cols) {
    f <- as_function(names_transform[[col]])
    output_names[[col]] <- f(output_names[[col]])
  }

  out <- tibble::tibble(.name = cols)
  out[[".value"]] <- values_to
  out <- vctrs::vec_cbind(out, output_names)
  out
}

sdf_pivot_longer <- function(data,
                             spec,
                             names_repair = "check_unique",
                             values_drop_na = FALSE,
                             values_ptypes = list(),
                             values_transform = list()) {
  sc <- spark_connection(data)
  if (spark_version(sc) < "2.0.0") {
    rlang::abort("`pivot_wider.tbl_spark` requires Spark 2.-.0 or higher")
  }

  id_col <- random_string("__row_id")
  id_sql <- list(dplyr::sql("monotonically_increasing_id()"))
  names(id_sql) <- id_col

  data <- data %>>%
    dplyr::mutate %@% id_sql %>%
    dplyr::compute()
  spec <- canonicalize_spec(spec)

  # Quick hack to ensure that split() preserves order
  v_fct <- factor(spec$.value, levels = unique(spec$.value))
  values <- split(spec$.name, v_fct)
  value_keys <- split(spec[-(1:2)], v_fct)

  out <- data %>>%
    dplyr::select %@% setdiff(colnames(data), spec$.name) %>%
    spark_dataframe()

  for (value in names(values)) {
    cols <- values[[value]]
    value_key <- value_keys[[value]]

    stack_expr <- lapply(
      seq_along(cols),
      function(idx) {
        key_tuple <- value_key[idx,] %>%
          as.list() %>%
          dbplyr::translate_sql_(con = dbplyr::simulate_dbi()) %>%
          lapply(as.character) %>%
          unlist() %>%
          c(id_col)

        c(quote_sql_name(cols[[idx]]), key_tuple) %>%
          paste0(collapse = ", ")
      }
    ) %>%
      paste0(collapse = ", ") %>%
      sprintf(
        "STACK(%d, %s) AS (%s)",
        length(cols),
        .,
        paste0(
          lapply(c(value, names(value_key), id_col), quote_sql_name), collapse = ", "
        )
      )
    stacked_sdf <- data %>%
      spark_dataframe() %>%
      invoke("selectExpr", list(stack_expr))

    join_cols <- intersect(
      out %>% invoke("columns"),
      c(value, names(value_key), id_col)
    ) %>%
      as.list()
    out <- out %>% invoke("join", stacked_sdf, join_cols, "left_outer")
  }

  key_cols <- colnames(spec[-(1:2)])
  out %>%
    invoke("sort", id_col, as.list(key_cols)) %>%
    invoke("drop", id_col) %>%
    sdf_register()
}

.strsep <- function(x, sep) {
  nchar <- nchar(x)
  pos <- purrr::map(sep, function(i) {
    if (i >= 0) return(i)
    pmax(0, nchar + i)
  })
  pos <- c(list(0), pos, list(nchar))

  purrr::map(1:(length(pos) - 1), function(i) {
    substr(x, pos[[i]] + 1, pos[[i + 1]])
  })
}

.slice_match <- function(x, i) {
  structure(
    x[i],
    match.length = attr(x, "match.length")[i],
    index.type = attr(x, "index.type"),
    useBytes = attr(x, "useBytes")
  )
}

.str_split_n <- function(x, pattern, n_max = -1) {
  m <- gregexpr(pattern, x, perl = TRUE)
  if (n_max > 0) {
    m <- lapply(m, function(x) .slice_match(x, seq_along(x) < n_max))
  }
  regmatches(x, m, invert = TRUE)
}

.list_indices <- function(x, max = 20) {
  if (length(x) > max) {
    x <- c(x[seq_len(max)], "...")
  }

  paste(x, collapse = ", ")
}

.simplify_pieces <- function(pieces, p, fill_left) {
  too_sml <- NULL
  too_big <- NULL
  n <- length(pieces)

  out <- lapply(seq(p), function(x) rep(NA, n))
  for (i in seq_along(pieces)) {
    x <- pieces[[i]]
    if (!(length(x) == 1 && is.na(x[[1]]))) {
      if (length(x) > p) {
        too_big <- c(too_big, i)

        for (j in seq(p)) {
          out[[j]][[i]] <- x[[j]]
        }
      } else if (length(x) < p) {
        too_sml <- c(too_sml, i)

        gap <- p - length(x)
        for (j in seq(p)) {
          if (fill_left) {
            out[[j]][[i]] <- (
              if (j >= gap) x[[j - gap]] else NA
            )
          } else {
            out[[j]][[i]] <- (
              if (j < length(x)) x[[j]] else NA
            )
          }
        }
      } else {
        for (j in seq(p)) {
          out[[j]][[i]] <- x[[j]]
        }
      }
    }
  }

  structure(list(
    strings = out,
    too_big = too_big,
    too_sml = too_sml
  ))
}

.str_split_fixed <- function(value, sep, n, extra = "warn", fill = "warn") {
  if (extra == "error") {
    rlang::warn(glue::glue(
      "`extra = \"error\"` is deprecated. \\
       Please use `extra = \"warn\"` instead"
    ))
    extra <- "warn"
  }

  extra <- rlang::arg_match(extra, c("warn", "merge", "drop"))
  fill <- rlang::arg_match(fill, c("warn", "left", "right"))

  n_max <- if (extra == "merge") n else -1L
  pieces <- .str_split_n(value, sep, n_max = n_max)

  simp <- .simplify_pieces(pieces, n, fill == "left")

  n_big <- length(simp$too_big)
  if (extra == "warn" && n_big > 0) {
    idx <- .list_indices(simp$too_big)
    rlang::warn(glue::glue(
      "Expected {n} pieces. ",
      "Additional pieces discarded in {n_big} rows [{idx}]."
    ))
  }

  n_sml <- length(simp$too_sml)
  if (fill == "warn" && n_sml > 0) {
    idx <- .list_indices(simp$too_sml)
    rlang::warn(glue::glue(
      "Expected {n} pieces. ",
      "Missing pieces filled with `NA` in {n_sml} rows [{idx}]."
    ))
  }

  simp$strings
}

.str_separate <- function(x, into, sep, extra = "warn", fill = "warn") {
  if (!is.character(into)) {
    rlang::abort("`into` must be a character vector")
  }

  if (is.numeric(sep)) {
    out <- .strsep(x, sep)
  } else if (rlang::is_character(sep)) {
    out <- .str_split_fixed(x, sep, length(into), extra = extra, fill = fill)
  } else {
    rlang::abort("`sep` must be either numeric or character")
  }

  names(out) <- rlang::as_utf8_character(into)
  out <- out[!is.na(names(out))]

  tibble::as_tibble(out)
}
