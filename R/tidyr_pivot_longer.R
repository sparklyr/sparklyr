#' @include mutation.R
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
                                   names_ptypes = NULL,
                                   names_transform = NULL,
                                   names_repair = "check_unique",
                                   values_to = "value",
                                   values_drop_na = FALSE,
                                   values_ptypes = NULL,
                                   values_transform = NULL,
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

  output_names <- build_output_names(
    cols = cols,
    names_to = names_to,
    names_prefix = names_prefix,
    names_sep = names_sep,
    names_pattern = names_pattern
  )

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
    f <- rlang::as_function(names_transform[[col]])
    output_names[[col]] <- f(output_names[[col]])
  }

  out <- dplyr::tibble(.name = cols)
  out[[".value"]] <- values_to
  out <- vctrs::vec_cbind(out, output_names)
  out
}

build_output_names <- function(cols,
                               names_to,
                               names_prefix,
                               names_sep,
                               names_pattern) {
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
      output_names <- .str_extract(output_names, names_to, regex = names_pattern)
    }
  } else if (length(names_to) == 0) {
    output_names <- vctrs::data_frame(.size = length(output_names))
  } else {
    if (!is.null(names_sep)) {
      rlang::abort("`names_sep` can not be used with `names_to` of length 1")
    }
    if (!is.null(names_pattern)) {
      output_names <- .str_extract(output_names, names_to, regex = names_pattern)[[1]]
    }

    output_names <- dplyr::tibble(!!names_to := output_names)
  }

  output_names
}

sdf_pivot_longer <- function(data,
                             spec,
                             names_repair = "check_unique",
                             values_drop_na = FALSE,
                             values_ptypes = NULL,
                             values_transform = NULL) {
  sc <- spark_connection(data)
  if (spark_version(sc) < "2.0.0") {
    rlang::abort("`pivot_wider.tbl_spark` requires Spark 2.0.0 or higher")
  }

  group_vars <- dplyr::group_vars(data)
  data <- data %>% dplyr::ungroup()

  spec <- spec %>%
    canonicalize_spec() %>%
    deduplicate_longer_spec()

  values <- NULL
  value_keys <- NULL
  list(data, group_vars, spec, values, value_keys) %<-%
    .apply_pivot_longer_names_repair(data, group_vars, spec, names_repair)

  id_col <- random_string("__row_id")
  id_sql <- list(dplyr::sql("MONOTONICALLY_INCREASING_ID()"))
  names(id_sql) <- id_col
  data <- data %>>% dplyr::mutate %@% id_sql %>% dplyr::compute()

  seq_col <- NULL
  list(spec, value_keys, seq_col) %<-% .rename_seq_col(spec, value_keys)

  out <- data %>>%
    dplyr::select %@% setdiff(colnames(data), spec$.name) %>%
    spark_dataframe()

  for (value in names(values)) {
    value_key <- value_keys[value]
    cols <- values[[value]]

    stack_expr <- .build_stack_expr(value_key, cols, id_col)
    stacked_sdf <- data %>%
      spark_dataframe() %>%
      invoke("selectExpr", list(stack_expr))

    if (rlang::has_name(values_transform, value)) {
      transform_args <- list(
        do.call(dplyr::vars, list(as.symbol(value))), values_transform[[value]]
      )
      transformed_vals <- stacked_sdf %>%
        invoke("select", value, list()) %>%
        sdf_register() %>%
        dplyr::group_by() %>>%
        dplyr::summarize_at %@% transform_args
      stacked_sdf <- stacked_sdf %>%
        invoke("drop", value) %>%
        sdf_register() %>%
        sdf_bind_cols(transformed_vals) %>%
        spark_dataframe()
    }

    if (values_drop_na) {
      cond <- invoke_new(sc, "org.apache.spark.sql.Column", value) %>%
        invoke_static(sc, "org.apache.spark.sql.functions", "isnull", .) %>%
        invoke_static(sc, "org.apache.spark.sql.functions", "not", .)
      stacked_sdf <- stacked_sdf %>% invoke("filter", cond)
    }

    join_cols <- intersect(
      out %>% invoke("columns"),
      c(value, names(value_key[[1]]), id_col)
    ) %>%
      as.list()
    out <- out %>% invoke("join", stacked_sdf, join_cols, "left_outer")
  }

  .postprocess_pivot_longer_output(data, group_vars, spec, values, id_col, seq_col)(out)
}

# Perform name repair and update column names
.apply_pivot_longer_names_repair <- function(data, group_vars, spec, names_repair) {
  # Quick hack to ensure that split() preserves order
  v_fct <- factor(spec$.value, levels = unique(spec$.value))
  values <- split(spec$.name, v_fct)
  value_keys <- split(spec[-(1:2)], v_fct)

  all_cols <- setdiff(colnames(data), spec$.name)
  group_vars_idxes <- vctrs::vec_match(group_vars, all_cols)

  unpivoted_cols <- all_cols
  unpivoted_cols_idxes <- seq(length(all_cols))

  val_idxes <- seq(length(all_cols) + 1, length(all_cols) + length(names(values)))
  all_cols <- c(all_cols, names(values))

  all_keys <- unique(unname(lapply(value_keys, names)))
  key_idxes <- seq(length(all_cols) + 1, length(all_cols) + length(all_keys))
  all_cols <- c(all_cols, all_keys)

  all_cols <- repair_names(all_cols, names_repair)

  if (length(unpivoted_cols) > 0) {
    rename_unpivoted_cols_args <- unpivoted_cols
    names(rename_unpivoted_cols_args) <- all_cols[unpivoted_cols_idxes]
    data <- data %>>% dplyr::rename %@% rename_unpivoted_cols_args
  }

  if (length(group_vars) > 0) {
    group_vars <- all_cols[group_vars_idxes]
  }

  if (length(values) > 0) {
    names(values) <- all_cols[val_idxes]
    names(value_keys) <- all_cols[val_idxes]
  }

  if (length(key_idxes) > 0) {
    key_renames <- as.list(all_cols[key_idxes])
    names(key_renames) <- all_keys
    key_renames[[".seq"]] <- ".seq"
    for (v in names(values)) {
      names(value_keys[[v]]) <- unlist(key_renames[names(value_keys[[v]])])
    }
    names(spec) <- c(".name", ".value", unlist(key_renames[names(spec[-(1:2)])]))
  }

  list(data, group_vars, spec, values, value_keys)
}

.rename_seq_col <- function(spec, value_keys) {
  if (".seq" %in% colnames(spec)) {
    seq_col <- random_string("__seq")
    rename_arg <- list(as.symbol(".seq"))
    names(rename_arg) <- seq_col
    spec <- spec %>>% dplyr::rename %@% rename_arg
    for (v in names(value_keys)) {
      if (".seq" %in% colnames(value_keys[[v]])) {
        value_keys[[v]] <- value_keys[[v]] %>>%
          dplyr::rename %@% rename_arg
      }
    }
  } else {
    seq_col <- NULL
  }

  list(spec, value_keys, seq_col)
}

.build_stack_expr <- function(value_key, value_cols, id_col) {
  value <- names(value_key)
  value_key <- value_key[[1]]
  lapply(
    seq_along(value_cols),
    function(idx) {
      key_tuple <- value_key[idx, ] %>%
        as.list() %>%
        dbplyr::translate_sql_(con = dbplyr::simulate_hive()) %>%
        lapply(as.character) %>%
        unlist() %>%
        c(id_col)

      c(quote_sql_name(value_cols[[idx]]), key_tuple) %>%
        paste0(collapse = ", ")
    }
  ) %>%
    paste0(collapse = ", ") %>%
    sprintf(
      "STACK(%d, %s) AS (%s)",
      length(value_cols),
      .,
      paste0(
        lapply(c(value, names(value_key), id_col), quote_sql_name),
        collapse = ", "
      )
    )
}

.postprocess_pivot_longer_output <- function(data, group_vars, spec, values, id_col, seq_col) {
  key_cols <- colnames(spec[-(1:2)])
  output_cols <- c(
    setdiff(colnames(data), c(spec$.name, id_col)),
    key_cols,
    names(values)
  )
  output_cols <- setdiff(output_cols, c(id_col, seq_col))
  group_vars <- intersect(group_vars, output_cols)

  impl <- function(out) {
    out %>%
      invoke("sort", id_col, as.list(key_cols)) %>%
      sdf_register() %>>%
      dplyr::select %@% lapply(output_cols, as.symbol) %>>%
      dplyr::group_by %@% lapply(group_vars, as.symbol)
  }

  impl
}

# Ensure that there's a one-to-one match from spec to data by adding
# a special .seq variable which is automatically removed after pivotting.
deduplicate_longer_spec <- function(spec) {
  # Ensure each .name has a unique output identifier
  key <- spec[setdiff(names(spec), ".name")]
  if (vctrs::vec_duplicate_any(key)) {
    pos <- vctrs::vec_group_loc(key)$loc
    seq <- vector("integer", length = nrow(spec))
    for (i in seq_along(pos)) {
      seq[pos[[i]]] <- seq_along(pos[[i]])
    }
    spec$.seq <- seq
  }

  spec
}

.strsep <- function(x, sep) {
  nchar <- nchar(x)
  pos <- purrr::map(sep, function(i) {
    if (i >= 0) {
      return(i)
    }
    pmax(0, nchar + i)
  })
  pos <- c(list(0), pos, list(nchar))

  purrr::map(seq_len(length(pos) - 1), function(i) {
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

  dplyr::as_tibble(out)
}

.str_extract <- function(x, into, regex, convert = FALSE) {
  stopifnot(
    rlang::is_string(regex),
    rlang::is_character(into)
  )

  out <- .str_match_first(x, regex)
  if (length(out) != length(into)) {
    stop(
      "`regex` should define ", length(into), " groups; ", ncol(matches), " found.",
      call. = FALSE
    )
  }

  # Handle duplicated names
  if (anyDuplicated(into)) {
    pieces <- split(out, into)
    into <- names(pieces)
    out <- purrr::map(pieces, purrr::pmap_chr, paste0, sep = "")
  }

  into <- rlang::as_utf8_character(into)

  non_na_into <- !is.na(into)
  out <- out[non_na_into]
  names(out) <- into[non_na_into]

  out <- dplyr::as_tibble(out)

  if (convert) {
    out[] <- purrr::map(out, type.convert, as.is = TRUE)
  }

  out
}

.str_match_first <- function(string, regex) {
  loc <- regexpr(regex, string, perl = TRUE)
  loc <- .group_loc(loc)

  out <- lapply(
    seq_len(loc$matches),
    function(i) substr(string, loc$start[, i], loc$end[, i])
  )
  out[-1]
}

.group_loc <- function(x) {
  start <- cbind(as.vector(x), attr(x, "capture.start"))
  end <- start + cbind(attr(x, "match.length"), attr(x, "capture.length")) - 1L

  no_match <- start == -1L
  start[no_match] <- NA
  end[no_match] <- NA

  list(matches = ncol(start), start = start, end = end)
}
