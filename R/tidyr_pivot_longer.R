#' @include tidyr_utils.R
NULL

_strsep <- function(x, sep) {
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

_slice_match <- function(x, i) {
  structure(
    x[i],
    match.length = attr(x, "match.length")[i],
    index.type = attr(x, "index.type"),
    useBytes = attr(x, "useBytes")
  )
}

_str_split_n <- function(x, pattern, n_max = -1) {
  m <- gregexpr(pattern, x, perl = TRUE)
  if (n_max > 0) {
    m <- lapply(m, function(x) _slice_match(x, seq_along(x) < n_max))
  }
  regmatches(x, m, invert = TRUE)
}

_list_indices <- function(x, max = 20) {
  if (length(x) > max) {
    x <- c(x[seq_len(max)], "...")
  }

  paste(x, collapse = ", ")
}

_str_split_fixed <- function(value, sep, n, extra = "warn", fill = "warn") {
  if (extra == "error") {
    rlang::warn(glue::glue(
      "`extra = \"error\"` is deprecated. \\
       Please use `extra = \"warn\"` instead"
    ))
    extra <- "warn"
  }

  extra <- arg_match(extra, c("warn", "merge", "drop"))
  fill <- arg_match(fill, c("warn", "left", "right"))

  n_max <- if (extra == "merge") n else -1L
# TODO:

# e.g.,
# tidyr:::str_split_n(c("a.b", "c.d.e"), pattern = "\\.")
# [[1]]
# [1] "a" "b"
#
# [[2]]
# [1] "c" "d" "e"

# AND
# tidyr:::str_split_n(c("a.b", "c.d.e"), pattern = "\\.", n_max = 2)
# [[1]]
# [1] "a" "b"
#
# [[2]]
# [1] "c"   "d.e"


  pieces <- _str_split_n(value, sep, n_max = n_max)

# TODO: implement _simplifyPieces
  simp <- _simplifyPieces(pieces, n, fill == "left")

  n_big <- length(simp$too_big)
  if (extra == "warn" && n_big > 0) {
    idx <- _list_indices(simp$too_big)
    rlang::warn(glue::glue("Expected {n} pieces. Additional pieces discarded in {n_big} rows [{idx}]."))
  }

  n_sml <- length(simp$too_sml)
  if (fill == "warn" && n_sml > 0) {
    idx <- _list_indices(simp$too_sml)
    warn(glue("Expected {n} pieces. Missing pieces filled with `NA` in {n_sml} rows [{idx}]."))
  }

  simp$strings
}

_str_separate <- function(x, into, sep, convert = FALSE, extra = "warn", fill = "warn") {
  if (!is.character(into)) {
    rlang::abort("`into` must be a character vector")
  }

  if (is.numeric(sep)) {
    out <- _strsep(x, sep)
  } else if (is_character(sep)) {
    out <- _str_split_fixed(x, sep, length(into), extra = extra, fill = fill)
  } else {
    rlang::abort("`sep` must be either numeric or character")
  }

  names(out) <- rlang::as_utf8_character(into)
  out <- out[!is.na(names(out))]
  if (convert) {
    out[] <- purrr::map(out, type.convert, as.is = TRUE)
  }
  tibble::as_tibble(out)
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
  cols <- tidyselect::eval_select(rlang::enquo(cols), colnames_df)

  if (length(cols) == 0) {
    rlang::abort(glue::glue("`cols` must select at least one column."))
  }

  if (is.null(names_prefix)) {
    names <- cols
  } else {
    names <- gsub(paste0("^", names_prefix), "", cols)
  }

  if (length(names_to) > 1) {
    if (!xor(is.null(names_sep), is.null(names_pattern))) {
      rlang::abort(glue::glue(
        "If you supply multiple names in `names_to` you must also supply one",
        " of `names_sep` or `names_pattern`."
      ))
    }

    if (!is.null(names_sep)) {
      names <- _str_separate(names, names_to, sep = names_sep)
    } else {
      names <- str_extract(names, names_to, regex = names_pattern)
    }
  } else if (length(names_to) == 0) {
    names <- tibble::new_tibble(x = list(), nrow = length(names))
  } else {
    if (!is.null(names_sep)) {
      abort("`names_sep` can not be used with length-1 `names_to`")
    }
    if (!is.null(names_pattern)) {
      names <- str_extract(names, names_to, regex = names_pattern)[[1]]
    }

    names <- tibble(!!names_to := names)
  }

  if (".value" %in% names_to) {
    values_to <- NULL
  } else {
    vec_assert(values_to, ptype = character(), size = 1)
  }

  # optionally, cast variables generated from columns
  cast_cols <- intersect(names(names), names(names_ptypes))
  for (col in cast_cols) {
    names[[col]] <- vec_cast(names[[col]], names_ptypes[[col]])
  }

  # transform cols
  coerce_cols <- intersect(names(names), names(names_transform))
  for (col in coerce_cols) {
    f <- as_function(names_transform[[col]])
    names[[col]] <- f(names[[col]])
  }

  out <- tibble(.name = names(cols))
  out[[".value"]] <- values_to
  out <- vec_cbind(out, names)
  out
}
