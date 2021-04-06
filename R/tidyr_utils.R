#' @include dplyr_spark_table.R
#' @include spark_data_build_types.R
#' @include sql_utils.R
#' @include utils.R
NULL

# emit an error if the given arg is missing
check_present <- function(x) {
  arg <- rlang::ensym(x)
  if (missing(x)) {
    rlang::abort(paste0("Argument `", arg, "` is missing with no default"))
  }
}

# helper method for updating dplyr group variables
update_group_vars <- function(input, output, preserved) {
  incl <- dplyr::group_vars(input)
  output <- output %>>% dplyr::group_by %@% lapply(incl, as.symbol)

  excl <- setdiff(incl, preserved)
  if (length(excl) > 0) {
    output <- output %>>% dplyr::ungroup %@% lapply(excl, as.symbol)
  }

  output
}

strip_names <- function(df, base, names_sep) {
  base <- paste0(base, names_sep)
  names <- names(df)

  has_prefix <- regexpr(base, names, fixed = TRUE) == 1L
  names[has_prefix] <- substr(names[has_prefix], nchar(base) + 1, nchar(names[has_prefix]))

  rlang::set_names(df, names)
}

# Given a list of column names possibly containing duplicates and a valid tibble
# name-repair strategy, apply that strategy to the column names and return the
# result. For compatibility with Spark SQL, all '.'s in column names will be
# replaced with '_'.
repair_names <- function(col_names, names_repair) {
  args <- as.list(rep(NA, length(col_names)))
  names(args) <- col_names
  args <- append(args, list(.name_repair = names_repair))

  do.call(tibble::tibble, args) %>%
    names() %>%
    lapply(function(x) gsub("\\.", "_", x)) %>%
    unlist()
}

# If x is already a Spark data frame, then return dbplyr::remote_name(x)
# Otherwise ensure the result from Spark SQL query encapsulated by x is
# materialized into a Spark temp view and return the name of that temp view
ensure_tmp_view <- function(x) {
  dbplyr::remote_name(x) %||% {
    sc <- spark_connection(x)
    sdf <- spark_dataframe(x)
    data_tmp_view_name <- random_string("sparklyr_tmp_")
    if (spark_version(sc) < "2.0.0") {
      invoke(sdf, "registerTempTable", data_tmp_view_name)
    } else {
      invoke(sdf, "createOrReplaceTempView", data_tmp_view_name)
    }

    data_tmp_view_name
  }
}

process_warnings <- function(out, substr_arr_col, n, extra, fill) {
  if (identical(extra, "warn") || identical(fill, "warn")) {
    output_cols <- colnames(out)
    tmp_tbl_name <- random_string("__tidyr_separate_tmp_tbl_")
    row_num <- random_string("__tidyr_separate_row_num_")
    row_num_sql <- list(dplyr::sql("ROW_NUMBER() OVER (ORDER BY (SELECT 0))"))
    names(row_num_sql) <- row_num
    out <- out %>>%
      dplyr::mutate %@% row_num_sql %>%
      dplyr::compute(name = tmp_tbl_name)
    substr_arr_col_sql <- sprintf(
      "%s.%s",
      quote_sql_name(tmp_tbl_name),
      substr_arr_col
    )
    if (identical(extra, "warn")) {
      pred <- sprintf("SIZE(%s) > %d", substr_arr_col_sql, n)
      rows <- out %>%
        dplyr::filter(dplyr::sql(pred)) %>%
        dplyr::select(rlang::sym(row_num)) %>%
        collect()
      rows <- rows[[row_num]]
      if (length(rows) > 0) {
        sprintf(
          "Expected %d piece(s). Additional piece(s) discarded in %d row(s) [%s].",
          n,
          length(rows),
          paste0(rows, collapse = ", ")
        ) %>%
          rlang::warn()
      }
    }
    if (identical(fill, "warn")) {
      pred <- sprintf("SIZE(%s) < %d", substr_arr_col_sql, n)
      rows <- out %>%
        dplyr::filter(dplyr::sql(pred)) %>%
        dplyr::select(rlang::sym(row_num)) %>%
        collect()
      rows <- rows[[row_num]]
      if (length(rows) > 0) {
        sprintf(
          "Expected %d piece(s). Missing piece(s) filled with NULL value(s) in %d row(s) [%s].",
          n,
          length(rows),
          paste0(rows, collapse = ", ")
        ) %>%
          rlang::warn()
      }
    }
  }

  out
}

strsep_to_sql <- function(column, into, sep) {
  splitting_idx <- lapply(
    sep,
    function(idx) {
      if (idx >= 0) {
        as.character(idx)
      } else {
        sprintf("ARRAY_MAX(ARRAY(0, CHAR_LENGTH(%s) + %d))", column, idx)
      }
    }
  )
  pos <-
    list("0") %>%
    append(splitting_idx) %>%
    append(sprintf("CHAR_LENGTH(%s)", column))
  sql <- lapply(
    seq(length(pos) - 1),
    function(i) {
      from <- sprintf("%s + 1", pos[[i]])
      len <- sprintf("%s - %s", pos[[i + 1]], pos[[i]])

      dplyr::sql(sprintf("SUBSTR(%s, %s, %s)", column, from, len))
    }
  )
  names(sql) <- into

  sql[!is.na(into)]
}

str_split_fixed_to_sql <- function(substr_arr_col, column, sep, n, extra, fill, impl) {
  if (identical(extra, "error")) {
    rlang::warn("`extra = \"error\"` is deprecated. Please use `extra = \"warn\"` instead")
    extra <- "warn"
  }

  extra <- rlang::arg_match(extra, c("warn", "merge", "drop"))
  fill <- rlang::arg_match(fill, c("warn", "left", "right"))

  sep <- dbplyr::translate_sql_(list(sep), con = dbplyr::simulate_dbi())
  limit <- if (identical(extra, "merge")) n else -1L
  sql <- list(dplyr::sql(sprintf("%s(%s, %s, %d)", impl, column, sep, limit)))
  names(sql) <- substr_arr_col

  sql
}

str_separate <- function(data, column, into, sep, extra = "warn", fill = "warn") {
  if (!is.character(into)) {
    rlang::abort("`into` must be a character vector")
  }
  sc <- spark_connection(data)

  if (is.numeric(sep)) {
    sql <- strsep_to_sql(column, into, sep)
    out <- data %>>% dplyr::mutate %@% sql
  } else if (rlang::is_character(sep)) {
    substr_arr_col <- random_string("__tidyr_separate_tmp_")
    n <- length(into)
    split_str_sql <- str_split_fixed_to_sql(
      substr_arr_col = substr_arr_col,
      column = column,
      sep = sep,
      n = n,
      extra = extra,
      fill = fill,
      impl = if (spark_version(sc) >= "3.0.0") "SPLIT" else "SPARKLYR_STR_SPLIT"
    )
    substr_arr_col <- quote_sql_name(substr_arr_col)
    fill_left <- identical(fill, "left")
    assign_results_sql <- lapply(
      seq_along(into),
      function(idx) {
        dplyr::sql(
          if (fill_left) {
            sprintf(
              "IF(%s, NULL, %s)",
              sprintf("%d <= %d - SIZE(%s)", idx, n, substr_arr_col),
              sprintf(
                "ELEMENT_AT(%s, %d - ARRAY_MAX(ARRAY(0, %d - SIZE(%s))))",
                substr_arr_col,
                idx,
                n,
                substr_arr_col
              )
            )
          } else {
            sprintf(
              "IF(%d <= SIZE(%s), ELEMENT_AT(%s, %d), NULL)",
              idx,
              substr_arr_col,
              substr_arr_col,
              idx
            )
          }
        )
      }
    )
    names(assign_results_sql) <- into
    assign_results_sql <- assign_results_sql[!is.na(into)]
    out <- data %>>%
      dplyr::mutate %@% split_str_sql %>%
      process_warnings(substr_arr_col, n, extra, fill) %>>%
      dplyr::mutate %@% assign_results_sql
  } else {
    rlang::abort("`sep` must be either numeric or character")
  }

  out
}

apply_ptype <- function(sdf, ptype) {
  if (is.null(ptype)) {
    sdf
  } else {
    sc <- spark_connection(sdf)
    schema <- spark_data_build_types(sc, lapply(ptype, class))
    invoke_static(
      sc,
      "sparklyr.SchemaUtils",
      "castColumns",
      spark_dataframe(sdf),
      schema
    ) %>%
      sdf_register()
  }
}

apply_transform <- function(sdf, transform) {
  if (length(names(transform)) > 0) {
    out <- list()
    non_transformed_cols <- setdiff(colnames(sdf), names(transform))
    if (length(non_transformed_cols) > 0) {
      out <- append(
        out,
        list(sdf %>>% dplyr::select %@% lapply(non_transformed_cols, as.symbol))
      )
    }
    for (i in seq_along(transform)) {
      tgt <- names(transform[i])
      transform_args <- list(
        do.call(dplyr::vars, list(as.symbol(tgt))), transform[[i]]
      )
      out <- append(
        out,
        list(sdf %>>% dplyr::summarize_at %@% transform_args)
      )
    }
    do.call(cbind, out) %>>%
      dplyr::select %@% lapply(colnames(sdf), as.symbol)
  } else {
    sdf
  }
}
