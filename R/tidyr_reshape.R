#' @include tidyr_utils.R
NULL

validate_args <- function(into, sep) {
  if (!is.character(into)) {
    rlang::abort("`into` must be a character vector")
  }

  if (!is.numeric(sep) && !rlang::is_character(sep)) {
    rlang::abort("`sep` must be either numeric or character")
  }

  if (is.numeric(sep) && !identical(length(sep) + 1L, length(into))) {
    rlang::abort("The length of `sep` should be one less than `into`.")
  }
}

#' @importFrom tidyr separate
#' @export
separate.tbl_spark <- function(
  data,
  col,
  into,
  sep = "[^[:alnum:]]+",
  remove = TRUE,
  convert = FALSE,
  extra = "warn",
  fill = "warn",
  ...
) {
  if (!identical(convert, FALSE)) {
    rlang::warn("`convert` will be ignored for Spark dataframes!")
  }
  check_present(col)
  if (rlang::is_character(sep)) {
    sep <- pcre_to_java(sep)
  }
  validate_args(into, sep)

  sc <- spark_connection(data)
  if (spark_version(sc) < "2.4.0") {
    rlang::abort("`separate.tbl_spark` requires Spark 2.4.0 or higher")
  }

  var <- tidyselect::vars_pull(colnames(data), !!rlang::enquo(col))

  out <- str_separate(data, quote_sql_name(var), into, sep, extra, fill)
  preserved <- setdiff(colnames(data), into)
  preserved <- setdiff(preserved, var)
  output_cols <- if (remove) into else union(colnames(data), into)
  output_cols <- union(preserved, output_cols)
  output_cols <- output_cols %>% lapply(as.symbol)

  update_group_vars(data, out, preserved) %>>%
    dplyr::select %@%
    output_cols
}

#' @importFrom tidyr unite
#' @export
unite.tbl_spark <- function(
  data,
  col,
  ...,
  sep = "_",
  remove = TRUE,
  na.rm = FALSE
) {
  col <- rlang::as_string(rlang::ensym(col))

  if (rlang::dots_n(...) == 0) {
    src_cols <- colnames(data)
  } else {
    src_cols <- names(
      tidyselect::eval_select(rlang::expr(c(...)), replicate_colnames(data))
    )
  }

  output_cols <- colnames(data)
  if (remove) {
    output_cols <- setdiff(output_cols, src_cols)
  }

  first_pos <- which(colnames(data) %in% src_cols)[[1]]
  output_cols <- append(output_cols, col, after = first_pos - 1L)

  concat_ws_args <- lapply(
    src_cols,
    function(col) {
      col <- quote_sql_name(col)
      if (!na.rm) {
        paste0("IF(ISNULL(", col, "), \"NA\", ", col, ")")
      } else {
        col
      }
    }
  )
  sql <- paste0(
    append(
      list(
        dbplyr::translate_sql(!!sep, con = dbplyr::simulate_spark_sql())
      ),
      concat_ws_args
    ),
    collapse = ", "
  ) %>%
    paste0("CONCAT_WS(", ., ")") %>%
    dplyr::sql() %>%
    list()
  names(sql) <- col

  data %>>%
    dplyr::mutate %@%
    sql %>%
    dplyr::ungroup(setdiff(colnames(data), output_cols)) %>%
    update_group_vars(data, ., output_cols) %>>%
    dplyr::select %@%
    lapply(output_cols, as.symbol)
}

#' @importFrom tidyr fill
#' @export
fill.tbl_spark <- function(
  data,
  ...,
  .direction = c("down", "up", "downup", "updown")
) {
  if (data %>% spark_connection() %>% spark_version() < "2.0.0") {
    rlang::abort("`fill.tbl_spark` requires Spark 2.0.0 or higher")
  }

  .direction <- match.arg(.direction)

  group_vars <- data %>% dplyr::group_vars()

  order_exprs <- lapply(dbplyr::op_sort(data), rlang::get_expr)
  if ("op_arrange" %in% class(data$lazy_query)) {
    # no need to include 'order by' in the input when the same 'order by' will
    # be part of the window spec
    data$lazy_query <- data$lazy_query$x
  }

  cols <- colnames(data)
  vars <- names(tidyselect::eval_select(
    rlang::expr(c(...)),
    replicate_colnames(data)
  ))

  sql <- lapply(
    vars,
    function(col) {
      switch(
        .direction,
        down = fill_down_sql(col, group_vars, order_exprs),
        up = fill_up_sql(col, group_vars, order_exprs),
        downup = sprintf(
          "COALESCE(%s, %s)",
          fill_down_sql(col, group_vars, order_exprs),
          fill_up_sql(col, group_vars, order_exprs)
        ),
        updown = sprintf(
          "COALESCE(%s, %s)",
          fill_up_sql(col, group_vars, order_exprs),
          fill_down_sql(col, group_vars, order_exprs)
        )
      ) %>%
        dplyr::sql()
    }
  )
  names(sql) <- vars

  data %>>% dplyr::mutate %@% sql
}

fill_down_sql <- function(col, group_vars, order_exprs) {
  sprintf(
    "LAST(%s, TRUE) OVER (%s ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
    quote_sql_name(col),
    to_partition_spec(group_vars, order_exprs)
  )
}

fill_up_sql <- function(col, group_vars, order_exprs) {
  sprintf(
    "FIRST(%s, TRUE) OVER (%s ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)",
    quote_sql_name(col),
    to_partition_spec(group_vars, order_exprs)
  )
}

to_partition_spec <- function(group_vars, order_exprs) {
  spec <- (if (length(group_vars) > 0) {
    sprintf(
      "PARTITION BY (%s)",
      lapply(group_vars, quote_sql_name) %>%
        paste0(collapse = ", ")
    )
  } else {
    ""
  })
  if (length(order_exprs) > 0) {
    spec <- sprintf(
      "%s ORDER BY %s",
      spec,
      order_exprs %>%
        lapply(
          function(x) {
            as.character(dbplyr::translate_sql(
              !!x,
              con = dbplyr::simulate_spark_sql()
            ))
          }
        ) %>%
        paste0(collapse = ", ")
    )
  }

  spec
}
