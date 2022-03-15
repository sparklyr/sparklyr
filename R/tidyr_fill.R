#' @include tidyr_utils.R
NULL

#' @importFrom tidyr fill
#' @export
fill.tbl_spark <- function(data, ..., .direction = c("down", "up", "downup", "updown")) {
  if (data %>% spark_connection() %>% spark_version() < "2.0.0") {
    rlang::abort("`fill.tbl_spark` requires Spark 2.0.0 or higher")
  }

  .direction <- match.arg(.direction)

  group_vars <- data %>% dplyr::group_vars()

  if (dbplyr_uses_ops()) {
    order_exprs <- lapply(dbplyr::op_sort(data$ops), rlang::get_expr)
    if ("op_arrange" %in% class(data$ops)) {
      # no need to include 'order by' in the input when the same 'order by' will
      # be part of the window spec
      data$ops <- data$ops$x
    }
  } else {
    order_exprs <- lapply(dbplyr::op_sort(data), rlang::get_expr)
    if ("op_arrange" %in% class(data$lazy_query)) {
      # no need to include 'order by' in the input when the same 'order by' will
      # be part of the window spec
      data$lazy_query <- data$lazy_query$x
    }
  }
  cols <- colnames(data)
  vars <- names(tidyselect::eval_select(rlang::expr(c(...)), replicate_colnames(data)))

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
  spec <- (
    if (length(group_vars) > 0) {
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
            as.character(dbplyr::translate_sql(!!x))
          }
        ) %>%
        paste0(collapse = ", ")
    )
  }

  spec
}
