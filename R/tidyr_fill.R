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
  cols <- colnames(data)
  vars <- names(tidyselect::eval_select(rlang::expr(c(...)), replicate_colnames(data)))

  sql <- lapply(
    vars,
    function(col) {
      switch(
        .direction,
        down = fill_down_sql(col, group_vars),
        up = fill_up_sql(col, group_vars),
        downup = sprintf(
          "COALESCE(%s, %s)",
          fill_down_sql(col, group_vars),
          fill_up_sql(col, group_vars)
        ),
        updown = sprintf(
          "COALESCE(%s, %s)",
          fill_up_sql(col, group_vars),
          fill_down_sql(col, group_vars)
        )
      ) %>%
        dplyr::sql()
    }
  )
  names(sql) <- vars

  data %>>% dplyr::mutate %@% sql
}

fill_down_sql <- function(col, group_vars) {
  sprintf(
    "LAST(%s, TRUE) OVER (%sROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
    quote_sql_name(col),
    to_partition_spec(group_vars)
  )
}

fill_up_sql <- function(col, group_vars) {
  sprintf(
    "FIRST(%s, TRUE) OVER (%sROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)",
    quote_sql_name(col),
    to_partition_spec(group_vars)
  )
}

to_partition_spec <- function(group_vars) {
  if (length(group_vars) > 0) {
    sprintf(
      "PARTITION BY (%s) ",
      lapply(group_vars, quote_sql_name) %>%
        paste0(collapse = ", ")
    )
  } else {
    ""
  }
}
