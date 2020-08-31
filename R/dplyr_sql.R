#' Helper functions to support dplyr sql operations
#'
#' @include sdf_interface.R
#' @include sdf_sql.R
NULL

#' @export
#' @importFrom dbplyr sql_build
#' @importFrom dbplyr select_query
sql_build.op_sample_n <- function(op, con, ...) {
  if (rlang::quo_is_null(op$args$weight)) {
    select_query(
      from = sql(
        sql_render(sql_build(op$x, con = con), con = con),
        sql(paste0(" TABLESAMPLE (",
          as.integer(op$args$size),
          " rows) ",
          collapse = ""
        )),
        con = con
      ) %>%
        as.character() %>%
        paste0(collapse = "") %>%
        sql(),
      select = build_sql("*", con = con)
    )
  } else {
    sql_build.op_weighted_sample(op, con, frac = FALSE)
  }
}

#' @export
#' @importFrom dbplyr sql_build
#' @importFrom dbplyr select_query
sql_build.op_sample_frac <- function(op, con, ...) {
  if (rlang::quo_is_null(op$args$weight)) {
    select_query(
      from = sql(
        sql_render(sql_build(op$x, con = con), con = con),
        sql(paste0(" TABLESAMPLE (",
          op$args$size * 100,
          " PERCENT)",
          collapse = ""
        )),
        con = con
      ) %>%
        as.character() %>%
        paste0(collapse = "") %>%
        sql(),
      select = build_sql("*", con = con)
    )
  } else {
    sql_build.op_weighted_sample(op, con, frac = TRUE)
  }
}

sql_build.op_weighted_sample <- function(op, con, frac = FALSE) {
  sdf <- sdf_sql(
    con,
    select_query(
      from = sql(
        sql_render(sql_build(op$x, con = con), con = con),
        con = con
      ),
      select = build_sql("*", con = con)
    ) %>%
      sql_render(con = con) %>%
      sql() %>%
      as.character() %>%
      paste0(collapse = "")
  )

  sample_size <- (
    if (frac) {
      cnt <- sdf %>% spark_dataframe() %>% invoke("count")
      round(cnt * check_frac(op$args$size, replace = op$args$replace))
    } else {
      op$args$size
    }
  )
  weight <- rlang::as_name(op$args$weight)

  sample_sdf <- sdf_weighted_sample(
    x = sdf,
    weight_col = weight,
    k = sample_size,
    replacement = op$args$replace,
    seed = NULL
  )

  select_query(
    from = sample_sdf %>%
      dbplyr::remote_name() %>%
      sql_build(con = con) %>%
      sql_render(con = con) %>%
      sql() %>%
      as.character() %>%
      paste0(collapse = "") %>%
      sql(),
    select = build_sql("*", con = con)
  )
}

check_frac <- function(size, replace = FALSE) {
  if (size <= 1 || replace) return(invisible(size))

  rlang::abort("size", "of sampled fraction must be less or equal to one, ",
    "set `replace` = TRUE to use sampling with replacement"
  )
}
