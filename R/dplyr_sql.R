#' @include sdf_interface.R
#' @include sdf_sql.R
#' @include utils.R
NULL

#' @export
#' @importFrom dbplyr sql_build
#' @importFrom dbplyr select_query
sql_build.op_sample_n <- function(op, con, ...) {
  if (rlang::quo_is_null(op$args$weight)) {
    sql_build.op_sample(op, con, frac = FALSE)
  } else {
    sql_build.op_weighted_sample(op, con, frac = FALSE)
  }
}

#' @export
#' @importFrom dbplyr sql_build
#' @importFrom dbplyr select_query
sql_build.op_sample_frac <- function(op, con, ...) {
  if (rlang::quo_is_null(op$args$weight)) {
    sql_build.op_sample(op, con, frac = TRUE)
  } else {
    sql_build.op_weighted_sample(op, con, frac = TRUE)
  }
}

sql_build.op_sample <- function(op, con, frac) {
  sdf <- to_sdf(op, con)
  cols <- colnames(sdf)

  sample_size <- (
    if (frac) {
      cnt <- sdf %>% spark_dataframe() %>% invoke("count")
      round(cnt * check_frac(op$args$size, replace = op$args$replace))
    } else {
      op$args$size
    }
  )
  sample_sdf <- sdf_weighted_sample(
    x = sdf,
    weight_col = NULL,
    k = sample_size,
    replacement = op$args$replace,
    seed = op$args$seed
  ) %>>%
    dplyr::select %@% lapply(cols, as.symbol)

  sample_sdf %>% dbplyr::remote_query()
}

sql_build.op_weighted_sample <- function(op, con, frac) {
  sdf <- to_sdf(op, con)

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
    seed = op$args$seed
  )

  sample_sdf %>% dbplyr::remote_query()
}

to_sdf <- function(op, con) {
  sdf_sql(
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
}

check_frac <- function(size, replace = FALSE) {
  if (size <= 1 || replace) return(invisible(size))

  rlang::abort("size", "of sampled fraction must be less or equal to one, ",
    "set `replace` = TRUE to use sampling with replacement"
  )
}
