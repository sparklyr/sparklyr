#' @export
sql_escape_ident.DBISparkConnection <- function(con, x) {
  sql_quote(x, '`')
}

#' @export
sql_translate_env.DBISparkConnection <- function(con) {
  dplyr::sql_variant(
    scalar = dplyr::sql_translator(.parent = dplyr::base_scalar,
                                   n = function() dplyr::sql("count(*)")
    ),
    aggregate = dplyr::sql_translator(.parent = dplyr::base_agg
    ),
    window = dplyr::sql_translator(.parent = dplyr::base_win
    )
  )
}

#' @export
#' @import assertthat
sql_select.DBISparkConnection <- function(con, select, from, where = NULL,
                                          group_by = NULL, having = NULL,
                                          order_by = NULL, distinct = FALSE,
                                          limit = NULL, ...) {
  out <- vector("list", 6)
  names(out) <- c("select", "from", "where", "group_by", "having", "order_by")

  assert_that(is.character(select), length(select) > 0L)
  out$select <- build_sql(
    "SELECT ",
    if (distinct) sql("DISTINCT "),
    escape(select, collapse = ", ", con = con)
  )

  assert_that(is.character(from), length(from) == 1L)
  out$from <- build_sql("FROM ", from, con = con)

  if (length(where) > 0L) {
    assert_that(is.character(where))

    where_paren <- escape(where, parens = TRUE, con = con)
    out$where <- build_sql("WHERE ", dplyr:::sql_vector(where_paren, collapse = " AND "))
  }

  if (length(group_by) > 0L) {
    assert_that(is.character(group_by))
    out$group_by <- build_sql("GROUP BY ",
                              escape(group_by, collapse = ", ", con = con))
  }

  if (length(having) > 0L) {
    assert_that(is.character(having))
    out$having <- build_sql("HAVING ",
                            escape(having, collapse = ", ", con = con))
  }

  if (length(order_by) > 0L) {
    assert_that(is.character(order_by))
    out$order_by <- build_sql("ORDER BY ",
                              escape(order_by, collapse = ", ", con = con))
  }

  if (length(limit) > 0L) {
    assert_that(is.numeric(limit))
    out$limit <- build_sql("LIMIT ", escape(as.integer(limit), con = con))
  }

  compact <- function(x) Filter(Negate(is.null), x)

  escape(unname(compact(out)), collapse = "\n", parens = FALSE, con = con)
}
