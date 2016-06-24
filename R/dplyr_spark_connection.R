#' @export
sql_escape_ident.DBISparkConnection <- function(con, x) {
  sql_quote(x, '`')
}

#' @export
sql_translate_env.DBISparkConnection <- function(con) {
  dplyr::sql_variant(

    scalar = dplyr::sql_translator(
      .parent = dplyr::base_scalar,
      as.numeric = function(x) build_sql("CAST(", x, " AS DOUBLE)"),
      as.double  = function(x) build_sql("CAST(", x, " AS DOUBLE)")
    ),

    aggregate = dplyr::sql_translator(
      .parent = dplyr::base_agg,
      n = function() dplyr::sql("count(*)"),
      count = function() dplyr::sql("count(*)")
    ),

    window = dplyr::sql_translator(
      .parent = dplyr::base_win
    )

  )
}

#' @export
#' @import assertthat
sql_select.DBISparkConnection <- function(con, select, from, where = NULL,
                                          group_by = NULL, having = NULL,
                                          order_by = NULL, limit = NULL,
                                          distinct = FALSE, ...) {
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
    out$where <- build_sql("WHERE ", dplyr::sql_vector(where_paren, collapse = " AND "))
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

#' @importFrom utils compareVersion
#' @export
sql_join.DBISparkConnection <- function(con, x, y, type = "inner", by = NULL, ...) {
  # TODO: This function needs to be removed once dplyr can workaround this issue by avoiding USING statements.
  
  sparkVersion <- spark_connection_version(con@scon, onlyVersion = TRUE)
  
  if (compareVersion(sparkVersion, "2.0.0") < 0) {
    sameNameColumns <- length(Filter(function(e) by$x[[e]] == by$y[[e]], seq_len(length(by$x))))
    if (sameNameColumns > 0) {
      stop(paste("This dplyr operation requires a feature not supported in Spark", sparkVersion,
                ". Try Spark 2.0.0 instead or avoid using same-column names in joins."))
    }
  }
  
  # Invoke dplyrs default join:
  join <- switch(type,
                 left = sql("LEFT"),
                 inner = sql("INNER"),
                 right = sql("RIGHT"),
                 full = sql("FULL"),
                 stop("Unknown join type:", type, call. = FALSE)
  )
  
  using <- all(by$x == by$y)
  
  if (using) {
    cond <- build_sql("USING ", lapply(by$x, ident), con = con)
  } else {
    on <- sql_vector(paste0(sql_escape_ident(con, by$x), " = ", sql_escape_ident(con, by$y)),
                     collapse = " AND ", parens = TRUE)
    cond <- build_sql("ON ", on, con = con)
  }
  
  build_sql(
    'SELECT * FROM ',x, "\n\n",
    join, " JOIN\n\n" ,
    y, "\n\n",
    cond,
    con = con
  )
}
