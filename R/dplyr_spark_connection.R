#' @export
sql_escape_ident.spark_connection <- function(con, x) {
  sql_quote(x, '`')
}

build_sql_if_compare <- function(..., con, compare) {
  args <- list(...)

  build_sql_if_parts <- function(ifParts, ifValues) {
    if(length(ifParts) == 1) return(ifParts[[1]])

    current <- ifParts[[1]]
    currentName <- ifValues[[1]]
    build_sql(
      "if(",
      current,
      ", ",
      currentName,
      ", ",
      build_sql_if_parts(ifParts[-1], ifValues[-1]),
      ")"
    )
  }

  thisIdx <- 0
  conditions <- lapply(seq_along(args), function(idx) {
    thisIdx <<- thisIdx + 1
    e <- args[[idx]]

    if (thisIdx == length(args)) {
      e
    } else {
      indexes <- Filter(function(innerIdx) innerIdx > thisIdx, seq_along(args))
      ifValues <- lapply(indexes, function(e) args[[e]])

      sql(paste(e, compare, ifValues, collapse = " and "))
    }
  })

  build_sql_if_parts(conditions, args)
}

#' @export
sql_translate_env.spark_connection <- function(con) {
  dplyr::sql_variant(

    scalar = dplyr::sql_translator(
      .parent = dplyr::base_scalar,
      as.numeric = function(x) build_sql("CAST(", x, " AS DOUBLE)"),
      as.double  = function(x) build_sql("CAST(", x, " AS DOUBLE)"),
      as.integer  = function(x) build_sql("CAST(", x, " AS INT)"),
      as.logical = function(x) build_sql("CAST(", x, " AS BOOLEAN)"),
      as.character  = function(x) build_sql("CAST(", x, " AS STRING)"),
      as.date  = function(x) build_sql("CAST(", x, " AS DATE)"),
      as.Date  = function(x) build_sql("CAST(", x, " AS DATE)"),
      paste = function(..., sep = " ") build_sql("CONCAT_WS", list(sep, ...)),
      paste0 = function(...) build_sql("CONCAT", list(...)),
      xor = function(x, y) build_sql(x, " ^ ", y),
      or = function(x, y) build_sql(x, " or ", y),
      and = function(x, y) build_sql(x, " and ", y),
      pmin = function(...) build_sql_if_compare(..., con = con, compare = "<="),
      pmax = function(...) build_sql_if_compare(..., con = con, compare = ">=")
    ),

    aggregate = dplyr::sql_translator(
      .parent = dplyr::base_agg,
      n = function() dplyr::sql("count(*)"),
      n_distinct = function(...) dplyr::build_sql("count(DISTINCT", list(...), ")"),
      count = function() dplyr::sql("count(*)"),
      cor = sql_prefix("corr"),
      cov = sql_prefix("covar_samp"),
      sd =  sql_prefix("stddev_samp"),
      var = sql_prefix("var_samp")
    ),

    window = dplyr::sql_translator(
      .parent = dplyr::base_win
    )

  )
}

#' @export
#' @import assertthat
sql_select.spark_connection <- function(con, select, from, where = NULL,
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

#' @export
sql_join.spark_connection <- function(con, x, y, type = "inner", by = NULL, ...) {
  # TODO: This function needs to be removed once dplyr can workaround this issue by avoiding USING statements.

  sparkVersion <- spark_version(con)

  if (compareVersion(toString(sparkVersion), "2.0.0") < 0) {
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
