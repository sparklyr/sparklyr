#' @export
#' @importFrom dbplyr sql_escape_ident
#' @importFrom dbplyr sql_quote
sql_escape_ident.spark_connection <- function(con, x) {
  # Assuming it might include database name like: `dbname.tableName`
  if (length(x) == 1)
    tbl_quote_name(x)
  else
    dbplyr::sql_quote(x, '`')
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
#' @importFrom dbplyr sql_translate_env
sql_translate_env.spark_connection <- function(con) {
  dbplyr::sql_variant(

    scalar = dbplyr::sql_translator(
      .parent = dbplyr::base_scalar,
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

    aggregate = dbplyr::sql_translator(
      .parent = dbplyr::base_agg,
      n = function() dbplyr::sql("count(*)"),
      count = function() dbplyr::sql("count(*)"),
      n_distinct = function(...) dbplyr::build_sql("count(DISTINCT", list(...), ")"),
      cor = function(...) dbplyr::build_sql("corr(", list(...), ")"),
      cov = function(...) dbplyr::build_sql("covar_samp(", list(...), ")"),
      sd =  function(...) dbplyr::build_sql("stddev_samp(", list(...), ")"),
      var = function(...) dbplyr::build_sql("var_samp(", list(...), ")")
    ),

    window = dbplyr::sql_translator(
      .parent = dbplyr::base_win,
      lag = function(x, n = 1L, default = NA, order = NULL) {
        dbplyr::base_win$lag(
          x = x,
          n = as.integer(n),
          default = default,
          order = order
        )
      }
    )

  )
}
