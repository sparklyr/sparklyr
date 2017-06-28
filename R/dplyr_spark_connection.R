#' @export
#' @importFrom dplyr sql_escape_ident
#' @importFrom dbplyr sql_quote
sql_escape_ident.spark_connection <- function(con, x) {
  # Assuming it might include database name like: `dbname.tableName`
  if (length(x) == 1)
    tbl_quote_name(x)
  else
    dbplyr::sql_quote(x, '`')
}

#' @importFrom dbplyr sql
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

      dbplyr::sql(paste(e, compare, ifValues, collapse = " and "))
    }
  })

  build_sql_if_parts(conditions, args)
}

#' @export
#' @importFrom dplyr sql_translate_env
#' @importFrom dbplyr build_sql
#' @importFrom dbplyr win_over
#' @importFrom dbplyr sql
#' @importFrom dbplyr win_current_group
#' @importFrom dbplyr win_current_order
sql_translate_env.spark_connection <- function(con) {
  win_recycled_params <- function(prefix) {

    function(x, y) {
      # Use win_current_frame() once exported form `dbplyr`
      sql_context <- get("sql_context", envir = asNamespace("dbplyr"))
      frame <- sql_context$frame

      dbplyr::win_over(
        dbplyr::build_sql(dbplyr::sql(prefix), "(", x, ",", y, ")"),
        partition = dbplyr::win_current_group(),
        order = if (!is.null(frame)) dbplyr::win_current_order(),
        frame = frame
      )
    }
  }

  dbplyr::sql_variant(

    scalar = dbplyr::sql_translator(
      .parent = dbplyr::base_scalar,
      as.numeric = function(x) dbplyr::build_sql("CAST(", x, " AS DOUBLE)"),
      as.double  = function(x) dbplyr::build_sql("CAST(", x, " AS DOUBLE)"),
      as.integer  = function(x) dbplyr::build_sql("CAST(", x, " AS INT)"),
      as.logical = function(x) dbplyr::build_sql("CAST(", x, " AS BOOLEAN)"),
      as.character  = function(x) dbplyr::build_sql("CAST(", x, " AS STRING)"),
      as.date  = function(x) dbplyr::build_sql("CAST(", x, " AS DATE)"),
      as.Date  = function(x) dbplyr::build_sql("CAST(", x, " AS DATE)"),
      paste = function(..., sep = " ") dbplyr::build_sql("CONCAT_WS", list(sep, ...)),
      paste0 = function(...) dbplyr::build_sql("CONCAT", list(...)),
      xor = function(x, y) dbplyr::build_sql(x, " ^ ", y),
      or = function(x, y) dbplyr::build_sql(x, " or ", y),
      and = function(x, y) dbplyr::build_sql(x, " and ", y),
      pmin = function(...) build_sql_if_compare(..., con = con, compare = "<="),
      pmax = function(...) build_sql_if_compare(..., con = con, compare = ">="),
      `%like%` = function(x, y) dbplyr::build_sql(x, " like ", y),
      `%rlike%` = function(x, y) dbplyr::build_sql(x, " rlike ", y),
      `%regexp%`  = function(x, y) dbplyr::build_sql(x, " regexp ", y)
    ),

    aggregate = dbplyr::sql_translator(
      .parent = dbplyr::base_agg,
      n = function() dbplyr::sql("count(*)"),
      count = function() dbplyr::sql("count(*)"),
      n_distinct = function(...) dbplyr::build_sql("count(DISTINCT", list(...), ")"),
      cor = dbplyr::sql_prefix("corr"),
      cov = dbplyr::sql_prefix("covar_samp"),
      sd =  dbplyr::sql_prefix("stddev_samp"),
      var = dbplyr::sql_prefix("var_samp")
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
      },
      count = function() dbplyr::win_over(
        dbplyr::sql("count(*)"),
        partition = dbplyr::win_current_group()
      ),
      n_distinct = dbplyr::win_absent("distinct"),
      cor = win_recycled_params("corr"),
      cov =  win_recycled_params("covar_samp"),
      sd =  dbplyr::win_recycled("stddev_samp"),
      var = dbplyr::win_recycled("var_samp"),
      cumprod = function(x) {
        dbplyr::build_sql(
          "exp(",
          dbplyr::win_over(
            dbplyr::build_sql("sum(if(", x,
                              "= 0, 0, ln(abs(", x,
                              "))))"),
            partition = dbplyr::win_current_group(),
            order = dbplyr::win_current_order()
          ), ") * (1 - 2 * pmod(",
          # count number of negatives up to current row
          #   and adjust sign accordingly
          dbplyr::win_over(
            dbplyr::build_sql("sum(if(", x,
                              ">= 0, 0, 1))"),
            partition = dbplyr::win_current_group(),
            order = dbplyr::win_current_order()
          ), ", 2)) * if(",
          # if there's a zero then all cumprod after that row
          #   vanish
          dbplyr::win_over(
            dbplyr::build_sql("min(abs(", x,
                              "))"),
            partition = dbplyr::win_current_group(),
            order = dbplyr::win_current_order()
          ), "= 0, 0, 1)"
        )
      }
    )

  )
}
