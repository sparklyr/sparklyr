#' @include dplyr_hof.R
#' @include utils.R
NULL

fix_na_real_values <- function(dots) {
  for (i in seq_along(dots)) {
    if (identical(rlang::quo_get_expr(dots[[i]]), rlang::expr(NA_real_))) {
      dots[[i]] <- rlang::quo(dbplyr::sql("CAST(NULL AS DOUBLE)"))
    }
  }

  dots
}

#' @export
#' @importFrom dplyr transmute
transmute.tbl_spark <- function (.data, ...) {
  dots <- rlang::enquos(..., .named = TRUE) %>%
    fix_na_real_values()

  do.call(NextMethod, dots)
}

#' @export
#' @importFrom dplyr mutate
mutate.tbl_spark <- function (.data, ...) {
  dots <- rlang::enquos(..., .named = TRUE) %>%
    fix_na_real_values()

  do.call(NextMethod, dots)
}

#' @export
#' @importFrom dplyr sql_escape_ident
#' @importFrom dbplyr sql_quote
sql_escape_ident.spark_connection <- function(con, x) {
  # Assuming it might include database name like: `dbname.tableName`
  if (length(x) == 1) {
    tbl_quote_name(con, x)
  } else {
    dbplyr::sql_quote(x, "`")
  }
}

#' @importFrom dbplyr sql
build_sql_if_compare <- function(..., con, compare) {
  args <- list(...)

  build_sql_if_parts <- function(ifParts, ifValues) {
    if (length(ifParts) == 1) {
      return(ifParts[[1]])
    }

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
  sql_if_else <- function(cond, if_true, if_false, if_missing = NULL) {
    dbplyr::build_sql(
      "IF(ISNULL(", cond, "), ",
      if_missing %||% dbplyr::sql("NULL"),
      ", IF(", cond %||% dbplyr::sql("NULL"),
      ", ", if_true %||% dbplyr::sql("NULL"), ", ",
      if_false %||% dbplyr::sql("NULL"), "))"
    )
  }

  weighted_mean_sql <- function(x, w) {
    x <- dbplyr::build_sql(x)
    w <- dbplyr::build_sql(w)
    dbplyr::sql(
      paste(
        "CAST(SUM(IF(ISNULL(", w, "), 0, ", w, ") * IF(ISNULL(", x, "), 0, ", x, ")) AS DOUBLE)",
        "/",
        "CAST(SUM(IF(ISNULL(", w, "), 0, ", w, ") * IF(ISNULL(", x, "), 0, 1)) AS DOUBLE)"
      )
    )
  }

  dbplyr::sql_variant(
    scalar = dbplyr::sql_translator(
      .parent = dbplyr::base_scalar,
      as.numeric = function(x) dbplyr::build_sql("CAST(", x, " AS DOUBLE)"),
      as.double = function(x) dbplyr::build_sql("CAST(", x, " AS DOUBLE)"),
      as.integer = function(x) dbplyr::build_sql("CAST(", x, " AS INT)"),
      as.logical = function(x) dbplyr::build_sql("CAST(", x, " AS BOOLEAN)"),
      as.character = function(x) dbplyr::build_sql("CAST(", x, " AS STRING)"),
      as.date = function(x) dbplyr::build_sql("CAST(", x, " AS DATE)"),
      as.Date = function(x) dbplyr::build_sql("CAST(", x, " AS DATE)"),
      paste = function(..., sep = " ") dbplyr::build_sql("CONCAT_WS", list(sep, ...)),
      paste0 = function(...) dbplyr::build_sql("CONCAT", list(...)),
      xor = function(x, y) dbplyr::build_sql(x, " ^ ", y),
      or = function(x, y) dbplyr::build_sql(x, " OR ", y),
      and = function(x, y) dbplyr::build_sql(x, " AND ", y),
      pmin = function(...) build_sql_if_compare(..., con = con, compare = "<="),
      pmax = function(...) build_sql_if_compare(..., con = con, compare = ">="),
      `%like%` = function(x, y) dbplyr::build_sql(x, " LIKE ", y),
      `%rlike%` = function(x, y) dbplyr::build_sql(x, " RLIKE ", y),
      `%regexp%` = function(x, y) dbplyr::build_sql(x, " REGEXP ", y),
      ifelse = sql_if_else,
      if_else = sql_if_else,
      grepl = function(x, y) dbplyr::build_sql(y, " RLIKE ", x),
      rowSums = function(x) {
        x <- rlang::enexpr(x)
        x <- rlang::eval_tidy(x)
        if (!"tbl_spark" %in% class(x)) {
          "unsupported subsetting expression"
        }
        col_names <- x %>% colnames()

        if (length(col_names) == 0) {
          dbplyr::sql("0")
        } else {
          sum_expr <- list(dbplyr::sql("(")) %>%
            append(
              lapply(
                col_names[-length(col_names)],
                function(x) {
                  list(dbplyr::ident(x), dbplyr::sql(" + "))
                }
              ) %>%
                unlist(recursive = FALSE)
            ) %>%
            append(
              list(dbplyr::ident(col_names[length(col_names)]), dbplyr::sql(")"))
            ) %>%
            lapply(function(x)  dbplyr::escape(x, con = con))
          args <- append(sum_expr, list(con = con))

          do.call(dbplyr::build_sql, args)
        }
      },
      transform = function(expr, func) {
        sprintf(
          "TRANSFORM(%s, %s)", dbplyr::build_sql(expr), build_sql_fn(func)
        ) %>%
          dbplyr::sql()
      },
      filter = function(expr, func) {
        sprintf(
          "FILTER(%s, %s)", dbplyr::build_sql(expr), build_sql_fn(func)
        ) %>%
          dbplyr::sql()
      },
      aggregate = function(expr, start, merge, finish = ~ .x) {
        sprintf(
          "AGGREGATE(%s, %s, %s, %s)",
          dbplyr::build_sql(expr),
          dbplyr::build_sql(start),
          build_sql_fn(merge),
          build_sql_fn(finish)
        ) %>%
          dbplyr::sql()
      },
      exists = function(expr, pred) {
        sprintf(
          "EXISTS(%s, %s)", dbplyr::build_sql(expr), build_sql_fn(pred)
        ) %>%
          dbplyr::sql()
      },
      zip_with = function(left, right, func) {
        sprintf(
          "ZIP_WITH(%s, %s, %s)",
          dbplyr::build_sql(left),
          dbplyr::build_sql(right),
          build_sql_fn(func)
        ) %>%
          dbplyr::sql()
      },
      array_sort = function(expr, func = ~ as.integer(sign(.x - .y))) {
        sprintf(
          "ARRAY_SORT(%s, %s)", dbplyr::build_sql(expr), build_sql_fn(func)
        ) %>%
          dbplyr::sql()
      },
      map_filter = function(expr, func) {
        sprintf(
          "MAP_FILTER(%s, %s)", dbplyr::build_sql(expr), build_sql_fn(func)
        ) %>%
          dbplyr::sql()
      },
      forall = function(expr, pred) {
        sprintf(
          "FORALL(%s, %s)", dbplyr::build_sql(expr), build_sql_fn(pred)
        ) %>%
          dbplyr::sql()
      },
      transform_keys = function(expr, func) {
        sprintf(
          "TRANSFORM_KEYS(%s, %s)", dbplyr::build_sql(expr), build_sql_fn(func)
        ) %>%
          dbplyr::sql()
      },
      transform_values = function(expr, func) {
        sprintf(
          "TRANSFORM_VALUES(%s, %s)", dbplyr::build_sql(expr), build_sql_fn(func)
        ) %>%
          dbplyr::sql()
      },
      map_zip_with = function(map1, map2, func) {
        sprintf(
          "MAP_ZIP_WITH(%s, %s, %s)",
          dbplyr::build_sql(map1),
          dbplyr::build_sql(map2),
          build_sql_fn(func)
        ) %>%
          dbplyr::sql()
      }
    ),

    aggregate = dbplyr::sql_translator(
      .parent = dbplyr::base_agg,
      n = function() dbplyr::sql("COUNT(*)"),
      count = function() dbplyr::sql("COUNT(*)"),
      n_distinct = function(...) dbplyr::build_sql("COUNT(DISTINCT", list(...), ")"),
      cor = dbplyr::sql_prefix("CORR"),
      cov = dbplyr::sql_prefix("COVAR_SAMP"),
      sd = dbplyr::sql_prefix("STDDEV_SAMP"),
      var = dbplyr::sql_prefix("VAR_SAMP"),
      weighted.mean = function(x, w) {
        weighted_mean_sql(x, w)
      }
    ),

    window = dbplyr::sql_translator(
      .parent = dbplyr::base_win,
      lag = function(x, n = 1L, default = NA, order_by = NULL) {
        dbplyr::base_win$lag(
          x = x,
          n = as.integer(n),
          default = default,
          order = order_by
        )
      },
      lead = function(x, n = 1L, default = NA, order_by = NULL) {
        dbplyr::base_win$lead(
          x = x,
          n = as.integer(n),
          default = default,
          order = order_by
        )
      },
      count = function() {
        dbplyr::win_over(
          dbplyr::sql("count(*)"),
          partition = dbplyr::win_current_group()
        )
      },
      n_distinct = dbplyr::win_absent("distinct"),
      cor = win_recycled_params("corr"),
      cov = win_recycled_params("covar_samp"),
      sd = dbplyr::win_recycled("stddev_samp"),
      var = dbplyr::win_recycled("var_samp"),
      cumprod = function(x) {
        dbplyr::win_over(
          dbplyr::build_sql("sparklyr_cumprod(", x, ")"),
          partition = dbplyr::win_current_group(),
          order = dbplyr::win_current_order()
        )
      },
      weighted.mean = function(x, w) {
        dbplyr::win_over(
          weighted_mean_sql(x, w),
          partition = dbplyr::win_current_group()
        )
      }
    )
  )
}

build_sql_fn <- function(fn) {
  as.character(
    if (rlang::is_formula(fn)) {
      process_lambda(fn)
    } else {
      fn
    }
  )
}

#' @export
#' @importFrom dplyr sql_set_op
#' @importFrom dbplyr build_sql
#' @importFrom dbplyr sql
#' @keywords internal
sql_set_op.spark_connection <- function(con, x, y, method) {
  if (spark_version(con) < "2.0.0") {
    # Spark 1.6 does not allow parentheses
    build_sql(
      x,
      "\n", sql(method), "\n",
      y
    )
  } else {
    class(con) <- class(con)[class(con) != "spark_connection"]
    sql_set_op(con, x, y, method)
  }
}

#' @export
#' @importFrom dplyr db_query_fields
#' @importFrom dplyr sql_select
#' @importFrom dplyr sql_subquery
#' @keywords internal
db_query_fields.spark_connection <- function(con, sql, ...) {
  sqlFields <- sql_select(
    con,
    sql("*"),
    sql_subquery(con, sql),
    where = sql("0 = 1")
  )

  hive_context(con) %>%
    invoke("sql", as.character(sqlFields)) %>%
    invoke("schema") %>%
    invoke("fieldNames") %>%
    as.character()
}
