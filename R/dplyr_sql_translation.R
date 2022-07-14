#' @importFrom dbplyr sql_translation
#' @export
sql_translation.spark_connection <- function(con) {
  spark_sql_translation(con)
}

#' @importFrom dbplyr build_sql
#' @importFrom dbplyr win_over
#' @importFrom dbplyr sql
#' @importFrom dbplyr win_current_group
#' @importFrom dbplyr win_current_order
spark_sql_translation <- function(con) {
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

  sparklyr_base_sql_variant <- list(
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
      rowSums = function(x, na.rm = FALSE) {
        x <- rlang::enexpr(x)
        x <- rlang::eval_tidy(x)
        if (!"tbl_spark" %in% class(x)) {
          "unsupported subsetting expression"
        }
        col_names <- x %>% colnames()

        if (length(col_names) == 0) {
          dbplyr::sql("0")
        } else {
          as_summand <- function(column) {
            if (!na.rm) {
              list(dbplyr::ident(column))
            } else {
              list(
                dbplyr::sql("IF(ISNULL("),
                dbplyr::ident(column),
                dbplyr::sql("), 0, "),
                dbplyr::ident(column),
                dbplyr::sql(")")
              )
            }
          }
          sum_expr <- list(dbplyr::sql("(")) %>%
            append(
              lapply(
                col_names[-length(col_names)],
                function(x) {
                  append(as_summand(x), list(dbplyr::sql(" + ")))
                }
              ) %>%
                unlist(recursive = FALSE)
            ) %>%
            append(
              as_summand(col_names[[length(col_names)]])
            ) %>%
            append(list(dbplyr::sql(")"))) %>%
            lapply(function(x) dbplyr::escape(x, con = con))
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
      aggregate = function(expr, start, merge, finish = ~.x) {
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
      n_distinct = function(..., na.rm = NULL) {
        if (identical(options("sparklyr.n_distinct.disable-na-rm"), TRUE)) {
          # If `na.rm` option is disabled for backward-compatibility reasons,
          # then fallback to old behavior in sparklyr 1.7 or below
          if (!is.null(na.rm)) {
            warning(
              "`na.rm` option for `n_distinct()` will be ignored while ",
              "options(\"sparklyr.n_distinct.disable-na-rm\") is set to `TRUE`"
            )
          }

          dbplyr::build_sql("COUNT(DISTINCT", list(...), ")")
        } else {
          na.rm <- na.rm %||% FALSE

          if (na.rm) {
            dbplyr::build_sql(
              "COUNT(DISTINCT",
              list(...) %>%
                lapply(
                  function(x) {
                    # consider NaN values as NA to match the `na.rm = TRUE`
                    # behavior of `dplyr::n_distinct()`
                    dbplyr::build_sql("NANVL(", x, ", NULL)")
                  }
                ),
              ")"
            )
          } else {
            dbplyr::build_sql(
              "COUNT(DISTINCT",
              list(...) %>%
                lapply(
                  function(x) {
                    # wrap each expression in a Spark array to ensure `NULL`
                    # values are counted
                    dbplyr::build_sql("ARRAY(", x, ")")
                  }
                ),
              ")"
            )
          }
        }
      },
      cor = dbplyr::sql_aggregate_2("CORR"),
      cov = dbplyr::sql_aggregate_2("COVAR_SAMP"),
      sd = dbplyr::sql_aggregate("STDDEV_SAMP", "sd"),
      var = dbplyr::sql_aggregate("VAR_SAMP", "var"),
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
          dbplyr::sql("COUNT(*)"),
          partition = dbplyr::win_current_group()
        )
      },
      n_distinct = dbplyr::win_absent("DISTINCT"),
      cor = win_recycled_params("CORR"),
      cov = win_recycled_params("COVAR_SAMP"),
      sd = dbplyr::win_recycled("STDDEV_SAMP"),
      var = dbplyr::win_recycled("VAR_SAMP"),
      cumprod = function(x) {
        dbplyr::win_over(
          dbplyr::build_sql("SPARKLYR_CUMPROD(", x, ")"),
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

  dbplyr::sql_variant(
    scalar = do.call(
      dbplyr::sql_translator,
      append(
        list(.parent = sparklyr_base_sql_variant$scalar),
        con$extensions$dbplyr_sql_variant$scalar
      )
    ),
    aggregate = do.call(
      dbplyr::sql_translator,
      append(
        list(.parent = sparklyr_base_sql_variant$aggregate),
        con$extensions$dbplyr_sql_variant$aggregate
      )
    ),
    window = do.call(
      dbplyr::sql_translator,
      append(
        list(.parent = sparklyr_base_sql_variant$window),
        con$extensions$dbplyr_sql_variant$window
      )
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

#' @importFrom dbplyr sql_query_set_op
#' @keywords internal
#' @export
sql_query_set_op.spark_connection <- function(con, x, y, method, ..., all = FALSE) {
  sql <- spark_sql_set_op(con, x, y, method)

  if (!is.null(sql)) {
    sql
  } else {
    class(con) <- class(con)[class(con) != "spark_connection"]
    NextMethod()
  }
}

#' @importFrom dbplyr sql_query_fields
#' @keywords internal
#' @export
sql_query_fields.spark_connection <- function(con, sql, ...) {
  spark_sql_query_fields(con, sql, ...)
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
