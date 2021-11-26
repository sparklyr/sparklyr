#' @include dbplyr_utils.R
#' @include dplyr_hof.R
#' @include partial_eval.R
#' @include spark_sql.R
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
transmute.tbl_spark <- function(.data, ...) {
  dots <- rlang::enquos(..., .named = TRUE) %>%
    fix_na_real_values() %>%
    partial_eval_dots(sim_data = simulate_vars(.data))

  nest_vars(.data, dots, character())
}

#' @export
#' @importFrom dplyr mutate
mutate.tbl_spark <- function(.data, ...) {
  dots <- rlang::enquos(..., .named = TRUE) %>%
    fix_na_real_values() %>%
    partial_eval_dots(sim_data = simulate_vars(.data))

  nest_vars(.data, dots, union(dbplyr::op_vars(.data), dbplyr::op_grps(.data)))
}

#' @export
#' @importFrom dplyr filter
filter.tbl_spark <- function(.data, ..., .preserve = FALSE) {
  if (!identical(.preserve, FALSE)) {
    stop("`.preserve` is not supported on database backends", call. = FALSE)
  }

  dots <- rlang::quos(...)
  dots <- partial_eval_dots(dots, sim_data = simulate_vars(.data))
  dbplyr::add_op_single("filter", .data, dots = dots)
}

#' @export
#' @importFrom dplyr select
select.tbl_spark <- function(.data, ...) {
  sim_data <- simulate_vars(.data)
  loc <- tidyselect::eval_select(
    rlang::expr(c(...)),
    sim_data,
    include = dbplyr::op_grps(.data$ops)
  )
  new_vars <- rlang::set_names(rlang::syms(names(sim_data)[loc]), names(loc))
  .class <- class(.data)
  class(.data) <- setdiff(class(.data), "tbl_spark")
  .data <- do.call(
    select,
    append(list(.data), as.list(new_vars))
  )
  class(.data) <- .class

  .data
}

#' @export
#' @importFrom dplyr summarise
#' @importFrom dbplyr add_op_single
#' @importFrom dbplyr op_vars
summarise.tbl_spark <- function(.data, ..., .groups = NULL) {
  # NOTE: this is mostly copy-pasted from
  # https://github.com/tidyverse/dbplyr/blob/master/R/verb-summarise.R
  # except for minor changes (see "partial-eval.R") to make use cases such as
  # `summarise(across(where(is.numeric), mean))` work as expected for Spark
  # dataframes
  dots <- rlang::quos(..., .named = TRUE)
  dots <- dots %>% partial_eval_dots(
    sim_data = simulate_vars(.data),
    ctx = "summarize",
    supports_one_sided_formula = FALSE
  )

  # For each expression, check if it uses any newly created variables
  check_summarise_vars <- function(dots) {
    for (i in seq_along(dots)) {
      used_vars <- all_names(rlang::get_expr(dots[[i]]))
      cur_vars <- names(dots)[seq_len(i - 1)]

      if (any(used_vars %in% cur_vars)) {
        stop(
          "`", names(dots)[[i]],
          "` refers to a variable created earlier in this summarise().\n",
          "Do you need an extra mutate() step?",
          call. = FALSE
        )
      }
    }
  }

  check_groups <- function(.groups) {
    if (rlang::is_null(.groups)) {
      return()
    }

    if (.groups %in% c("drop_last", "drop", "keep")) {
      return()
    }

    rlang::abort(c(
      paste0(
        "`.groups` can't be ", rlang::as_label(.groups),
        if (.groups == "rowwise") " in dbplyr"
      ),
      i = 'Possible values are NULL (default), "drop_last", "drop", and "keep"'
    ))
  }

  check_summarise_vars(dots)
  check_groups(.groups)

  add_op_single(
    "summarise",
    .data,
    dots = dots,
    args = list(.groups = .groups, env_caller = rlang::caller_env())
  )
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

#' @rawNamespace
#' if (utils::packageVersion("dbplyr") < "2") {
#'   importFrom(dplyr, sql_translate_env)
#'   S3method(sql_translate_env, spark_connection)
#' } else {
#'   importFrom(dbplyr, sql_translation)
#'   S3method(sql_translation, spark_connection)
#' }

sql_translate_env.spark_connection <- function(con) {
  spark_sql_translation(con)
}

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

#' @rawNamespace
#' if (utils::packageVersion("dbplyr") < "2") {
#'   importFrom(dplyr, sql_set_op)
#'   S3method(sql_set_op, spark_connection)
#' } else {
#'   importFrom(dbplyr, sql_query_set_op)
#'   S3method(sql_query_set_op, spark_connection)
#' }

#' @keywords internal
sql_set_op.spark_connection <- function(con, x, y, method) {
  sql <- spark_sql_set_op(con, x, y, method)

  if (!is.null(sql)) {
    sql
  } else {
    class(con) <- class(con)[class(con) != "spark_connection"]
    NextMethod()
  }
}

#' @keywords internal
sql_query_set_op.spark_connection <- function(con, x, y, method, ..., all = FALSE) {
  sql <- spark_sql_set_op(con, x, y, method)

  if (!is.null(sql)) {
    sql
  } else {
    class(con) <- class(con)[class(con) != "spark_connection"]
    NextMethod()
  }
}

#' @rawNamespace
#' if (utils::packageVersion("dbplyr") < "2") {
#'   importFrom(dplyr, db_query_fields)
#'   S3method(db_query_fields, spark_connection)
#' } else {
#'   importFrom(dbplyr, sql_query_fields)
#'   S3method(sql_query_fields, spark_connection)
#' }

#' @keywords internal
db_query_fields.spark_connection <- function(con, sql, ...) {
  spark_db_query_fields(con, sql)
}

#' @keywords internal
sql_query_fields.spark_connection <- function(con, sql, ...) {
  spark_sql_query_fields(con, sql, ...)
}
