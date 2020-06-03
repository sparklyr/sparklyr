#' dplyr wrappers for Apache Spark higher order functions
#'
#' These methods implement dplyr grammars for Apache Spark higher order functions
#'
#' @name dplyr_hof
NULL

# throw an error if f is not a valid lambda expression
validate_lambda <- function(f) {
  if (! "spark_sql_lambda" %in% class(f) && ! "formula" %in% class(f))
    stop("Expected 'f' to be a lambda expression (e.g., 'a %->% (a + 1)' or ",
         "'.(a, b) %->% (a + b + 1)') or a formula (e.g., '~ .x + 1' or ",
         "'~ .x + .y + 1').")
}

translate_formula <- function(f) {
  params_sql <- paste0("(", paste0(all.vars(f), collapse = ", "), ")")
  body_sql <- dbplyr::translate_sql_(f[[2]], con = dbplyr::simulate_dbi())
  lambda <- dbplyr::sql(paste(params_sql, "->", body_sql))

  lambda
}

process_lambda <- function(f) {
  validate_lambda(f)
  if ("formula" %in% class(f))
    f <- translate_formula(f)
  else
    f
}

#' Infix operator for composing a lambda expression
#'
#' Infix operator that allows a lambda expression to be composed in R and be
#' translated to Spark SQL equivalent using ' \code{dbplyr::translate_sql} functionalities
#'
#' Notice when composing a lambda expression in R, the body of the lambda expression
#' *must always be surrounded with parentheses*, otherwise a parsing error will occur.
#'
#' @param params Parameter(s) of the lambda expression, can be either a single
#'   parameter or a comma separated listed of parameters in the form of
#'   \code{.(param1, param2, ... )} (see examples)
#' @param body Body of the lambda expression, *must be within parentheses*
#'
#' @examples
#' \dontrun{
#'
#' a %->% (mean(a) + 1) # translates to <SQL> `a` -> (AVG(`a`) OVER () + 1.0)
#'
#' .(a, b) %->% (a < 1 && b > 1) # translates to <SQL> `a`,`b` -> (`a` < 1.0 AND `b` > 1.0)
#'}
#' @export
`%->%` <- function(params, body) {
  `.` <- function(...) {
    params <- do.call(
      paste,
      c(lapply(rlang::ensyms(...), as.character), sep = ", ")
    )
    paste0("(", params, ")")
  }
  process_params <- function(x) {
    if ("call" %in% class(x))
      # params is of the form '.(<comma-separated list of variables>)'
      eval(x)
    else
      # params consists of a single variable
      x
  }

  params_sql <- rlang::enexpr(params) %>%
    process_params() %>%
    lapply(as.character) %>%
    c(sep = ",") %>%
    do.call(paste, .)

  body_sql <- dbplyr::translate_sql_(
    rlang::enexprs(body),
    con = dbplyr::simulate_dbi()
  )

  lambda <- dbplyr::sql(paste(params_sql, "->", body_sql))
  class(lambda) <- c(class(lambda), "spark_sql_lambda")

  lambda
}

do.mutate <- function(x, dest_col_name, sql, ...) {
  args <- list(dbplyr::sql(sql))
  names(args) <- as.character(dest_col_name)

  do.call(dplyr::mutate, c(list(x), args, list(...)))
}

#' dplyr wrapper for \code{transform(array<T>, function<T, U>): array<U>} and
#' \code{transform(array<T>, function<T, Int, U>): array<U>} in Spark SQL
#'
#' @param x The Spark data frame to transform
#' @param dest_col Column to store the transformed result
#' @param expr The array being transformed (could be any SQL expression evaluating to an array)
#' @param func The transformation to apply
#' @param ... Additional params to dplyr::mutate
#'
#' @export
hof_transform <- function(x, dest_col, expr, func, ...) {
  func <- process_lambda(func)
  sql <- paste(
    "TRANSFORM(",
    as.character(dbplyr::translate_sql(!! rlang::enexpr(expr))),
    ",",
    as.character(func),
    ")"
  )

  do.mutate(x, rlang::ensym(dest_col), sql, ...)
}

#' dplyr wrapper for \code{filter(array<T>, function<T, Boolean>): array<T>} in Spark SQL
#'
#' @param x The Spark data frame to filter
#' @param dest_col Column to store the filtered result
#' @param expr The array being filtered (could be any SQL expression evaluating to an array)
#' @param func The filtering function
#' @param ... Additional params to dplyr::mutate
#'
#' @export
hof_filter <- function(x, dest_col, expr, func, ...) {
  func <- process_lambda(func)
  sql <- paste(
    "FILTER(",
    as.character(dbplyr::translate_sql(!! rlang::enexpr(expr))),
    ",",
    as.character(func),
    ")"
  )

  do.mutate(x, rlang::ensym(dest_col), sql, ...)
}

#' dplyr wrapper for \code{aggregate(array<T>, A, function<A, T, A>[, function<A, R>]): R}
#'
#' @param x The Spark data frame to run aggregation on
#' @param dest_col Column to store the aggregated result
#' @param expr The array being aggregated (could be any SQL expression evaluating to an array)
#' @param start The starting value of the aggregation
#' @param merge The aggregation function
#' @param finish Optional param specifying a transformation to apply on the final value of the aggregation
#' @param ... Additional params to dplyr::mutate
#'
#' @export
hof_aggregate <- function(x, dest_col, expr, start, merge, finish = NULL, ...) {
  merge <- process_lambda(merge)
  args <- list(...)
  if (!identical(finish, NULL)) finish <- process_lambda(finish)
  sql <- do.call(paste, as.list(c(
    "AGGREGATE(",
    as.character(dbplyr::translate_sql(!! rlang::enexpr(expr))),
    ",",
    as.character(dbplyr::translate_sql(!! rlang::enexpr(start))),
    ",",
    as.character(merge),
    if (identical(finish, NULL)) NULL else c(",", as.character(finish)),
    ")"
  )))

  do.call(
    do.mutate,
    c(list(x, as.character(rlang::ensym(dest_col)), sql), args)
  )
}

#' dplyr wrapper for \code{exists(array<T>, function<T, Boolean>): Boolean}
#'
#' @param x The Spark data frame to search
#' @param dest_col Column to store the search result
#' @param expr The array being searched (could be any SQL expression evaluating to an array)
#' @param pred A boolean predicate
#' @param ... Additional params to dplyr::mutate
#'
#' @export
hof_exists <- function(x, dest_col, expr, pred, ...) {
  validate_lambda(pred)
  sql <- paste(
    "EXISTS(",
    as.character(dbplyr::translate_sql(!! rlang::enexpr(expr))),
    ",",
    as.character(pred),
    ")"
  )

  do.mutate(x, rlang::ensym(dest_col), sql, ...)
}

#' dplyr wrapper for \code{zip_with(array<T>, array<U>, function<T, U, R>): array<R>}
#'
#' @param x The Spark data frame to process
#' @param dest_col Column to store the query result
#' @param left Any expression evaluating to an array
#' @param right Any expression evaluating to an array
#' @param func Element-wise merge function to be applied
#' @param ... Additional params to dplyr::mutate
#'
#' @export
hof_zip_with <- function(x, dest_col, left, right, func, ...) {
  validate_lambda(func)
  sql <- paste(
    "ZIP_WITH(",
    as.character(dbplyr::translate_sql(!! rlang::enexpr(left))),
    ",",
    as.character(dbplyr::translate_sql(!! rlang::enexpr(right))),
    ",",
    as.character(func),
    ")"
  )

  do.mutate(x, rlang::ensym(dest_col), sql, ...)
}
