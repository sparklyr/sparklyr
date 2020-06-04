#' dplyr wrappers for Apache Spark higher order functions
#'
#' These methods implement dplyr grammars for Apache Spark higher order functions
#'
#' @name dplyr_hof
#' @include utils.R
NULL

# throw an error if f is not a valid lambda expression
validate_lambda <- function(f) {
  if (! "spark_sql_lambda" %in% class(f) && ! "formula" %in% class(f))
    stop("Expected 'f' to be a lambda expression (e.g., 'a %->% (a + 1)' or ",
         "'.(a, b) %->% (a + b + 1)') or a formula (e.g., '~ .x + 1' or ",
         "'~ .x + .y + 1').")
}

translate_formula <- function(f) {
  var_x <- as.name(random_string("x_"))
  var_y <- as.name(random_string("y_"))
  # renaming variables because Spark SQL cannot handle lambda variable name
  # starting with '.'
  f <- do.call(substitute, list(f, list(.x = var_x, .y = var_y)))
  vars <- all.vars(f)
  params_sql <- (
    if (length(vars) > 1)
      paste0("(", paste0(vars, collapse = ", "), ")")
    else
      as.character(vars)
  )
  body_sql <- dbplyr::translate_sql_(list(f[[2]]), con = dbplyr::simulate_dbi())
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

process_expr <- function(x, expr) {
  if (is.null(expr))
    as.name(tail(colnames(x), 1))
  else
    expr
}

process_col <- function(x, col, default_idx) {
  if (is.null(col))
    as.name(colnames(x)[[default_idx]])
  else
    col
}

process_dest_col <- function(expr, dest_col) {
  if (is.null(dest_col))
    expr
  else
    dest_col
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

#' Transform Array Column
#'
#' Apply an element-wise transformation function to an array column
#' (this is essentially a dplyr wrapper for the
#' \code{transform(array<T>, function<T, U>): array<U>} and the
#' \code{transform(array<T>, function<T, Int, U>): array<U>} built-in Spark SQL functions)
#'
#' @param x The Spark data frame to transform
#' @param func The transformation to apply
#' @param expr The array being transformed, could be any SQL expression evaluating to an array
#'  (default: the last column of the Spark data frame)
#' @param dest_col Column to store the transformed result (default: expr)
#' @param ... Additional params to dplyr::mutate
#'
#' @examples
#' \dontrun{
#'
#' library(sparklyr)
#' sc <- spark_connect(master = "local[3]")
#' # applies the (x -> x * x) transformation to elements of all arrays
#' copy_to(sc, tibble::tibble(arr = list(1:5, 21:25))) %>%
#'   hof_transform(~ .x * .x)
#' }
#'
#' @export
hof_transform <- function(
  x,
  func,
  expr = NULL,
  dest_col = NULL,
  ...
) {
  func <- process_lambda(func)
  expr <- process_expr(x, rlang::enexpr(expr))
  dest_col <- process_dest_col(expr, rlang::enexpr(dest_col))

  sql <- paste(
    "TRANSFORM(",
    as.character(dbplyr::translate_sql(!! expr)),
    ",",
    as.character(func),
    ")"
  )

  do.mutate(x, dest_col, sql, ...)
}

#' Filter Array Column
#'
#' Apply an element-wise filtering function to an array column
#' (this is essentially a dplyr wrapper for the
#' \code{filter(array<T>, function<T, Boolean>): array<T>} built-in Spark SQL functions)
#'
#' @param x The Spark data frame to filter
#' @param func The filtering function
#' @param expr The array being filtered, could be any SQL expression evaluating to an array
#'  (default: the last column of the Spark data frame)
#' @param dest_col Column to store the filtered result (default: expr)
#' @param ... Additional params to dplyr::mutate
#'
#' @examples
#' \dontrun{
#'
#' library(sparklyr)
#' sc <- spark_connect(master = "local[3]")
#' # only keep odd elements in each array in `array_column`
#' copy_to(sc, tibble::tibble(array_column = list(1:5, 21:25))) %>%
#'   hof_filter(~ .x %% 2 == 1)
#' }
#'
#' @export
hof_filter <- function(x, func, expr = NULL, dest_col = NULL, ...) {
  func <- process_lambda(func)
  expr <- process_expr(x, rlang::enexpr(expr))
  dest_col <- process_dest_col(expr, rlang::enexpr(dest_col))

  sql <- paste(
    "FILTER(",
    as.character(dbplyr::translate_sql(!! expr)),
    ",",
    as.character(func),
    ")"
  )

  do.mutate(x, dest_col, sql, ...)
}

#' Apply Aggregate Function to Array Column
#'
#' Apply an element-wise aggregation function to an array column
#' (this is essentially a dplyr wrapper for the
#' \code{aggregate(array<T>, A, function<A, T, A>[, function<A, R>]): R}
#' built-in Spark SQL functions)
#'
#' @param x The Spark data frame to run aggregation on
#' @param start The starting value of the aggregation
#' @param merge The aggregation function
#' @param finish Optional param specifying a transformation to apply on the final value of the aggregation
#' @param expr The array being aggregated, could be any SQL expression evaluating to an array
#'  (default: the last column of the Spark data frame)
#' @param dest_col Column to store the aggregated result (default: expr)
#' @param ... Additional params to dplyr::mutate
#'
#' @examples
#' \dontrun{
#'
#' library(sparklyr)
#' sc <- spark_connect(master = "local[3]")
#' # concatenates all numbers of each array in `array_column` and add parentheses
#' # around the resulting string
#' copy_to(sc, tibble::tibble(array_column = list(1:5, 21:25))) %>%
#'   hof_aggregate(
#'     start = "",
#'     merge = ~ CONCAT(.y, .x),
#'     finish = ~ CONCAT("(", .x, ")")
#'   )
#' }
#'
#' @export
hof_aggregate <- function(
  x,
  start,
  merge,
  finish = NULL,
  expr = NULL,
  dest_col = NULL,
  ...
) {
  merge <- process_lambda(merge)
  args <- list(...)
  if (!identical(finish, NULL)) finish <- process_lambda(finish)
  expr <- process_expr(x, rlang::enexpr(expr))
  dest_col <- process_dest_col(expr, rlang::enexpr(dest_col))

  sql <- do.call(paste, as.list(c(
    "AGGREGATE(",
    as.character(dbplyr::translate_sql(!! expr)),
    ",",
    as.character(dbplyr::translate_sql(!! rlang::enexpr(start))),
    ",",
    as.character(merge),
    if (identical(finish, NULL)) NULL else c(",", as.character(finish)),
    ")"
  )))

  do.call(do.mutate, c(list(x, as.character(dest_col), sql), args))
}

#' Determine Whether Some Element Exists in an Array Column
#'
#' Determines whether an element satisfying the given predicate exists in each array from
#' an array column
#' (this is essentially a dplyr wrapper for the
#' \code{exists(array<T>, function<T, Boolean>): Boolean} built-in Spark SQL function)
#'
#' @param x The Spark data frame to search
#' @param pred A boolean predicate
#' @param expr The array being searched (could be any SQL expression evaluating to an array)
#' @param dest_col Column to store the search result
#' @param ... Additional params to dplyr::mutate
#'
#' @export
hof_exists <- function(x, pred, expr = NULL, dest_col = NULL, ...) {
  pred <- process_lambda(pred)
  expr <- process_expr(x, rlang::enexpr(expr))
  dest_col <- process_dest_col(expr, rlang::enexpr(dest_col))

  sql <- paste(
    "EXISTS(",
    as.character(dbplyr::translate_sql(!! expr)),
    ",",
    as.character(pred),
    ")"
  )

  do.mutate(x, dest_col, sql, ...)
}

#' Combines 2 Array Columns
#'
#' Applies an element-wise function to combine elements from 2 array columns
#' (this is essentially a dplyr wrapper for the
#' \code{zip_with(array<T>, array<U>, function<T, U, R>): array<R>}
#' built-in function in Spark SQL)
#'
#' @param x The Spark data frame to process
#' @param func Element-wise combining function to be applied
#' @param dest_col Column to store the query result
#'   (default: the last column of the Spark data frame)
#' @param left Any expression evaluating to an array
#'   (default: the first column of the Spark data frame)
#' @param right Any expression evaluating to an array
#'   (default: the second column of the Spark data frame)
#' @param ... Additional params to dplyr::mutate
#'
#' @examples
#' \dontrun{
#'
#' library(sparklyr)
#' sc <- spark_connect(master = "local[3]")
#' # compute element-wise products of 2 arrays from each row of `left` and `right`
#' # and store the resuling array in `res`
#' copy_to(
#'   sc,
#'   tibble::tibble(
#'     left = list(1:5, 21:25),
#'     right = list(6:10, 16:20),
#'     res = c(0, 0))
#' ) %>%
#'   hof_zip_with(~ .x * .y)
#' }
#'
#' @export
hof_zip_with <- function(
  x,
  func,
  dest_col = NULL,
  left = NULL,
  right = NULL,
  ...
) {
  func <- process_lambda(func)
  dest_col <- process_col(
    x,
    rlang::enexpr(dest_col),
    default_idx = length(colnames(x))
  )
  left <- process_col(x, rlang::enexpr(left), default_idx = 1)
  right <- process_col(x, rlang::enexpr(right), default_idx = 2)

  sql <- paste(
    "ZIP_WITH(",
    as.character(dbplyr::translate_sql(!! left)),
    ",",
    as.character(dbplyr::translate_sql(!! right)),
    ",",
    as.character(func),
    ")"
  )

  do.mutate(x, dest_col, sql, ...)
}
