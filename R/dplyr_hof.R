#' dplyr wrappers for Apache Spark higher order functions
#'
#' These methods implement dplyr grammars for Apache Spark higher order functions
#'
#' @name dplyr_hof
#' @include utils.R
NULL

# throw an error if f is not a valid lambda expression
validate_lambda <- function(f) {
  if (!"spark_sql_lambda" %in% class(f) && !rlang::is_formula(f)) {
    stop(
      "Expected 'f' to be a lambda expression (e.g., 'a %->% (a + 1)' or ",
      "'.(a, b) %->% (a + b + 1)') or a formula (e.g., '~ .x + 1' or ",
      "'~ .x + .y + 1')."
    )
  }
}

translate_formula <- function(f) {
  var_x <- as.name(random_string("x_"))
  var_y <- as.name(random_string("y_"))
  var_z <- as.name(random_string("z_"))
  # renaming variables because Spark SQL cannot handle lambda variable name
  # starting with '.'
  f <- f %>>% substitute %@% list(list(.x = var_x, .y = var_y, .z = var_z))
  vars <- sort(all.vars(f))
  params_sql <- (
    if (length(vars) > 1) {
      paste0("(", paste0(vars, collapse = ", "), ")")
    } else {
      as.character(vars)
    }
  )
  body_sql <- dbplyr::translate_sql(!!f[[2]])
  lambda <- dbplyr::sql(paste(params_sql, "->", body_sql))

  lambda
}

process_lambda <- function(f) {
  validate_lambda(f)
  if ("formula" %in% class(f)) {
    f <- translate_formula(f)
  } else {
    f
  }
}

process_expr <- function(x, expr) {
  if (is.null(expr)) {
    as.name(tail(colnames(x), 1))
  } else {
    expr
  }
}

process_col <- function(x, col, default_idx) {
  if (is.null(col)) {
    as.name(colnames(x)[[default_idx]])
  } else {
    col
  }
}

process_dest_col <- function(expr, dest_col) {
  if (is.null(dest_col)) {
    expr
  } else {
    dest_col
  }
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
#' @param ... Body of the lambda expression, *must be within parentheses*
#'
#' @examples
#' \dontrun{
#'
#' a %->% (mean(a) + 1) # translates to <SQL> `a` -> (AVG(`a`) OVER () + 1.0)
#'
#' .(a, b) %->% (a < 1 && b > 1) # translates to <SQL> `a`,`b` -> (`a` < 1.0 AND `b` > 1.0)
#' }
#' @export
`%->%` <- function(params, ...) {
  `.` <- function(...) {
    rlang::ensyms(...) %>%
      lapply(as.character) %>>%
      paste %@% list(collapse = ", ") %>%
      paste0("(", ., ")")
  }
  process_params <- function(x) {
    if ("call" %in% class(x)) {
      # params is of the form '.(<comma-separated list of variables>)'
      eval(x)
    } else {
      # params consists of a single variable
      x
    }
  }

  params_sql <- rlang::enexpr(params) %>%
    process_params() %>%
    lapply(as.character) %>%
    c(sep = ",") %>%
    do.call(paste, .)

  body_sql <- dbplyr::translate_sql(...)

  lambda <- dbplyr::sql(paste(params_sql, "->", body_sql))
  class(lambda) <- c(class(lambda), "spark_sql_lambda")

  lambda
}

do_mutate <- function(x, dest_col_name, sql, ...) {
  args <- list(dbplyr::sql(sql))
  names(args) <- as.character(dest_col_name)

  x %>>%
    dplyr::mutate %@% c(args, list(...))
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
#' sc <- spark_connect(master = "local")
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
                          ...) {
  func <- process_lambda(func)
  expr <- process_expr(x, rlang::enexpr(expr))
  dest_col <- process_dest_col(expr, rlang::enexpr(dest_col))

  sql <- paste(
    "TRANSFORM(",
    as.character(dbplyr::translate_sql(!!expr)),
    ",",
    as.character(func),
    ")"
  )

  do_mutate(x, dest_col, sql, ...)
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
#' sc <- spark_connect(master = "local")
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
    as.character(dbplyr::translate_sql(!!expr)),
    ",",
    as.character(func),
    ")"
  )

  do_mutate(x, dest_col, sql, ...)
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
#' sc <- spark_connect(master = "local")
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
                          ...) {
  merge <- process_lambda(merge)
  args <- list(...)
  if (!identical(finish, NULL)) finish <- process_lambda(finish)
  expr <- process_expr(x, rlang::enexpr(expr))
  dest_col <- process_dest_col(expr, rlang::enexpr(dest_col))

  sql <- do.call(paste, as.list(c(
    "AGGREGATE(",
    as.character(dbplyr::translate_sql(!!expr)),
    ",",
    as.character(dbplyr::translate_sql(!!rlang::enexpr(start))),
    ",",
    as.character(merge),
    if (identical(finish, NULL)) NULL else c(",", as.character(finish)),
    ")"
  )))

  x %>>%
    do_mutate %@% c(list(as.character(dest_col), sql), args)
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
    as.character(dbplyr::translate_sql(!!expr)),
    ",",
    as.character(pred),
    ")"
  )

  do_mutate(x, dest_col, sql, ...)
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
#' sc <- spark_connect(master = "local")
#' # compute element-wise products of 2 arrays from each row of `left` and `right`
#' # and store the resuling array in `res`
#' copy_to(
#'   sc,
#'   tibble::tibble(
#'     left = list(1:5, 21:25),
#'     right = list(6:10, 16:20),
#'     res = c(0, 0)
#'   )
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
                         ...) {
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
    as.character(dbplyr::translate_sql(!!left)),
    ",",
    as.character(dbplyr::translate_sql(!!right)),
    ",",
    as.character(func),
    ")"
  )

  do_mutate(x, dest_col, sql, ...)
}

#' Sorts array using a custom comparator
#'
#' Applies a custom comparator function to sort an array
#' (this is essentially a dplyr wrapper to the `array_sort(expr, func)` higher-
#' order function, which is supported since Spark 3.0)
#'
#' @param x The Spark data frame to be processed
#' @param func The comparator function to apply (it should take 2 array elements as arguments
#'  and return an integer, with a return value of -1 indicating the first element is less than
#'  the second, 0 indicating equality, or 1 indicating the first element is greater than the
#'  second)
#' @param expr The array being sorted, could be any SQL expression evaluating to an array
#'  (default: the last column of the Spark data frame)
#' @param dest_col Column to store the sorted result (default: expr)
#' @param ... Additional params to dplyr::mutate
#'
#' @examples
#' \dontrun{
#'
#' library(sparklyr)
#' sc <- spark_connect(master = "local", version = "3.0.0")
#' copy_to(
#'   sc,
#'   tibble::tibble(
#'     # x contains 2 arrays each having elements in ascending order
#'     x = list(1:5, 6:10)
#'   )
#' ) %>%
#'   # now each array from x gets sorted in descending order
#'   hof_array_sort(~ as.integer(sign(.y - .x)))
#' }
#'
#' @export
hof_array_sort <- function(
                           x,
                           func,
                           expr = NULL,
                           dest_col = NULL,
                           ...) {
  func <- process_lambda(func)
  expr <- process_expr(x, rlang::enexpr(expr))
  dest_col <- process_dest_col(expr, rlang::enexpr(dest_col))

  sql <- paste(
    "ARRAY_SORT(",
    as.character(dbplyr::translate_sql(!!expr)),
    ",",
    as.character(func),
    ")"
  )

  do_mutate(x, dest_col, sql, ...)
}

#' Filters a map
#'
#' Filters entries in a map using the function specified
#' (this is essentially a dplyr wrapper to the `map_filter(expr, func)` higher-
#' order function, which is supported since Spark 3.0)
#'
#' @param x The Spark data frame to be processed
#' @param func The filter function to apply (it should take (key, value) as arguments
#'  and return a boolean value, with FALSE indicating the key-value pair should be discarded
#'  and TRUE otherwise)
#' @param expr The map being filtered, could be any SQL expression evaluating to a map
#'  (default: the last column of the Spark data frame)
#' @param dest_col Column to store the filtered result (default: expr)
#' @param ... Additional params to dplyr::mutate
#'
#' @examples
#' \dontrun{
#'
#' library(sparklyr)
#' sc <- spark_connect(master = "local", version = "3.0.0")
#' sdf <- sdf_len(sc, 1) %>% dplyr::mutate(m = map(1, 0, 2, 2, 3, -1))
#' filtered_sdf <- sdf %>% hof_map_filter(~ .x > .y)
#' }
#'
#' @export
hof_map_filter <- function(
                           x,
                           func,
                           expr = NULL,
                           dest_col = NULL,
                           ...) {
  func <- process_lambda(func)
  expr <- process_expr(x, rlang::enexpr(expr))
  dest_col <- process_dest_col(expr, rlang::enexpr(dest_col))

  sql <- paste(
    "MAP_FILTER(",
    as.character(dbplyr::translate_sql(!!expr)),
    ",",
    as.character(func),
    ")"
  )

  do_mutate(x, dest_col, sql, ...)
}

#' Checks whether all elements in an array satisfy a predicate
#'
#' Checks whether the predicate specified holds for all elements in an array
#' (this is essentially a dplyr wrapper to the `forall(expr, pred)` higher-
#' order function, which is supported since Spark 3.0)
#'
#' @param x The Spark data frame to be processed
#' @param pred The predicate to test (it should take an array element as argument and
#'   return a boolean value)
#' @param expr The array being tested, could be any SQL expression evaluating to an
#'  array (default: the last column of the Spark data frame)
#' @param dest_col Column to store the boolean result (default: expr)
#' @param ... Additional params to dplyr::mutate
#'
#' @examples
#' \dontrun{
#'
#' sc <- spark_connect(master = "local", version = "3.0.0")
#' df <- tibble::tibble(
#'   x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
#'   y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
#' )
#' sdf <- sdf_copy_to(sc, df, overwrite = TRUE)
#'
#' all_positive_tbl <- sdf %>%
#'   hof_forall(pred = ~ .x > 0, expr = y, dest_col = all_positive) %>%
#'   dplyr::select(all_positive)
#' }
#'
#' @export
hof_forall <- function(
                       x,
                       pred,
                       expr = NULL,
                       dest_col = NULL,
                       ...) {
  pred <- process_lambda(pred)
  expr <- process_expr(x, rlang::enexpr(expr))
  dest_col <- process_dest_col(expr, rlang::enexpr(dest_col))

  sql <- paste(
    "FORALL(",
    as.character(dbplyr::translate_sql(!!expr)),
    ",",
    as.character(pred),
    ")"
  )

  do_mutate(x, dest_col, sql, ...)
}

#' Transforms keys of a map
#'
#' Applies the transformation function specified to all keys of a map
#' (this is essentially a dplyr wrapper to the `transform_keys(expr, func)` higher-
#' order function, which is supported since Spark 3.0)
#'
#' @param x The Spark data frame to be processed
#' @param func The transformation function to apply (it should take (key, value) as
#'  arguments and return a transformed key)
#' @param expr The map being transformed, could be any SQL expression evaluating to a map
#'  (default: the last column of the Spark data frame)
#' @param dest_col Column to store the transformed result (default: expr)
#' @param ... Additional params to dplyr::mutate
#'
#' @examples
#' \dontrun{
#'
#' library(sparklyr)
#' sc <- spark_connect(master = "local", version = "3.0.0")
#' sdf <- sdf_len(sc, 1) %>% dplyr::mutate(m = map("a", 0L, "b", 2L, "c", -1L))
#' transformed_sdf <- sdf %>% hof_transform_keys(~ CONCAT(.x, " == ", .y))
#' }
#'
#' @export
hof_transform_keys <- function(
                               x,
                               func,
                               expr = NULL,
                               dest_col = NULL,
                               ...) {
  func <- process_lambda(func)
  expr <- process_expr(x, rlang::enexpr(expr))
  dest_col <- process_dest_col(expr, rlang::enexpr(dest_col))

  sql <- paste(
    "TRANSFORM_KEYS(",
    as.character(dbplyr::translate_sql(!!expr)),
    ",",
    as.character(func),
    ")"
  )

  do_mutate(x, dest_col, sql, ...)
}

#' Transforms values of a map
#'
#' Applies the transformation function specified to all values of a map
#' (this is essentially a dplyr wrapper to the `transform_values(expr, func)` higher-
#' order function, which is supported since Spark 3.0)
#'
#' @param x The Spark data frame to be processed
#' @param func The transformation function to apply (it should take (key, value) as
#'  arguments and return a transformed value)
#' @param expr The map being transformed, could be any SQL expression evaluating to a map
#'  (default: the last column of the Spark data frame)
#' @param dest_col Column to store the transformed result (default: expr)
#' @param ... Additional params to dplyr::mutate
#'
#' @examples
#' \dontrun{
#'
#' library(sparklyr)
#' sc <- spark_connect(master = "local", version = "3.0.0")
#' sdf <- sdf_len(sc, 1) %>% dplyr::mutate(m = map("a", 0L, "b", 2L, "c", -1L))
#' transformed_sdf <- sdf %>% hof_transform_values(~ CONCAT(.x, " == ", .y))
#' }
#'
#' @export
hof_transform_values <- function(
                                 x,
                                 func,
                                 expr = NULL,
                                 dest_col = NULL,
                                 ...) {
  func <- process_lambda(func)
  expr <- process_expr(x, rlang::enexpr(expr))
  dest_col <- process_dest_col(expr, rlang::enexpr(dest_col))

  sql <- paste(
    "TRANSFORM_VALUES(",
    as.character(dbplyr::translate_sql(!!expr)),
    ",",
    as.character(func),
    ")"
  )

  do_mutate(x, dest_col, sql, ...)
}

#' Merges two maps into one
#'
#' Merges two maps into a single map by applying the function specified to pairs of
#' values with the same key
#' (this is essentially a dplyr wrapper to the `map_zip_with(map1, map2, func)` higher-
#' order function, which is supported since Spark 3.0)
#'
#' @param x The Spark data frame to be processed
#' @param func The function to apply (it should take (key, value1, value2) as arguments,
#'   where (key, value1) is a key-value pair present in map1, (key, value2) is a key-value
#'   pair present in map2, and return a transformed value associated with key in the
#'   resulting map
#' @param dest_col Column to store the query result
#'   (default: the last column of the Spark data frame)
#' @param map1 The first map being merged, could be any SQL expression evaluating to a
#'  map (default: the first column of the Spark data frame)
#' @param map2 The second map being merged, could be any SQL expression evaluating to a
#'  map (default: the second column of the Spark data frame)
#' @param ... Additional params to dplyr::mutate
#'
#' @examples
#' \dontrun{
#'
#' library(sparklyr)
#' sc <- spark_connect(master = "local", version = "3.0.0")
#'
#' # create a Spark dataframe with 2 columns of type MAP<STRING, INT>
#' two_maps_tbl <- sdf_copy_to(
#'   sc,
#'   tibble::tibble(
#'     m1 = c("{\"1\":2,\"3\":4,\"5\":6}", "{\"2\":1,\"4\":3,\"6\":5}"),
#'     m2 = c("{\"1\":1,\"3\":3,\"5\":5}", "{\"2\":2,\"4\":4,\"6\":6}")
#'   ),
#'   overwrite = TRUE
#' ) %>%
#'   dplyr::mutate(m1 = from_json(m1, "MAP<STRING, INT>"),
#'                 m2 = from_json(m2, "MAP<STRING, INT>"))
#'
#' # create a 3rd column containing MAP<STRING, INT> values derived from the
#' # first 2 columns
#'
#' transformed_two_maps_tbl <- two_maps_tbl %>%
#'   hof_map_zip_with(
#'     func = .(k, v1, v2) %->% (CONCAT(k, "_", v1, "_", v2)),
#'     dest_col = m3
#'   )
#' }
#'
#' @export
hof_map_zip_with <- function(
                             x,
                             func,
                             dest_col = NULL,
                             map1 = NULL,
                             map2 = NULL,
                             ...) {
  func <- process_lambda(func)
  dest_col <- process_col(
    x,
    rlang::enexpr(dest_col),
    default_idx = length(colnames(x))
  )
  map1 <- process_col(x, rlang::enexpr(map1), default_idx = 1)
  map2 <- process_col(x, rlang::enexpr(map2), default_idx = 2)

  sql <- paste(
    "MAP_ZIP_WITH(",
    as.character(dbplyr::translate_sql(!!map1)),
    ",",
    as.character(dbplyr::translate_sql(!!map2)),
    ",",
    as.character(func),
    ")"
  )

  do_mutate(x, dest_col, sql, ...)
}
