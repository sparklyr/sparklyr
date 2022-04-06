
lazy_select_query <- function(from,
                              last_op,
                              select = NULL,
                              where = NULL,
                              group_by = NULL,
                              order_by = NULL,
                              limit = NULL,
                              distinct = FALSE,
                              group_vars = NULL,
                              order_vars = NULL,
                              frame = NULL,
                              select_operation = c("mutate", "summarise"),
                              message_summarise = NULL) {
  stopifnot(inherits(from, "lazy_query"))
  stopifnot(is_string(last_op))
  stopifnot(is.null(select) || is_lazy_sql_part(select))
  stopifnot(is_lazy_sql_part(where))
  # stopifnot(is.character(group_by))
  stopifnot(is_lazy_sql_part(order_by))
  stopifnot(is.null(limit) || (is.numeric(limit) && length(limit) == 1L))
  stopifnot(is.logical(distinct), length(distinct) == 1L)

  # stopifnot(is.null(group_vars) || (is.character(group_vars) && is.null(names(group_vars))))
  stopifnot(is_lazy_sql_part(order_vars), is.null(names(order_vars)))
  stopifnot(is.null(frame) || is_integerish(frame, n = 2, finite = TRUE))

  select <- select %||% syms(set_names(op_vars(from)))
  select_operation <- arg_match0(select_operation, c("mutate", "summarise"))

  stopifnot(is.null(message_summarise) || is_string(message_summarise))

  # inherit `group_vars`, `order_vars`, and `frame` from `from`
  group_vars <- group_vars %||% op_grps(from)
  order_vars <- order_vars %||% op_sort(from)
  frame <- frame %||% op_frame(from)

  if (last_op == "mutate") {
    select <- new_lazy_select(
      select,
      group_vars = group_vars,
      order_vars = order_vars,
      frame = frame
    )
  } else {
    select <- new_lazy_select(select)
  }

  structure(
    list(
      from = from,
      select = select,
      where = where,
      group_by = group_by,
      order_by = order_by,
      distinct = distinct,
      limit = limit,
      group_vars = group_vars,
      order_vars = order_vars,
      frame = frame,
      select_operation = select_operation,
      last_op = last_op,
      message_summarise = message_summarise
    ),
    class = c("lazy_select_query", "lazy_query")
  )
}

is_lazy_sql_part <- function(x) {
  if (is.null(x)) return(TRUE)
  if (is_quosures(x)) return(TRUE)

  if (!is.list(x)) return(FALSE)
  purrr::every(x, ~ is_quosure(.x) || is_symbol(.x) || is_expression(.x))
}

new_lazy_select <- function(vars, group_vars = NULL, order_vars = NULL, frame = NULL) {
  vctrs::vec_as_names(names2(vars), repair = "check_unique")

  var_names <- names(vars)
  vars <- unname(vars)

  tibble(
    name = var_names %||% character(),
    expr = vars %||% list(),
    group_vars = rep_along(vars, list(group_vars)),
    order_vars = rep_along(vars, list(order_vars)),
    frame = rep_along(vars, list(frame))
  )
}

update_lazy_select <- function(select, vars) {
  vctrs::vec_as_names(names(vars), repair = "check_unique")

  sel_vars <- purrr::map_chr(vars, as_string)
  idx <- vctrs::vec_match(sel_vars, select$name)
  select <- vctrs::vec_slice(select, idx)
  select$name <- names(vars)
  select
}

add_select <- function(.data, vars, op = c("select", "mutate")) {
  op <- match.arg(op, c("select", "mutate"))
  lazy_query <- .data$lazy_query

  # drop NULLs
  vars <- purrr::discard(vars, ~ is_quosure(.x) && quo_is_null(.x))
  if (selects_same_variables(.data, vars)) {
    return(lazy_query)
  }

  if (length(lazy_query$last_op) == 1 && lazy_query$last_op %in% c("select", "mutate")) {
    # Special optimisation when applied to pure projection() - this is
    # conservative and we could expand to any op_select() if combined with
    # the logic in nest_vars()
    select <- lazy_query$select

    if (purrr::every(vars, is.symbol)) {
      # if current operation is pure projection
      # we can just subset the previous selection
      sel_vars <- purrr::map_chr(vars, as_string)
      lazy_query$select <- update_lazy_select(select, sel_vars)

      return(lazy_query)
    }

    prev_vars <- select$expr
    if (purrr::every(prev_vars, is.symbol)) {
      # if previous operation is pure projection
      sel_vars <- purrr::map_chr(prev_vars, as_string)
      if (all(select$name == sel_vars)) {
        # and there's no renaming
        # we can just ignore the previous step
        if (op == "select") {
          lazy_query$select <- update_lazy_select(select, vars)
        } else {
          lazy_query$select <- new_lazy_select(
            vars,
            group_vars = op_grps(lazy_query),
            order_vars = op_sort(lazy_query),
            frame = op_frame(lazy_query)
          )
        }
        return(lazy_query)
      }
    }
  }

  lazy_select_query(
    from = lazy_query,
    last_op = op,
    select = vars
  )
}
