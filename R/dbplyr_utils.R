#' @include utils.R
NULL

# NOTE: everything here is entirely copy-pasted from dbplyr.
# Reason for doing so is `sparklyr` needs to override some other
# implementation detail of `dplyr::mutate` so that it can correctly handle
# the `mutate(across(where(...), ...))` type of usage for Spark dataframes

nest_vars <- function(.data, dots, all_vars) {
  new_vars <- character()
  init <- 0L
  for (i in seq_along(dots)) {
    cur_var <- names(dots)[[i]]
    used_vars <- all_names(rlang::get_expr(dots[[i]]))

    if (any(used_vars %in% new_vars)) {
      new_actions <- dots[rlang::seq2(init, length(dots))][new_vars]
      .data$ops <- op_select(.data$ops, carry_over(union(all_vars, used_vars), new_actions))
      all_vars <- c(all_vars, setdiff(new_vars, all_vars))
      new_vars <- cur_var
      init <- i
    } else {
      new_vars <- c(new_vars, cur_var)
    }
  }

  if (init != 0L) {
    dots <- dots[-rlang::seq2(1L, init - 1)]
  }
  .data$ops <- op_select(.data$ops, carry_over(all_vars, dots))
  .data
}

op_select <- function(x, vars) {
  if (inherits(x, "op_select")) {
    prev_vars <- x$args$vars
    if (purrr::every(vars, is.symbol)) {
      sel_vars <- purrr::map_chr(vars, rlang::as_string)
      vars <- rlang::set_names(prev_vars[sel_vars], names(sel_vars))
      x <- x$x
    }
    else if (purrr::every(prev_vars, is.symbol)) {
      sel_vars <- purrr::map_chr(prev_vars, rlang::as_string)
      if (all(names(sel_vars) == sel_vars)) {
        x <- x$x
      }
    }
  }
  new_op_select(x, vars)
}

new_op_select <- function(x, vars) {
  stopifnot(inherits(x, "op"))
  stopifnot(is.list(vars))
  dbplyr::op_single("select", x, dots = list(), args = list(vars = vars))
}

carry_over <- function(sel = character(), act = list()) {
  if (is.null(names(sel))) {
    names(sel) <- sel
  }
  sel <- rlang::syms(sel)

  # Keep last of duplicated acts
  act <- act[!duplicated(names(act), fromLast = TRUE)]

  # Preserve order of sel
  both <- intersect(names(sel), names(act))
  sel[both] <- act[both]

  # Adding new variables at end
  new <- setdiff(names(act), names(sel))

  c(sel, act[new])
}

all_names <- function(x) {
  if (is.name(x)) {
    return(as.character(x))
  }
  if (rlang::is_quosure(x)) {
    return(all_names(rlang::quo_get_expr(x)))
  }
  if (!is.call(x)) {
    return(NULL)
  }

  unique(unlist(lapply(x[-1], all_names), use.names = FALSE))
}
