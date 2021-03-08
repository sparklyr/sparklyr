# Everything below has been mostly copy-pasted from
# https://github.com/tidyverse/dbplyr/blob/master/R/partial-eval.R
# except for minor changes to `partial_eval_across()` and other related
# functions to make use cases such as
# `summarise(across(where(is.numeric), mean))` work as expected for Spark
# dataframes

partial_eval <- function(call, sim_data, env = rlang::caller_env()) {
  vars <- colnames(sim_data)
  if (rlang::is_null(call)) {
    NULL
  } else if (rlang::is_atomic(call) || blob::is_blob(call)) {
    call
  } else if (rlang::is_symbol(call)) {
    partial_eval_sym(call, vars, env)
  } else if (rlang::is_quosure(call)) {
    partial_eval(rlang::get_expr(call), sim_data, rlang::get_env(call))
  } else if (rlang::is_call(call)) {
    partial_eval_call(call, sim_data, env)
  } else {
    rlang::abort(glue::glue("Unknown input type: ", typeof(call)))
  }
}

partial_eval_dots <- function(dots, sim_data) {
  stopifnot(inherits(dots, "quosures"))

  dots <- lapply(dots, partial_eval_quo, sim_data)

  # Flatten across() calls
  is_list <- vapply(dots, is.list, logical(1))
  dots[!is_list] <- lapply(dots[!is_list], list)
  names(dots)[is_list] <- ""

  unlist(dots, recursive = FALSE)
}

partial_eval_quo <- function(x, sim_data) {
  expr <- partial_eval(rlang::get_expr(x), sim_data, rlang::get_env(x))
  if (is.list(expr)) {
    lapply(expr, rlang::new_quosure, env = rlang::get_env(x))
  } else {
    rlang::new_quosure(expr, rlang::get_env(x))
  }
}

partial_eval_sym <- function(sym, vars, env) {
  name <- rlang::as_string(sym)
  if (name %in% vars) {
    sym
  } else if (rlang::env_has(env, name, inherit = TRUE)) {
    rlang::eval_bare(sym, env)
  } else {
    sym
  }
}

is_namespaced_dplyr_call <- function(call) {
  rlang::is_symbol(call[[1]], "::") && rlang::is_symbol(call[[2]], "dplyr")
}

is_tidy_pronoun <- function(call) {
  rlang::is_symbol(call[[1]], c("$", "[[")) && rlang::is_symbol(call[[2]], c(".data", ".env"))
}

partial_eval_call <- function(call, sim_data, env) {
  fun <- call[[1]]
  vars <- colnames(sim_data)

  # Try to find the name of inlined functions
  if (inherits(fun, "inline_colwise_function")) {
    dot_var <- vars[[attr(call, "position")]]
    call <- replace_dot(attr(fun, "formula")[[2]], dot_var)
    env <- rlang::get_env(attr(fun, "formula"))
  } else if (is.function(fun)) {
    fun_name <- find_fun(fun)
    if (is.null(fun_name)) {
      # This probably won't work, but it seems like it's worth a shot.
      return(rlang::eval_bare(call, env))
    }

    call[[1]] <- fun <- rlang::sym(fun_name)
  }

  # So are compound calls, EXCEPT dplyr::foo()
  if (is.call(fun)) {
    if (is_namespaced_dplyr_call(fun)) {
      call[[1]] <- fun[[3]]
    } else if (is_tidy_pronoun(fun)) {
      stop("Use local() or remote() to force evaluation of functions", call. = FALSE)
    } else {
      return(rlang::eval_bare(call, env))
    }
  }

  # .data$, .data[[]], .env$, .env[[]] need special handling
  if (is_tidy_pronoun(call)) {
    if (rlang::is_symbol(call[[1]], "$")) {
      idx <- call[[3]]
    } else {
      idx <- as.name(rlang::eval_bare(call[[3]], env))
    }

    if (rlang::is_symbol(call[[2]], ".data")) {
      idx
    } else {
      rlang::eval_bare(idx, env)
    }
  } else if (rlang::is_call(call, "across")) {
    partial_eval_across(call, sim_data, env)
  } else if (rlang::is_call(call, "if_all")) {
    partial_eval_if_all(call, sim_data, env)
  } else if (rlang::is_call(call, "if_any")) {
    partial_eval_if_any(call, sim_data, env)
  } else {
    # Process call arguments recursively, unless user has manually called
    # remote/local
    name <- rlang::as_string(call[[1]])
    if (name == "local") {
      rlang::eval_bare(call[[2]], env)
    } else if (name == "remote") {
      call[[2]]
    } else {
      call[-1] <- lapply(call[-1], partial_eval, sim_data = sim_data, env = env)
      call
    }
  }
}

utils::globalVariables(c("array_contains", "concat", "element_at"))
partial_eval_across <- function(call, sim_data, env) {
  call <- match.call(dplyr::across, call, expand.dots = FALSE, envir = env)
  vars <- colnames(sim_data)

  cols <- rlang::syms(vars)[tidyselect::eval_select(call$.cols, sim_data, allow_rename = TRUE)]

  .fns <- eval(call$.fns, env)

  if (rlang::is_formula(.fns)) {
    # as is
    funs <- .fns
  } else {
    if (is.function(.fns)) {
      .fns <- find_fun(.fns)
    } else if (is.list(.fns)) {
      .fns <- purrr::map_chr(.fns, find_fun)
    } else if (is.character(.fns)) {
      # as is
    } else {
      rlang::abort("Unsupported `.fns` for dplyr::across()")
    }
    funs <- rlang::set_names(rlang::syms(.fns), .fns)
  }

  # Generate grid of expressions
  if (rlang::is_formula(funs)) {
    if (length(call$...) > 0) {
      rlang::abort("Formula with additional parameters is unsupported")
    }
    out <- vector("list", length(cols))
    for (i in seq_along(cols)) {
      out[[i]] <- rlang::expr(element_at(transform(array(!!cols[[i]]), !!funs), 1L))
    }
  } else {
    out <- vector("list", length(cols) * length(.fns))
    k <- 1
    for (i in seq_along(cols)) {
      for (j in seq_along(funs)) {
        out[[k]] <- rlang::expr((!!funs[[j]])(!!cols[[i]], !!!call$...))
        k <- k + 1
      }
    }
  }

  .names <- eval(call$.names, env)
  names(out) <- across_names(
    cols,
    if (rlang::is_formula(funs)) {
      "formula"
    } else {
      names(funs)
    },
    .names,
    env
  )

  out
}

partial_eval_if_all <- function(call, sim_data, env) {
  expr <- partial_eval_apply_fns(call, sim_data, env, "dplyr::if_all()")

  rlang::expr(!array_contains(!!expr, FALSE))
}

partial_eval_if_any <- function(call, sim_data, env) {
  expr <- partial_eval_apply_fns(call, sim_data, env, "dplyr::if_any()")

  rlang::expr(array_contains(!!expr, TRUE))
}

partial_eval_apply_fns <- function(call, sim_data, env, what) {
  signature_fn <- function(.cols, .fns = NULL, ..., .names = NULL) {}
  call <- match.call(signature_fn, call, expand.dots = FALSE, envir = env)
  if (!is.null(call$.names)) {
    stop(".names is unsupported for Spark dataframes.")
  }
  vars <- colnames(sim_data)
  cols <- rlang::syms(vars)[tidyselect::eval_select(call$.cols, sim_data, allow_rename = TRUE)]
  fns <- c(eval(call$.fns, env))
  if (is.null(fns)) {
    rlang::expr(array(!!!cols))
  } else {
    sub_exprs <- vector("list", length(fns))
    for (i in seq_along(fns)) {
      sub_exprs[[i]] <- (
        if (is.null(fns[[i]])) {
          rlang::expr(array(!!!cols))
        } else {
          fn <- (
            if (rlang::is_formula(fns[[i]])) {
              fns[[i]]
            } else if (rlang::is_function(fns[[i]])) {
              rlang::expr(~ (!!rlang::sym(find_fun(fns[[i]])))(.x, !!!call$...))
            } else {
              rlang::abort(sprintf("Unsupported `.fns` for %s", what))
            }
          )

          rlang::expr(transform(array(!!!cols), !!fn))
        }
      )
    }

    rlang::expr(concat(!!!sub_exprs))
  }
}

process_fn <- function(fn, what) {
  if (rlang::is_function(call$.fns)) {
  } else if (rlang::is_formula(call$.fns)) {
  } else {
    rlang::abort("Unsupported `.fns` for dbplyr::across()")
  }
}

across_names <- function(cols, funs, names = NULL, env = parent.frame()) {
  if (length(funs) == 1) {
    names <- names %||% "{.col}"
  } else {
    names <- names %||% "{.col}_{.fn}"
  }

  glue_env <- rlang::child_env(env,
    .col = rep(cols, each = length(funs)),
    .fn = rep(funs %||% seq_along(funs), length(cols))
  )
  glue::glue(names, .envir = glue_env)
}

find_fun <- function(fun) {
  if (rlang::is_lambda(fun)) {
    body <- body(fun)
    if (!rlang::is_call(body)) {
      return(NULL)
    }

    fun_name <- body[[1]]
    if (!rlang::is_symbol(fun_name)) {
      return(NULL)
    }

    as.character(fun_name)
  } else if (is.function(fun)) {
    fun_name(fun)
  }
}

fun_name <- function(fun) {
  pkg_env <- rlang::env_parent(rlang::global_env())
  sparklyr_fns <- spark_sql_translation(dbplyr::simulate_dbi())
  known <- c(
    ls(dbplyr::base_agg),
    ls(dbplyr::base_scalar),
    ls(sparklyr_fns$aggregate),
    ls(sparklyr_fns$scalar)
  )

  for (x in known) {
    if (!rlang::env_has(pkg_env, x, inherit = TRUE)) {
      next
    }

    fun_x <- rlang::env_get(pkg_env, x, inherit = TRUE)
    if (identical(fun, fun_x)) {
      return(x)
    }
  }

  NULL
}

replace_dot <- function(call, var) {
  if (rlang::is_symbol(call, ".")) {
    rlang::sym(var)
  } else if (rlang::is_call(call)) {
    call[] <- lapply(call, replace_dot, var = var)
    call
  } else {
    call
  }
}
