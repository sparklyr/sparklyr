expect_warning_on_arrow <- function(x) {
  if(using_arrow()) {
    expect_warning(x)
  } else {
    x
  }
}

expect_coef_equal <- function(lhs, rhs) {
  nm <- names(lhs)
  lhs <- lhs[nm]
  rhs <- rhs[nm]

  expect_true(all.equal(lhs, rhs, tolerance = 0.01, scale = 1))
}

# helper functions for testing from broom
# test the basics of tidy/augment/glance output: is a data frame, no row names
check_tidiness <- function(o) {
  expect_is(o, "data.frame")
  expect_equal(rownames(o), as.character(seq_len(nrow(o))))
}

# check the output of a tidy function
check_tidy <- function(o, exp.row = NULL, exp.col = NULL, exp.names = NULL) {
  check_tidiness(o)

  if (!is.null(exp.row)) {
    expect_equal(nrow(o), exp.row)
  }
  if (!is.null(exp.col)) {
    expect_equal(ncol(o), exp.col)
  }
  if (!is.null(exp.names)) {
    expect_true(all(exp.names %in% colnames(o)))
  }
}

check_params <- function(test_args, params) {
  purrr::iwalk(
    test_args,
    ~ expect_equal(params[[.y]], .x, info = .y)
  )
}

test_param_setting <- function(sc, fn, test_args, is_ml_pipeline = TRUE) {
  collapse_sublists <- function(x) purrr::map_if(x, rlang::is_bare_list, unlist)

  params1 <- do.call(fn, c(list(x = sc), test_args)) %>%
    ml_params() %>%
    collapse_sublists()

  expected <- collapse_sublists(test_args)
  check_params(expected, params1)

  if (is_ml_pipeline) {
    params2 <- do.call(fn, c(list(x = ml_pipeline(sc)), test_args)) %>%
      ml_stage(1) %>%
      ml_params() %>%
      collapse_sublists()
    check_params(expected, params2)
  }
}

test_default_args <- function(sc, fn) {
  default_args <- rlang::fn_fmls(fn) %>%
    as.list() %>%
    purrr::discard(~ is.symbol(.x) || is.language(.x))

  default_args$Uid <- NULL # rlang::modify is deprecated
  default_args <- purrr::compact(default_args)

  params <- do.call(fn, list(x = sc)) %>%
    ml_params()
  check_params(default_args, params)
}

expect_same_remote_result <- function(.data, pipeline) {
  temp_name <- random_table_name("test_")
  spark_data <- copy_to(sc, .data, temp_name)

  local <- pipeline(.data)


  remote <- try(
    spark_data %>%
      pipeline() %>%
      collect()
  )

  if(inherits(remote, "try-error")) {
    expect_equal(remote[[1]], "")
  } else {
    expect_equivalent(local, remote)
  }

  DBI::dbRemoveTable(sc, temp_name)
}
