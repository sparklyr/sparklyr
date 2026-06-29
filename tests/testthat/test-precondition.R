skip_connection("precondition")
skip_on_livy()
skip_on_arrow_devel()

test_that("ensure_not_na errors on NA (scalar and vector) and passes otherwise", {
  expect_error(ensure_not_na(NA), "is NA")
  expect_error(ensure_not_na(c(1, NA)), "contains NA")
  expect_equal(ensure_not_na(1:3), 1:3)
})

test_that("ensure_not_null errors on NULL and passes otherwise", {
  expect_error(ensure_not_null(NULL), "is NULL")
  expect_equal(ensure_not_null(5), 5)
})

test_that("require_file_exists / require_directory_exists enforce paths", {
  f <- tempfile()
  file.create(f)
  expect_equal(require_file_exists(f), f)
  expect_error(require_file_exists("/no/such/path"), "no file at path")

  d <- tempfile()
  dir.create(d)
  expect_equal(require_directory_exists(d), d)
  expect_error(require_directory_exists(f), "no file at path") # f is a file
})

test_that("ensure_directory returns, creates, and rejects non-directories", {
  d <- tempfile()
  dir.create(d)
  expect_equal(ensure_directory(d), d) # already a directory

  d2 <- file.path(tempfile(), "nested")
  ensure_directory(d2)
  expect_true(dir.exists(d2)) # created recursively

  f <- tempfile()
  file.create(f)
  expect_error(ensure_directory(f), "not a directory")
})

test_that("param_name_r_to_spark camel-cases and honors a catalog override", {
  expect_equal(param_name_r_to_spark("max_iter"), "MaxIter")
  expect_equal(param_name_r_to_spark("x", list(x = "CustomName")), "CustomName")
})

test_that("params_validate casts matched params and can fail on unmatched", {
  est <- structure(list(), class = "ml_estimator")
  v <- params_validate(est, list(max_iter = 5, reg_param = 0.1))
  expect_identical(v$max_iter, 5L)
  expect_identical(v$reg_param, 0.1)
  # NULL values pass through untouched
  expect_null(params_validate(est, list(weight_col = NULL))$weight_col)
  expect_error(
    params_validate(est, list(not_a_param = 1), unmatched_fail = TRUE),
    "Could not find"
  )
  # pre_ml_estimator dispatches to the same base validator; exercise a choice
  # validator (family) too
  v2 <- params_validate(
    structure(list(), class = "pre_ml_estimator"),
    list(family = "gaussian")
  )
  expect_equal(v2$family, "gaussian")
})

test_that("stopf and warnf format their messages", {
  expect_error(stopf("bad %s", "value"), "bad value")
  expect_warning(warnf("careful %d", 7L), "careful 7")
})

test_clear_cache()
