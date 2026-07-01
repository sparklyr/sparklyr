skip_connection("tidiers_utils")
skip_on_livy()
skip_on_arrow_devel()
test_requires("dplyr")

sc <- testthat_spark_connection()

# pure helpers ----------------------------------------------------------------

test_that("tidy/augment/glance.ml_model stubs error for unsupported models", {
  x <- structure(list(), class = c("ml_model_made_up", "ml_model"))
  expect_error(tidy(x), "'tidy\\(\\)' not yet supported for")
  expect_error(augment(x), "'augment\\(\\)' not yet supported for")
  expect_error(glance(x), "'glance\\(\\)' not yet supported for")
})

test_that("unrowname strips row names", {
  df <- data.frame(a = 1:2)
  rownames(df) <- c("foo", "bar")
  out <- unrowname(df)
  expect_equal(rownames(out), c("1", "2"))
})

test_that("fix_data_frame keeps default row names in place", {
  df <- data.frame(a = 1:3, b = 4:6)
  out <- fix_data_frame(df, newnames = c("x", "y"))
  expect_s3_class(out, "tbl_df")
  expect_equal(colnames(out), c("x", "y"))
  expect_equal(nrow(out), 3)
})

test_that("fix_data_frame moves non-default row names into a term column", {
  m <- matrix(1:4, nrow = 2, dimnames = list(c("aa", "bb"), c("c1", "c2")))
  out <- fix_data_frame(m)
  expect_equal(colnames(out)[1], "term")
  expect_equal(out$term, c("aa", "bb"))

  # newnames rename the non-term columns
  out2 <- fix_data_frame(m, newnames = c("p", "q"))
  expect_equal(colnames(out2), c("term", "p", "q"))
})

test_that("fix_data_frame rejects a mismatched newnames length", {
  df <- data.frame(a = 1:3, b = 4:6)
  expect_error(
    fix_data_frame(df, newnames = c("only_one")),
    "newnames must be NULL or have length equal to number of columns"
  )
})

test_that("check_newdata flags a `newdata` argument", {
  expect_error(check_newdata(newdata = 1), "new_data")
  expect_silent(check_newdata(new_data = 1))
})

# live helpers (fitted model) -------------------------------------------------

test_that("broom_augment_supervised and extract_model_metrics run on a model", {
  test_requires("dplyr")
  mtcars_tbl <- testthat_tbl("mtcars")
  model <- ml_linear_regression(mtcars_tbl, mpg ~ wt + cyl)

  # augment() -> broom_augment_supervised(): adds a .prediction column, reusing
  # the training set when newdata is omitted
  aug <- broom_augment_supervised(model)
  expect_true(".prediction" %in% colnames(aug))

  # glance() routes through extract_model_metrics() to assemble a 1-row summary
  gl <- glance(model)
  expect_s3_class(gl, "data.frame")
  expect_equal(nrow(gl), 1)
})

test_that("broom_augment_supervised renames the classification predicted_label", {
  test_requires("dplyr")
  iris_tbl <- testthat_tbl("iris")
  # a classifier's predictions carry a `predicted_label` column, exercising the
  # `predicted_label` -> `.predicted_label` rename branch
  clf <- ml_decision_tree_classifier(
    iris_tbl,
    Species ~ Sepal_Length + Sepal_Width
  )
  aug <- broom_augment_supervised(clf)
  expect_true(".predicted_label" %in% colnames(aug))
})

test_clear_cache()
