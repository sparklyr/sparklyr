skip_connection("project-template")
skip_on_livy()
skip_on_arrow_devel()

test_that("'project_template' creation succeeds", {
  expect_true(project_template(tempdir()))
})

test_that("'spark_extension' creation succeeds", {
  expect_true(spark_extension(tempdir()))
})
