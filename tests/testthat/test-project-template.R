context("project template")

test_that("'project_template' creation succeeds", {
  expect_true(project_template(tempdir()))
})
