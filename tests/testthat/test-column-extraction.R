context("column separation")

test_requires("dplyr")
test_requires("janeaustenr")

sc <- testthat_spark_connection()

mtcars_tbl <- testthat_tbl("mtcars")

austen     <- austen_books()
austen_tbl <- testthat_tbl("austen")

test_that("we can interact with vector columns", {

  # fit a (nonsensical) logistic regression model
  model <- ml_logistic_regression(
    mtcars_tbl,
    response = "vs",
    features = "mpg"
  )

  # get predicted values
  fitted <- sdf_predict(model)

  # extract first element from 'probability' vector
  extracted <- sdf_separate_column(
    fitted,
    "probability",
    list("on" = 1)
  )

  # retrieve the columns
  probability <- extracted %>%
    sdf_read_column("probability") %>%
    lapply(function(el) {
      invoke(el, "toArray")
    })

  # split into pieces
  first <- as.numeric(lapply(probability, `[[`, 1L))
  second <- as.numeric(lapply(probability, `[[`, 2L))

  # verify we have the expected result
  expect_equal(first, sdf_read_column(extracted, "on"))

  # now, try generating for each element
  splat <- sdf_separate_column(
    fitted,
    "probability",
    c("on", "off")
  )

  # verify they're equal
  expect_equal(first, sdf_read_column(splat, "on"))
  expect_equal(second, sdf_read_column(splat, "off"))

})

test_that("we can separate array<string> columns", {

  tokens <- austen_tbl %>%
    na.omit() %>%
    filter(length(text) > 0) %>%
    head(5) %>%
    sdf_mutate(tokens = ft_regex_tokenizer(text, pattern = "\\s+"))

  separated <- sdf_separate_column(
    tokens,
    "tokens",
    list(first = 1L)
  )

  all <- sdf_read_column(separated, "tokens")
  first <- sdf_read_column(separated, "first")

  expect_equal(as.list(first), lapply(all, `[[`, 1))

})
