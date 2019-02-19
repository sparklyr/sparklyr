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
  fitted <- ml_predict(model)

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
    ft_regex_tokenizer("text", "tokens")

  separated <- sdf_separate_column(
    tokens,
    "tokens",
    list(first = 1L)
  )

  all <- sdf_read_column(separated, "tokens")
  first <- sdf_read_column(separated, "first")

  expect_equal(as.list(first), lapply(all, `[[`, 1))

})

test_that("we can separate struct columns (#690)", {
  test_requires_version("2.0.0", "hive window support")
  date_seq <- seq.Date(
    from = as.Date("2013-01-01"),
    to = as.Date("2017-01-01"),
    by = "1 day")

  date_sdf <- copy_to(sc, tibble::data_frame(event_date = as.character(date_seq)),
                      overwrite = TRUE)

  sliding_window_sdf <- date_sdf %>%
    dplyr::mutate(sw = window(event_date, "150 days", "30 days"))
  split1 <- sliding_window_sdf %>%
    sdf_separate_column("sw")
  split2 <- sliding_window_sdf %>%
    sdf_separate_column("sw", c("a", "b"))
  split3 <- sliding_window_sdf %>%
    sdf_separate_column("sw", list("c" = 2))

  expect_identical(
    colnames(split1),
    c("event_date", "sw", "start", "end")
  )
  expect_identical(
    split1 %>%
      pull(start) %>%
      head(1) %>%
      as.Date(),
    as.Date("2012-08-18 UTC")
  )
  expect_identical(
    split2 %>%
      pull(b) %>%
      head(1) %>%
      as.Date(),
    as.Date("2013-01-15 UTC")
  )
  expect_identical(
    split3 %>%
      pull(c) %>%
      head(1) %>%
      as.Date(),
    as.Date("2013-01-15 UTC")
  )
})
