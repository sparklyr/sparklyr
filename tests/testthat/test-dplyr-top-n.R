skip_connection("dplyr-top-n")
skip_on_livy()
sc <- testthat_spark_connection()

test_that("slice_max works as expected", {
  # skip("skip while dbplyr/#330 is investigated")

  test_requires_version("2.0.0", "bug in spark-csv")
  test_requires("dplyr")

  test_data <- c()
  for (i in seq_along(LETTERS))
  {
    test_data <- c(test_data, rep.int(LETTERS[i], times = i * 10))
  }

  test_data <- tibble("X" = test_data, stringsAsFactors = F)
  test_tbl <- copy_to(sc, test_data)

  tn1 <- test_tbl %>%
    count(X) %>%
    slice_max(n = 10, order_by = n) %>%
    collect()
  tn2 <- test_data %>%
    count(X) %>%
    slice_max(n = 10, order_by = n)

  tn2 <- tn2 %>%
    mutate(n = as.integer(n)) %>%
    arrange(X)
  tn1 <- tn1 %>%
    mutate(n = as.integer(n)) %>%
    arrange(X)

  expect_equal(tn1, tn2)
})

test_clear_cache()
