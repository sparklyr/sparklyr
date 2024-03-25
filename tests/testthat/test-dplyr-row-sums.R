skip_connection("dplyr-row-sums")
skip_on_livy()

sc <- testthat_spark_connection()

test_that("rowSums works as expected", {
  df <- do.call(
    dplyr::tibble,
    lapply(
      seq(6L),
      function(x) {
        column <- list(runif(10))
        names(column) <- paste0("col", x)
        column
      }
    ) %>%
      unlist(recursive = FALSE)
  ) %>%
    dplyr::mutate(na = NA_real_)
  sdf <- copy_to(sc, df, overwrite = TRUE)
  expect_row_sums_eq <- function(x, na.rm) {
    expected <- df %>% dplyr::mutate(row_sum = rowSums(.[x], na.rm = na.rm))

    expect_equivalent(
      expected,
      sdf %>%
        dplyr::mutate(row_sum = rowSums(.[x], na.rm = na.rm)) %>%
        collect()
    )
    expect_equivalent(
      expected,
      sdf %>%
        dplyr::mutate(row_sum = rowSums(sdf[x], na.rm = na.rm)) %>%
        collect()
    )
  }
  test_cases <- list(
    4L, 2:4, 4:2, -3L, -2:-4, c(2L, 4L),
    "col5", c("col1", "col3", "col6"),
    "na", c("col1", "na"), c("col1", "na", "col3", "col6"),
    NULL
  )

  for (x in test_cases) {
    for (na.rm in c(FALSE, TRUE)) {
      expect_row_sums_eq(x, na.rm = na.rm)
    }
  }
})

test_clear_cache()
