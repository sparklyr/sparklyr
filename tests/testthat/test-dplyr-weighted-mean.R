skip_connection("dplyr-weighted-mean")

sc <- testthat_spark_connection()

test_that("weighted.mean works as expected", {
  df <- dplyr::tibble(
    x = c(NA_real_, 3.1, 2.2, NA_real_, 3.3, 4),
    w = c(NA_real_, 1, 0.5, 1, 0.75, NA_real_)
  )
  sdf <- copy_to(sc, df, overwrite = TRUE)

  expect_equal(
    sdf %>% dplyr::summarize(wm = weighted.mean(x, w)) %>% dplyr::pull(wm),
    df %>%
      dplyr::summarize(
        wm = sum(w * x, na.rm = TRUE) /
          sum(w * as.numeric(!is.na(x)), na.rm = TRUE)
      ) %>%
      dplyr::pull(wm)
  )

  df <- dplyr::tibble(
    x = rep(c(NA_real_, 3.1, 2.2, NA_real_, 3.3, 4), 3L),
    w = rep(c(NA_real_, 1, 0.5, 1, 0.75, NA_real_), 3L),
    grp = c(rep(1L, 6L), rep(2L, 6L), rep(3L, 6L))
  )
  sdf <- copy_to(sc, df, overwrite = TRUE)

  expect_equal(
    sdf %>% dplyr::summarize(wm = weighted.mean(x, w)) %>% dplyr::pull(wm),
    df %>%
      dplyr::summarize(
        wm = sum(w * x, na.rm = TRUE) /
          sum(w * as.numeric(!is.na(x)), na.rm = TRUE)
      ) %>%
      dplyr::pull(wm)
  )
  expect_equal(
    sdf %>% dplyr::summarize(wm = weighted.mean(x^3, w^2)) %>% dplyr::pull(wm),
    df %>%
      dplyr::summarize(
        wm = sum(w^2 * x^3, na.rm = TRUE) /
          sum(w^2 * as.numeric(!is.na(x)), na.rm = TRUE)
      ) %>%
      dplyr::pull(wm)
  )
})
