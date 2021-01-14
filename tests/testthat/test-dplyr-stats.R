context("dplyr stats")

sc <- testthat_spark_connection()

test_that("cor, cov, sd and var works as expected", {
  test_requires("dplyr")

  stats <- data.frame(x = 1:10, y = 10:1)
  stats_tbl <- copy_to(sc, stats, overwrite = TRUE)

  s1 <- stats %>% mutate(
    cor = cor(x, y),
    cov = cov(x, y),
    sd = sd(x),
    var = var(x)
  )

  s2 <- stats_tbl %>%
    mutate(
      cor = cor(x, y),
      cov = cov(x, y),
      sd = sd(x, na.rm = TRUE),
      var = var(x, na.rm = TRUE)
    ) %>%
    collect() %>%
    as.data.frame()

  expect_equal(s1, s2)
})

test_that("cor, cov, sd and var works as expected over groups", {
  test_requires("dplyr")

  stats <- data.frame(id = rep(c(1, 2), 5), x = 1:10, y = 10:1)
  stats_tbl <- copy_to(sc, stats, overwrite = TRUE)

  s1 <- stats %>%
    group_by(id) %>%
    mutate(
      cor = cor(x, y),
      cov = cov(x, y),
      sd = sd(x),
      var = var(x)
    ) %>%
    arrange(id, x, y) %>%
    as.data.frame()

  s2 <- stats_tbl %>%
    group_by(id) %>%
    mutate(
      cor = cor(x, y),
      cov = cov(x, y),
      sd = sd(x, na.rm = TRUE),
      var = var(x, na.rm = TRUE)
    ) %>%
    arrange(id, x, y) %>%
    collect() %>%
    as.data.frame()

  expect_equal(s1, s2)
})

test_that("rowSums works as expected", {
  df <- do.call(
    tibble::tibble,
    lapply(
      seq(6L),
      function(x) {
        column <- list(runif(100))
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

test_that("weighted.mean works as expected", {
  df <- tibble::tibble(
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

  df <- tibble::tibble(
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
    sdf %>% dplyr::summarize(wm = weighted.mean(x ^ 3, w ^ 2)) %>% dplyr::pull(wm),
    df %>%
      dplyr::summarize(
        wm = sum(w ^ 2 * x ^ 3, na.rm = TRUE) /
             sum(w ^ 2 * as.numeric(!is.na(x)), na.rm = TRUE)
      ) %>%
      dplyr::pull(wm)
  )
})

test_that("cumprod works as expected", {
  test_requires_version("2.0.0")
  test_requires("dplyr")
  skip_livy()

  for (stats in list(
                     data.frame(id = 1:10, x = c(1:3, -4, 5, -6, 7, 0, 0, 10)),
                     data.frame(id = 1:10, x = c(1:3, -4, 5, -6, 7, NA, 0, 10)),
                     data.frame(id = 1:10, x = c(1:3, -4, 5, -6, 7, 0, NA, 10))
                    )) {
    stats_tbl <- copy_to(sc, stats, overwrite = TRUE)

    expected <- stats %>%
      arrange(id) %>%
      mutate(
        cumprod = cumprod(x)
      )

    actual <- stats_tbl %>%
      arrange(id) %>%
      mutate(
        cumprod = cumprod(x)
      ) %>%
      collect() %>%
      as.data.frame()

    expect_equal(actual, expected)
  }
})

test_that("count() works in grouped mutate", {
  test_requires("dplyr")
  iris_tbl <- testthat_tbl("iris")

  c1 <- iris_tbl %>%
    group_by(Species) %>%
    mutate(n = count()) %>%
    select(Species, n) %>%
    distinct() %>%
    collect()
  c2 <- iris_tbl %>%
    group_by(Species) %>%
    count() %>%
    collect()

  expect_equal(c1, c2)
})
