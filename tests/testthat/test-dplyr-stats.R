context("dplyr stats")
sc <- testthat_spark_connection()

test_that("cor, cov, sd and var works as expected", {
  test_requires("dplyr")

  stats <- data.frame(x=1:10, y=10:1)
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
      sd = sd(x),
      var = var(x)
    ) %>%
    collect() %>%
    as.data.frame()

  expect_equal(s1, s2)
})

test_that("cor, cov, sd and var works as expected over groups", {
  test_requires("dplyr")

  stats <- data.frame(id=rep(c(1,2), 5), x=1:10, y=10:1)
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
      sd = sd(x),
      var = var(x)
    ) %>%
    arrange(id, x, y) %>%
    collect() %>%
    as.data.frame()

  expect_equal(s1, s2)
})

test_that("cumprod works as expected", {
  test_requires("dplyr")

  stats <- data.frame(id = 1:10,
                      x = c(1:3, -4, 5, -6, 7, 0, 0, 10))
  stats_tbl <- copy_to(sc, stats, overwrite = TRUE)

  s1 <- stats %>%
    arrange(id) %>%
    mutate(
      cumprod = cumprod(x)
    )

  s2 <- stats_tbl %>%
    arrange(id) %>%
    mutate(
      cumprod = cumprod(x)
    ) %>%
    collect() %>%
    as.data.frame()

  expect_equal(s1, s2)
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
