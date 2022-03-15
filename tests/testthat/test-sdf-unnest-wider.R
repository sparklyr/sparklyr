
sc <- testthat_spark_connection()

sdf <- copy_to(
  sc,
  tibble::tibble(
    x = 1:3,
    y = list(list(a = 1, b = 2), list(a = 3, b = 4), list(a = 5, b = 6))
  )
)

test_that("can unnest a struct column", {
  test_requires_version("2.4.0")

  expect_equivalent(
    sdf %>% sdf_unnest_wider(y) %>% collect(),
    tibble::tibble(x = 1:3, a = c(1, 3, 5), b = c(2, 4, 6))
  )
})

test_that("bad input generates error", {
  test_requires_version("2.4.0")

  expect_error(sdf %>% sdf_unnest_wider(x), "`x` must be a struct column")
})

test_that("`names_sep` works as expected", {
  test_requires_version("2.4.0")

  expect_equivalent(
    sdf %>% sdf_unnest_wider(y, names_sep = "_") %>% collect(),
    tibble::tibble(x = 1:3, y_a = c(1, 3, 5), y_b = c(2, 4, 6))
  )
})

test_that("`names_repair` works as expected", {
  test_requires_version("2.4.0")

  expect_equivalent(
    sdf %>%
      dplyr::rename(y_a = x) %>%
      sdf_unnest_wider(y, names_sep = "_", names_repair = "universal") %>%
      collect(),
    tibble::tibble(y_a___1 = 1:3, y_a___2 = c(1, 3, 5), y_b = c(2, 4, 6))
  )
})

test_that("`ptype` works as expected", {
  test_requires_version("2.4.0")

  ptype <- data.frame(x = numeric(), a = character(), b = integer())

  expect_equivalent(
    sdf %>% sdf_unnest_wider(y, ptype = ptype) %>% collect(),
    tibble::tibble(x = c(1, 2, 3), a = c("1", "3", "5"), b = c(2L, 4L, 6L))
  )
})

test_that("`transform` works as expected", {
  test_requires_version("2.4.0")

  transform <- list(x = as.character, a = as.integer)

  expect_equivalent(
    sdf %>% sdf_unnest_wider(y, transform = transform) %>% collect(),
    tibble::tibble(x = c("1", "2", "3"), a = c(1L, 3L, 5L), b = c(2, 4, 6))
  )
})
