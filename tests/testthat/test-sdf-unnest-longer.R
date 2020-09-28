context("sdf-unnest-longer")

sc <- testthat_spark_connection()

test_that("can unnest array column correctly", {
  test_requires_version("2.4.0")

  sdf <- copy_to(sc, tibble::tibble(x = 1:4, y = list(1, 1:2, 1:3, 1:4)))

  expect_equivalent(
    sdf %>% sdf_unnest_longer(y) %>% collect(),
    tibble::tibble(
      x = c(1L, rep(2L, 2), rep(3L, 3), rep(4L, 4)),
      y = c(1, 1:2, 1:3, 1:4)
    )
  )
})

test_that("automatically adds id col if named", {
  test_requires_version("2.4.0")

  sdf <- copy_to(
    sc,
    tibble::tibble(
      x = 1:2,
      y = list(c(a = 1L, b = 2L), c(a = 3L, b = 4L))
    )
  )

  expect_equivalent(
    sdf %>% sdf_unnest_longer(y) %>% collect(),
    tibble::tibble(x = c(1L, 1L, 2L, 2L), y = 1:4, y_id = rep(c("a", "b"), 2))
  )
})

test_that("`include_indices = TRUE` works as expected", {
  test_requires_version("2.4.0")

  sdf <- copy_to(
    sc,
    tibble::tibble(x = 1:3, y = list(1:10, 1:5, 1:2))
  )

  expect_equivalent(
    sdf %>% sdf_unnest_longer(y, include_indices = TRUE) %>% collect(),
    tibble::tibble(
      x = c(rep(1L, 10), rep(2L, 5), rep(3L, 2)),
      y_id = c(1:10, 1:5, 1:2),
      y = c(1:10, 1:5, 1:2)
    )
  )

  sdf <- copy_to(
    sc,
    tibble::tibble(
      x = 1:2,
      y = list(c(a = 1L, b = 2L), c(a = 3L, b = 4L))
    )
  )

  expect_equivalent(
    sdf %>% sdf_unnest_longer(y, include_indices = TRUE) %>% collect(),
    tibble::tibble(x = c(1L, 1L, 2L, 2L), y = 1:4, y_id = rep(c("a", "b"), 2))
  )
})

test_that("`include_indices = FALSE` works as expected", {
  test_requires_version("2.4.0")

  sdf <- copy_to(
    sc,
    tibble::tibble(x = 1:3, y = list(1:10, 1:5, 1:2))
  )

  expect_equivalent(
    sdf %>% sdf_unnest_longer(y, include_indices = FALSE) %>% collect(),
    tibble::tibble(
      x = c(rep(1L, 10), rep(2L, 5), rep(3L, 2)),
      y = c(1:10, 1:5, 1:2)
    )
  )

  sdf <- copy_to(
    sc,
    tibble::tibble(
      x = 1:2,
      y = list(c(a = 1L, b = 2L), c(a = 3L, b = 4L))
    )
  )

  expect_equivalent(
    sdf %>% sdf_unnest_longer(y, include_indices = FALSE) %>% collect(),
    tibble::tibble(x = c(1L, 1L, 2L, 2L), y = 1:4)
  )
})

test_that("preserves empty rows", {
  test_requires_version("2.4.0")

  sdf <- copy_to(sc, tibble::tibble(x = 1:3, y = list(1, NULL, NULL)))
  out <- sdf %>% sdf_unnest_longer(y) %>% collect()

  expect_equal(nrow(out), 3)
})

test_that("bad inputs generate errors", {
  test_requires_version("2.4.0")

  sdf <- copy_to(sc, tibble::tibble(x = 1, y = 1))

  expect_error(
    sdf %>% sdf_unnest_longer(y),
    "`y` must be a struct column or an array column"
  )
})

test_that("`values_to` and `indices_to` works as expected", {
  test_requires_version("2.4.0")

  sdf <- copy_to(
    sc,
    tibble::tibble(x = 1:3, y = list(1:10, 1:5, 1:2))
  )

  expect_equivalent(
    sdf %>%
      sdf_unnest_longer(y, values_to = "y_values", indices_to = "y_indices") %>%
      collect(),
    tibble::tibble(
      x = c(rep(1L, 10), rep(2L, 5), rep(3L, 2)),
      y_indices = c(1:10, 1:5, 1:2),
      y_values = c(1:10, 1:5, 1:2)
    )
  )

  sdf <- copy_to(
    sc,
    tibble::tibble(
      x = 1:2,
      y = list(c(a = 1L, b = 2L), c(a = 3L, b = 4L))
    )
  )

  expect_equivalent(
    sdf %>%
      sdf_unnest_longer(y, values_to = "y_values", indices_to = "y_names") %>%
      collect(),
    tibble::tibble(
      x = c(1L, 1L, 2L, 2L), y_values = 1:4, y_names = rep(c("a", "b"), 2)
    )
  )
})

test_that("`names_repair` works as expected", {
  test_requires_version("2.4.0")

  sdf <- copy_to(
    sc,
    tibble::tibble(y_id = 1:3, y = list(1:10, 1:5, 1:2))
  )

  expect_equivalent(
    sdf %>%
      sdf_unnest_longer(
        y,
        include_indices = TRUE,
        values_to = "y_id",
        names_repair = "universal"
      ) %>%
      collect(),
    tibble::tibble(
      y_id___1 = c(rep(1L, 10), rep(2L, 5), rep(3L, 2)),
      y_id___2 = c(1:10, 1:5, 1:2),
      y_id___3 = c(1:10, 1:5, 1:2)
    )
  )

  sdf <- copy_to(
    sc,
    tibble::tibble(
      y_a = 1:2,
      y_b = 3:4,
      y = list(c(a = 1L, b = 2L), c(a = 3L, b = 4L))
    )
  )

  expect_equivalent(
    sdf %>%
      sdf_unnest_longer(
        y,
        indices_to = "y_a",
        values_to = "y_b",
        names_repair = "universal"
      ) %>%
      collect(),
    tibble::tibble(
      y_a___1 = c(1L, 1L, 2L, 2L),
      y_b___2 = c(3L, 3L, 4L, 4L),
      y_b___3 = 1:4,
      y_a___4 = rep(c("a", "b"), 2)
    )
  )
})

test_that("`ptype` works as expected", {
  test_requires_version("2.4.0")

  sdf <- copy_to(sc, tibble::tibble(x = 1:4, y = list(1, 1:2, 1:3, 1:4)))
  ptype <- tibble::tibble(x = character(), y = numeric())

  expect_equivalent(
    sdf %>% sdf_unnest_longer(y, ptype = ptype) %>% collect(),
    tibble::tibble(
      x = c("1", rep("2", 2), rep("3", 3), rep("4", 4)),
      y = as.numeric(c(1, 1:2, 1:3, 1:4))
    )
  )
})

test_that("`transform` works as expected", {
  test_requires_version("2.4.0")

  sdf <- copy_to(sc, tibble::tibble(x = 1:4, y = list(1, 1:2, 1:3, 1:4)))
  transform <- list(x = as.character, y = as.numeric)

  expect_equivalent(
    sdf %>% sdf_unnest_longer(y, transform = transform) %>% collect(),
    tibble::tibble(
      x = c("1", rep("2", 2), rep("3", 3), rep("4", 4)),
      y = as.numeric(c(1, 1:2, 1:3, 1:4))
    )
  )
})
