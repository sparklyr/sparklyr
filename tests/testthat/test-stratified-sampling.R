context("stratified-sampling")

test_requires("dplyr")

sample_space_sz <- 100L
num_zeroes <- 10L
num_groups <- 5L

sampling_test_data <- data.frame(
  id = rep(seq(sample_space_sz + num_zeroes), num_groups),
  group = seq(num_groups) %>%
    lapply(function(x) rep(x, sample_space_sz + num_zeroes)) %>%
    unlist(),
  weight = rep(
    c(
      rep(1, 50),
      rep(2, 25),
      rep(4, 10),
      rep(8, 10),
      rep(16, 5),
      rep(0, num_zeroes)
    ),
    num_groups
  )
)
sdf <- testthat_tbl(
  name = "sampling_test_data",
  repartition = 5L
)

sample_sz <- 20L
sample_frac <- 0.12
num_sampling_iters <- 50L
alpha <- 0.05

verify_sample_n_results <- function(weighted, replacement) {
  expected_dist <- rep(0L, sample_space_sz + num_zeroes)
  actual_dist <- rep(0L, sample_space_sz + num_zeroes)

  for (x in seq(num_sampling_iters)) {
    set.seed(142857L + x)

    args <- list(
      size = sample_sz,
      weight = if (weighted) as.symbol("weight") else NULL,
      replace = replacement
    )
    sample <- sampling_test_data %>%
      dplyr::group_by(group) %>>%
      dplyr::sample_n %@% args
    for (id in sample$id) {
      expected_dist[[id]] <- expected_dist[[id]] + 1L
    }

    sample <- sdf %>%
      dplyr::group_by(group) %>>%
      dplyr::sample_n %@% args %>%
      collect()
    for (id in sample$id) {
      actual_dist[[id]] <- actual_dist[[id]] + 1L
    }

    expect_equal(
      sample %>%
        dplyr::group_by(group) %>%
        dplyr::summarize(n = n()) %>%
        dplyr::pull(n),
      rep(sample_sz, num_groups)
    )
  }

  res <- ks.test(x = actual_dist, y = expected_dist)

  expect_gte(res$p.value, alpha)
}

verify_sample_frac_results <- function(weighted, replacement) {
  expected_dist <- rep(0L, sample_space_sz + num_zeroes)
  actual_dist <- rep(0L, sample_space_sz + num_zeroes)

  for (x in seq(num_sampling_iters)) {
    set.seed(142857L + x)

    args <- list(
      size = sample_frac,
      weight = if (weighted) as.symbol("weight") else NULL,
      replace = replacement
    )
    sample <- sampling_test_data %>%
      dplyr::group_by(group) %>>%
      dplyr::sample_frac %@% args
    for (id in sample$id) {
      expected_dist[[id]] <- expected_dist[[id]] + 1L
    }

    sample <- sdf %>%
      dplyr::group_by(group) %>>%
      dplyr::sample_frac %@% args %>%
      collect()
    for (id in sample$id) {
      actual_dist[[id]] <- actual_dist[[id]] + 1L
    }

    expect_equal(
      sample %>%
        dplyr::group_by(group) %>%
        dplyr::summarize(n = n()) %>%
        dplyr::pull(n),
      rep(ceiling((sample_space_sz + num_zeroes) * sample_frac), num_groups)
    )
  }
  res <- ks.test(x = actual_dist, y = expected_dist)

  expect_gte(res$p.value, alpha)
}

test_that("stratified sampling without replacement works as expected", {
  test_requires_version("3.0.0")

  verify_sample_n_results(weighted = FALSE, replacement = FALSE)
  verify_sample_frac_results(weighted = FALSE, replacement = FALSE)
})

test_that("stratified sampling with replacement works as expected", {
  test_requires_version("3.0.0")

  verify_sample_n_results(weighted = FALSE, replacement = TRUE)
  verify_sample_frac_results(weighted = FALSE, replacement = TRUE)
})

test_that("stratified weighted sampling without replacement works as expected", {
  test_requires_version("3.0.0")

  verify_sample_n_results(weighted = TRUE, replacement = FALSE)
  verify_sample_frac_results(weighted = TRUE, replacement = FALSE)
})

test_that("stratified weighted sampling with replacement works as expected", {
  test_requires_version("3.0.0")

  verify_sample_n_results(weighted = TRUE, replacement = TRUE)
  verify_sample_frac_results(weighted = TRUE, replacement = TRUE)
})

test_that("stratified sampling returns repeatable results from a fixed PRNG seed", {
  test_requires_version("3.0.0")

  for (replacement in c(TRUE, FALSE)) {
    for (weighted in c(TRUE, FALSE)) {
      args <- list(
        size = sample_sz,
        weight = if (weighted) as.symbol("weight") else NULL,
        replace = replacement
      )
      samples <- lapply(
        seq(2),
        function(x) {
          set.seed(142857L)
          sdf %>%
            dplyr::group_by(group) %>>%
            dplyr::sample_n %@% args %>%
            collect()
        }
      )

      expect_equivalent(
        samples[[1]] %>% dplyr::arrange(id),
        samples[[2]] %>% dplyr::arrange(id)
      )

      args <- list(
        size = sample_frac,
        weight = if (weighted) as.symbol("weight") else NULL,
        replace = replacement
      )
      samples <- lapply(
        seq(2),
        function(x) {
          set.seed(142857L)
          sdf %>%
            dplyr::group_by(group) %>>%
            dplyr::sample_frac %@% args %>%
            collect()
        }
      )

      expect_equivalent(
        samples[[1]] %>% dplyr::arrange(id),
        samples[[2]] %>% dplyr::arrange(id)
      )
    }
  }
})
