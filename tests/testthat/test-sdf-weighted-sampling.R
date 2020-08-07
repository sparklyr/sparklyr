context("sdf-weighted-sampling")

test_requires("dplyr")

sample_space_sz <- 100
num_zeroes <- 50

weighted_sampling_test_data <- data.frame(
  id = seq(sample_space_sz + num_zeroes),
  weight = c(
    rep(1, 50),
    rep(2, 25),
    rep(4, 10),
    rep(8, 10),
    rep(16, 5),
    rep(0, num_zeroes)
  )
)
sdf <- testthat_tbl(
  name = "weighted_sampling_test_data",
  repartition = 5L
)

sample_sz <- 20
num_sampling_iters <- 100
alpha <- 0.05

verify_distribution <- function(replacement) {
  expected_dist <- rep(0, sample_space_sz)
  actual_dist <- rep(0, sample_space_sz)

  seed <- 142857L
  set.seed(seed)

  for (x in seq(num_sampling_iters)) {
    sample <- weighted_sampling_test_data %>%
      dplyr::slice_sample(
        n = sample_sz,
        weight_by = weight,
        replace = replacement
      )
    for (id in sample$id) {
      expected_dist[[id]] <- expected_dist[[id]] + 1
    }

    sample <- sdf %>%
      sdf_weighted_sample(
        k = sample_sz,
        weight_col = "weight",
        replacement = replacement,
        seed = seed + x
      ) %>%
      collect()
    for (id in sample$id) {
      actual_dist[[id]] <- actual_dist[[id]] + 1
    }
  }

  res <- ks.test(x = actual_dist, y = expected_dist)

  expect_gte(res$p.value, alpha)
}

test_that("sdf_weighted_sample without replacement works as expected", {
  verify_distribution(replacement = FALSE)
})

test_that("sdf_weighted_sample with replacement works as expected", {
  verify_distribution(replacement = TRUE)
})

test_that("sdf_weighted_sample returns repeatable results from a fixed PRNG seed", {
  seed <- 142857
  for (replacement in c(TRUE, FALSE)) {
    samples <- lapply(
      seq(2),
      function(x) {
        sdf %>%
          sdf_weighted_sample(
            weight_col = "weight",
            k = sample_sz,
            replacement = replacement,
            seed = seed
          ) %>%
          collect()
      }
    )

    expect_equivalent(
      samples[[1]] %>% dplyr::arrange(id),
      samples[[2]] %>% dplyr::arrange(id)
    )
  }
})
