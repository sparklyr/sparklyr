context("sdf-weighted-sampling-ext")

test_requires("dplyr")

weighted_sampling_octal_test_data <- data.frame(
  x = rep(seq(0, 7), 100),
  weight = 1 + (seq(800) * 7 + 11) %% 17
)
sdf <- testthat_tbl(
  name = "weighted_sampling_octal_test_data",
  repartition = 4L
)

num_sampling_iters <- 1000
alpha <- 0.05

sample_sz <- 3

# map each possible outcome to an octal value
to_oct <- function(sample) {
  sum(8^seq(0, sample_sz - 1) * sample$x)
}

max_possible_outcome <- to_oct(list(x = rep(7, sample_sz)))

verify_distribution <- function(replacement) {
  expected_dist <- rep(0, max_possible_outcome + 1)
  actual_dist <- rep(0, max_possible_outcome + 1)

  for (x in seq(num_sampling_iters)) {
    seed <- x * 97
    set.seed(seed)
    sample <- weighted_sampling_octal_test_data %>%
      dplyr::slice_sample(
        n = sample_sz,
        weight_by = weight,
        replace = replacement
      ) %>%
      to_oct()
    expected_dist[[sample + 1]] <- expected_dist[[sample + 1]] + 1

    sample <- sdf %>%
      sdf_weighted_sample(
        k = sample_sz,
        weight_col = "weight",
        replacement = replacement,
        seed = seed
      ) %>%
      collect() %>%
      to_oct()
    actual_dist[[sample + 1]] <- actual_dist[[sample + 1]] + 1
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
