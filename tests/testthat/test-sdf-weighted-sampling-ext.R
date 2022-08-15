skip_on_livy()
skip_on_arrow_devel()

test_requires("dplyr")

weighted_sampling_octal_test_data <- data.frame(
  x = rep(seq(0L, 7L), 100L),
  weight = 1L + (seq(800L) * 7L + 11L) %% 17L
)

sdf <- testthat_tbl(
  name = "weighted_sampling_octal_test_data",
  repartition = 4L
)

num_sampling_iters <- 100L
alpha <- 0.05

sample_sz <- 3L

# map each possible outcome to an octal value
to_oct <- function(sample) {
  sum(8L^seq(0L, sample_sz - 1L) * sample$x)
}

max_possible_outcome <- to_oct(list(x = rep(7, sample_sz)))

verify_distribution <- function(replacement) {
  expected_dist <- rep(0L, max_possible_outcome + 1L)
  actual_dist <- rep(0L, max_possible_outcome + 1L)

  for (x in seq(num_sampling_iters)) {
    seed <- x * 97L
    set.seed(seed)

    sample <- weighted_sampling_octal_test_data %>%
      dplyr::slice_sample(
        n = sample_sz,
        weight_by = weight,
        replace = replacement
      ) %>%
      to_oct()
    expected_dist[[sample + 1L]] <- expected_dist[[sample + 1L]] + 1L

    sample <- sdf %>%
      sdf_weighted_sample(
        k = sample_sz,
        weight_col = "weight",
        replacement = replacement,
        seed = seed
      ) %>%
      collect() %>%
      to_oct()
    actual_dist[[sample + 1L]] <- actual_dist[[sample + 1L]] + 1L
  }

  expect_warning(
    res <- ks.test(x = actual_dist, y = expected_dist)
  )

  expect_gte(res$p.value, alpha)
}

test_that("sdf_weighted_sample without replacement works as expected", {
  verify_distribution(replacement = FALSE)
})

test_that("sdf_weighted_sample with replacement works as expected", {
  verify_distribution(replacement = TRUE)
})
