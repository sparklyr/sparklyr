skip_connection("stratified-sampling")
skip_on_livy()
skip_on_arrow_devel()

test_requires("dplyr")

sample_space_sz <- 100L
num_zeroes <- 50L
num_groups <- 5L

sample_sz <- 20L
sample_frac <- 0.10

num_sampling_iters <- 10L
alpha <- 0.05

## ------------------------ Data prep ------------------------------------------

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

## ---------------------- Sampling testing function ----------------------------

verify_sample_results <- function(weighted, replacement, sampling) {

  expected_dist <- rep(0L, sample_space_sz + num_zeroes)

  actual_dist <- rep(0L, sample_space_sz + num_zeroes)

  if(weighted) {
    weight <- expr(weight)
  } else {
    weight <- NULL
  }

  for (x in seq(num_sampling_iters)) {
    set.seed(142857L + x)

    if(sampling == "fraction") {
      r_sample <- sampling_test_data %>%
        group_by(group) %>%
        sample_frac(
          weight = !! weight,
          replace = !! replacement,
          size = sample_frac
          )

      spark_sample <- sdf %>%
        group_by(group) %>%
        sample_frac(
          weight = !! weight,
          replace = !! replacement,
          size = sample_frac
        ) %>%
        collect()
    }

    if(sampling == "number") {
      r_sample <- sampling_test_data %>%
        group_by(group) %>%
        sample_n(
          weight = !! weight,
          replace = !! replacement,
          size = sample_sz
        )

      spark_sample <- sdf %>%
        group_by(group) %>%
        sample_n(
          weight = !! weight,
          replace = !! replacement,
          size = sample_sz
        ) %>%
        collect()
    }

    spark_count <- spark_sample %>%
      count(group) %>%
      pull(n)

    if(sampling == "fraction") {
      expect_equal(
        spark_count,
        rep(ceiling((sample_space_sz + num_zeroes) * sample_frac), num_groups)
      )}

    if(sampling == "number") {
      expect_equal(
        spark_count,
        rep(sample_sz, num_groups)
      )}

    for (id in r_sample$id) {
      expected_dist[[id]] <- expected_dist[[id]] + 1L
    }

    for (id in spark_sample$id) {
      actual_dist[[id]] <- actual_dist[[id]] + 1L
    }
  }

  expect_warning(
    res <- ks.test(x = actual_dist, y = expected_dist)
  )

  expect_gte(res$p.value, alpha)


}

test_that("stratified sampling without replacement works as expected", {
  test_requires_version("3.0.0")

  verify_sample_results(weighted = FALSE, replacement = FALSE, sampling = "number")
  verify_sample_results(weighted = FALSE, replacement = FALSE, sampling = "fraction")
})

test_that("stratified sampling with replacement works as expected", {
  test_requires_version("3.0.0")

  verify_sample_results(weighted = FALSE, replacement = TRUE, sampling = "number")
  verify_sample_results(weighted = FALSE, replacement = TRUE, sampling = "fraction")
})

test_that("stratified weighted sampling without replacement works as expected", {
  test_requires_version("3.0.0")

  verify_sample_results(weighted = TRUE, replacement = FALSE, sampling = "number")
  verify_sample_results(weighted = TRUE, replacement = FALSE, sampling = "fraction")
})

test_that("stratified weighted sampling with replacement works as expected", {
  test_requires_version("3.0.0")

  verify_sample_results(weighted = TRUE, replacement = TRUE, sampling = "number")
  verify_sample_results(weighted = TRUE, replacement = TRUE, sampling = "fraction")
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
