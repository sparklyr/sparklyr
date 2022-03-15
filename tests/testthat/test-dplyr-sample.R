skip_on_livy()
skip_on_arrow_devel()

sc <- testthat_spark_connection()

test_requires("dplyr")

test_that("set.seed makes sampling outcomes deterministic", {
  test_requires_version("2.0.0")
  skip_livy()
  test_requires("dplyr")

  sdf <- copy_to(sc, tibble::tibble(id = seq(1000), weight = rep(seq(5), 200)))

  for (weight in list(NULL, rlang::sym("weight"))) {
    for (replace in list(FALSE, TRUE)) {
      outcomes <- lapply(
        seq(2),
        function(i) {
          set.seed(142857L)
          sdf %>%
            sample_n(200, weight = weight, replace = replace) %>%
            collect()
        }
      )

      expect_equivalent(outcomes[[1]], outcomes[[2]])

      outcomes <- lapply(
        seq(2),
        function(i) {
          set.seed(142857L)
          sdf %>%
            sample_frac(0.2, weight = weight, replace = replace) %>%
            collect()
        }
      )

      expect_equivalent(outcomes[[1]], outcomes[[2]])
    }
  }
})
