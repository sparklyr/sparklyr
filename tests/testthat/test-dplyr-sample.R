skip_connection("dplyr-sample")
test_requires_version("2.0.0")
test_requires("dplyr")
skip_on_livy()

sc <- testthat_spark_connection()



test_that("set.seed makes sampling outcomes deterministic", {
  skip_connection("sample-with-seed")
  sdf <- copy_to(sc, dplyr::tibble(id = seq(1000), weight = rep(seq(5), 200)))

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

test_that("dplyr query is executed before sampling (n)", {
  expect_equal(
    testthat_tbl("mtcars") %>%
      select(hp, mpg) %>%
      sample_n(5) %>%
      collect() %>%
      dim(),
    c(5, 2)
  )
})

test_that("dplyr query is executed before sampling (frac)", {
  skip_connection("sample-frac-exact")
  expect_equal(
    testthat_tbl("mtcars") %>%
      select(hp, mpg) %>%
      sample_frac(0.1) %>%
      collect() %>%
      dim(),
    c(3, 2)
  )
})
