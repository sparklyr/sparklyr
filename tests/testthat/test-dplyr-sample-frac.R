
sc <- testthat_spark_connection()

iris_tbl <- testthat_tbl("iris")
test_requires("dplyr")

test_that("'sample_frac' works as expected", {
  test_requires_version("2.0.0")
  skip_livy()
  test_requires("dplyr")

  for (weight in list(NULL, rlang::sym("Petal_Length"))) {
    for (replace in list(FALSE, TRUE)) {
      sample_sdf <- iris_tbl %>%
        sample_frac(0.2, weight = !!weight, replace = replace)
      expect_equal(colnames(sample_sdf), colnames(iris_tbl))
      expect_equal(sample_sdf %>% collect() %>% nrow(), round(0.2 * nrow(iris)))

      sample_sdf <- iris_tbl %>%
        select(Petal_Length) %>%
        sample_frac(0.2, weight = !!weight, replace = replace)
      expect_equal(colnames(sample_sdf), "Petal_Length")
      expect_equal(sample_sdf %>% collect() %>% nrow(), round(0.2 * nrow(iris)))
    }
  }
})
