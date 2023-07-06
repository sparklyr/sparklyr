skip_connection("dplyr-sample-frac")
test_requires_version("2.0.0")
skip_on_livy()

sc <- testthat_spark_connection()

iris_tbl <- testthat_tbl("iris")

test_that("'sample_frac' works as expected", {
  s_exact <- spark_integ_test_skip(sc, "sample-frac-exact")
  s_replace <- spark_integ_test_skip(sc,  "sample-frac-replace")
  s_weights <- spark_integ_test_skip(sc, "sample-frac-weights")

  for (weight in list(NULL, rlang::sym("Petal_Length"))) {
    for (replace in list(FALSE, TRUE)) {
      skip_test <- FALSE
      if(replace && s_replace) skip_test <- TRUE
      if(!is.null(weight) && s_weights) skip_test <- TRUE
      if(!skip_test) {
        sample_sdf <- iris_tbl %>%
          sample_frac(0.2, weight = !!weight, replace = replace)

        expect_equal(colnames(sample_sdf), colnames(iris_tbl))

        sample_sdf <- iris_tbl %>%
          select(Petal_Length) %>%
          sample_frac(0.2, weight = !!weight, replace = replace)

        expect_equal(colnames(sample_sdf), "Petal_Length")

        if(!s_exact && !is.null(weight)) {
          expect_equal(sample_sdf %>% collect() %>% nrow(), round(0.2 * nrow(iris)))
          expect_equal(sample_sdf %>% collect() %>% nrow(), round(0.2 * nrow(iris)))
        }
      }
    }
  }
})
