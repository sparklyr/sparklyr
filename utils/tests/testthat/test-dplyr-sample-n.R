skip_connection("dplyr-sample-n")
test_requires_version("2.0.0")
test_requires("dplyr")
skip_on_livy()

sc <- testthat_spark_connection()

iris_tbl <- testthat_tbl("iris")

test_that("'sample_n' works as expected", {

  s_replace <- spark_integ_test_skip(sc,  "sample-n-replace")
  s_weights <- spark_integ_test_skip(sc, "sample-n-weights")
  for (weight in list(NULL, rlang::sym("Petal_Length"))) {
    for (replace in list(FALSE, TRUE)) {
      skip_test <- FALSE
      if(replace && s_replace) skip_test <- TRUE
      if(!is.null(weight) && s_weights) skip_test <- TRUE
      if(!skip_test) {
        sample_sdf <- iris_tbl %>%
          sample_n(10, weight = !!weight, replace = replace)
        expect_equal(colnames(sample_sdf), colnames(iris_tbl))
        expect_equal(sample_sdf %>% collect() %>% nrow(), 10)

        sample_sdf <- iris_tbl %>%
          select(Petal_Length) %>%
          sample_n(10, weight = !!weight, replace = replace)
        expect_equal(colnames(sample_sdf), "Petal_Length")
        expect_equal(sample_sdf %>% collect() %>% nrow(), 10)
      }
    }
  }
})

test_that("weighted sampling works as expected with integer weight columns", {
  skip_connection("sample-n-weights")
  sdf <- copy_to(sc, tibble::tibble(id = seq(100), weight = seq(100)))
  for (replace in list(FALSE, TRUE)) {
    sample_sdf <- sdf %>%
      sample_n(20, weight = weight, replace = replace)
    expect_equal(colnames(sample_sdf), colnames(sdf))
    expect_equal(sample_sdf %>% collect() %>% nrow(), 20)

    sample_sdf <- sdf %>%
      sample_frac(0.2, weight = weight, replace = replace)
    expect_equal(colnames(sample_sdf), colnames(sdf))
    expect_equal(sample_sdf %>% collect() %>% nrow(), 20)
  }
})
