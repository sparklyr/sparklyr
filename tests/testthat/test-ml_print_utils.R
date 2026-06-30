skip_connection("ml_print_utils")
skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
test_that("input_cols print correctly", {
  sc <- testthat_spark_connection()
  expect_output_file(
    print(ft_vector_assembler(sc, c("foo", "bar"), "features", uid = "va")),
    output_file("print/vector-assembler.txt")
  )
})

test_that("ml_print helpers render estimators, model summaries, and transformers", {
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")

  # estimator -> class + (column) params
  expect_true(any(grepl("Parameters", capture.output(print(ml_kmeans(sc, k = 2))))))

  # regression model summary -> residuals + detailed coefficients
  m <- iris_tbl %>%
    ml_linear_regression(Sepal_Length ~ Petal_Length + Petal_Width)
  out <- capture.output(summary(m))
  expect_true(any(grepl("Residuals|Coefficients", out)))

  # transformer info section
  tm <- ml_fit(
    ft_standard_scaler(sc, "f", "s"),
    iris_tbl %>% ft_vector_assembler("Petal_Length", "f")
  )
  expect_gt(length(capture.output(print(tm))), 0)
})

test_clear_cache()
