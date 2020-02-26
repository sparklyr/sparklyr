context("ml feature - elementwise product")

test_that("ft_elementwise_product() param setting", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_args <- list(
    input_col = "foo",
    output_col = "bar",
    scaling_vec = 1:5
  )
  test_param_setting(sc, ft_elementwise_product, test_args)
})

test_that("ft_elementwise_product() works", {
  sc <- testthat_spark_connection()
  test_requires_version('2.0.0', "elementwise product requires spark 2.0+")
  df <- data.frame(a = 1, b = 3, c = 5)
  df_tbl <- copy_to(sc, df, overwrite = TRUE)

  nums <- df_tbl %>%
    ft_vector_assembler(list("a", "b", "c"), output_col = "features") %>%
    ft_elementwise_product("features", "multiplied", c(2, 4, 6)) %>%
    pull(multiplied) %>%
    rlang::flatten_dbl()

  expect_identical(nums,
                   c(1, 3, 5) * c(2, 4, 6))

  ewp <- ft_elementwise_product(
    sc, "features", "multiplied", scaling_vec = c(1, 3, 5))

  expect_equal(
    ml_params(ewp, list(
      "input_col", "output_col", "scaling_vec"
    )),
    list(input_col = "features",
         output_col = "multiplied",
         scaling_vec = c(1, 3, 5))
  )
})
