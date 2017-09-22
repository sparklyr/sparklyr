context("r formula")

sc <- testthat_spark_connection()

iris_tbl <- testthat_tbl("iris")

test_that("r formula works as expected", {
  pipeline <- ml_pipeline(sc) %>%
    ft_string_indexer("Species", "species_idx") %>%
    ft_one_hot_encoder("species_idx", "species_dummy") %>%
    ft_vector_assembler(list("Petal_Width", "species_dummy"), "features")

  df1 <- pipeline %>%
    ml_fit_and_transform(iris_tbl) %>%
    select(features, label = Sepal_Length) %>%
    collect()

  df2 <- iris_tbl %>%
    ft_r_formula("Sepal_Length ~ Petal_Width + Species") %>%
    select(features, label) %>%
    collect()

  expect_equal(pull(df1, features), pull(df2, features))
  expect_equal(pull(df1, label), pull(df2, label))
})
