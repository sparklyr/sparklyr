skip_connection("ml_feature_vectors")
skip_on_livy()
skip_on_arrow_devel()

test_that("ft_pca() param setting", {
  skip_databricks_connect()
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    input_col = "foo",
    output_col = "bar",
    k = 4
  )
  test_param_setting(sc, ft_pca, test_args)
})

test_that("ft_pca() works", {
  skip_databricks_connect()
  sc <- testthat_spark_connection()
  mat <- dplyr::tibble(
    V1 = c(0, 2, 4),
    V2 = c(1, 0, 0),
    V3 = c(0, 3, 0),
    V4 = c(7, 4, 6),
    V5 = c(0, 5, 7)
  )

  # PC3 is in the null space of this rank-2 (3-row) dataset, so its sign and
  # magnitude are arbitrary and BLAS-dependent; only assert on PC1/PC2.
  s <- dplyr::tibble(
    PC1 = c(1.6485728230883807, -4.645104331781534, -6.428880535676489),
    PC2 = c(-4.013282700516296, -1.1167972663619026, -5.337951427775355)
  )
  mat_tbl <- testthat_tbl("mat")

  r <- mat_tbl %>%
    ft_vector_assembler(paste0("V", 1:5), "v") %>%
    ft_pca("v", "pc", k = 2) %>%
    sdf_separate_column("pc", into = paste0("PC", 1:2)) %>%
    select(starts_with("PC", ignore.case = FALSE)) %>%
    collect()

  expect_equal(s, r, tolerance = 1)
})

# Backwards compat

test_that("ml_pca() agrees with Scala result", {
  skip_databricks_connect()
  # import org.apache.spark.ml.feature.PCA
  # import org.apache.spark.ml.linalg.Vectors
  #
  # val data = Array(
  #   Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
  #   Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
  #   Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
  # )
  # val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")
  # val pca = new PCA()
  # .setInputCol("features")
  # .setOutputCol("pcaFeatures")
  # .setK(3)
  # .fit(df)
  # val pcaDF = pca.transform(df)
  # val result = pcaDF.select("pcaFeatures")
  # result.collect()
  #
  # res1: Array[org.apache.spark.sql.Row] =
  #   Array([[1.6485728230883807,-4.013282700516296,-5.524543751369388]],
  #         [[-4.645104331781534,-1.1167972663619026,-5.524543751369387]],
  #         [[-6.428880535676489,-5.337951427775355,-5.524543751369389]])

  # PC3 is in the null space of this rank-2 (3-row) dataset, so its sign and
  # magnitude are arbitrary and BLAS-dependent; only assert on PC1/PC2.
  s <- data.frame(
    PC1 = c(1.6485728230883807, -4.645104331781534, -6.428880535676489),
    PC2 = c(-4.013282700516296, -1.1167972663619026, -5.337951427775355)
  )

  mat_tbl <- testthat_tbl("mat")

  r <- mat_tbl %>%
    ml_pca(k = 2) %>%
    sdf_project() %>%
    select(dplyr::starts_with("PC")) %>%
    collect() %>%
    as.data.frame()

  expect_equal(s, r, tolerance = 1)
})

test_that("sdf_project() returns correct number of columns", {
  skip_databricks_connect()
  mat_tbl <- testthat_tbl("mat")

  for (k in 1:2) {
    expect_equal(
      mat_tbl %>%
        ml_pca(k = k) %>%
        sdf_project() %>%
        select(starts_with("PC")) %>%
        collect() %>%
        ncol(),
      k
    )
  }
})

test_that("sdf_project() takes newdata argument", {
  skip_databricks_connect()
  mat_tbl <- testthat_tbl("mat")

  expect_equal(
    mat_tbl %>%
      ml_pca(k = 3) %>%
      sdf_project() %>%
      collect(),
    mat_tbl %>% ml_pca(k = 3) %>% sdf_project(mat_tbl) %>% collect()
  )
})

test_that("ft_vector_assembler() param setting", {
  skip_databricks_connect()
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    input_cols = c("foo", "baz"),
    output_col = "bar"
  )
  test_param_setting(sc, ft_vector_assembler, test_args)
})

test_that("ft_vector_slicer() param setting", {
  skip_databricks_connect()
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    input_col = "foo",
    output_col = "bar",
    indices = 1:5
  )
  test_param_setting(sc, ft_vector_slicer, test_args)
})

test_that("ft_vector_slicer works", {
  skip_databricks_connect()
  sc <- testthat_spark_connection()
  df <- data.frame(
    V1 = 1,
    V2 = 2,
    V3 = 3
  )

  expect_warning_on_arrow(
    sliced <- sdf_copy_to(sc, df, overwrite = TRUE) %>%
      ft_vector_assembler(
        input_cols = paste0("V", 1:3),
        output_col = "vector"
      ) %>%
      ft_vector_slicer("vector", "sliced", 0:1) %>%
      pull(sliced)
  )

  expect_identical(sliced, list(c(1, 2)))
})

test_that("ft_vector_indexer() default params", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_vector_indexer)
})

test_that("ft_vector_indexer() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    input_col = "foo",
    output_col = "bar",
    max_categories = 15
  )
  test_param_setting(sc, ft_vector_indexer, test_args)
})

test_that("ft_vector_indexer() works properly", {
  sc <- testthat_spark_connection()
  sample_data_path <- get_test_data_path("sample_libsvm_data.txt")
  sample_data <- spark_read_libsvm(
    sc,
    "sample_data",
    sample_data_path,
    overwrite = TRUE
  )
  indexer <- ft_vector_indexer(
    sc,
    input_col = "features",
    output_col = "indexed",
    max_categories = 10
  ) %>%
    ml_fit(sample_data)

  expect_warning_on_arrow(
    expect_identical(
      indexer %>%
        ml_transform(sample_data) %>%
        head(1) %>%
        pull(indexed) %>%
        unlist() %>%
        length(),
      692L
    )
  )

  expect_warning_on_arrow(
    expect_identical(
      sample_data %>%
        ft_vector_indexer("features", "indexed", max_categories = 10) %>%
        head(1) %>%
        pull(indexed) %>%
        unlist() %>%
        length(),
      692L
    )
  )
})

test_clear_cache()
