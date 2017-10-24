context("ml feature - pca")

sc <- testthat_spark_connection()

mat <- dplyr::data_frame(
  V1 = c(0, 2, 4),
  V2 = c(1, 0, 0),
  V3 = c(0, 3, 0),
  V4 = c(7, 4, 6),
  V5 = c(0, 5, 7))

test_that("ft_pca() param setting", {
  args <- list(
    x = sc, input_col = "in", output_col = "out", k = 4
  )
  ft <- do.call(ft_pca, args)
  expect_equal(
    ml_params(ft, names(args)[-1]),
    args[-1]
  )
})

test_that("ft_pca() works", {
  test_requires("dplyr")

  s <- data_frame(
    PC1 = c(1.6485728230883807, -4.645104331781534, -6.428880535676489),
    PC2 = c(-4.013282700516296, -1.1167972663619026, -5.337951427775355),
    PC3 = c(-5.524543751369388, -5.524543751369387, -5.524543751369389)
  )

  mat_tbl <- testthat_tbl("mat")

  r <- mat_tbl %>%
    ft_vector_assembler(paste0("V", 1:5), "v") %>%
    ft_pca("v", "pc", k = 3) %>%
    sdf_separate_column("pc", into = paste0("PC", 1:3)) %>%
    select(starts_with("PC", ignore.case = FALSE)) %>%
    collect()

  expect_equal(s, r)
})

# Backwards compat

test_that("ml_pca() agrees with Scala result", {
  test_requires("dplyr")

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

  s <- data.frame(
    PC1 = c(1.6485728230883807, -4.645104331781534, -6.428880535676489),
    PC2 = c(-4.013282700516296, -1.1167972663619026, -5.337951427775355),
    PC3 = c(-5.524543751369388, -5.524543751369387, -5.524543751369389)
  )

  mat_tbl <- testthat_tbl("mat")

  r <- mat_tbl %>%
    ml_pca(k = 3) %>%
    sdf_project() %>%
    select(dplyr::starts_with("PC")) %>%
    collect() %>%
    as.data.frame()

  expect_equal(s, r)
})

test_that("sdf_project() returns correct number of columns", {
  mat_tbl <- testthat_tbl("mat")

  for (k in 1:2) {
    expect_equal(mat_tbl %>%
                   ml_pca(k = k) %>%
                   sdf_project() %>%
                   select(dplyr::starts_with("PC")) %>%
                   collect() %>%
                   ncol(),
                 k)
  }
})

test_that("sdf_project() takes newdata argument", {
  mat_tbl <- testthat_tbl("mat")

  expect_equal(mat_tbl %>%
                 ml_pca(k = 3) %>%
                 sdf_project() %>%
                 collect(),
               mat_tbl %>% ml_pca(k = 3) %>%
                 sdf_project(mat_tbl) %>%
                 collect())
})
