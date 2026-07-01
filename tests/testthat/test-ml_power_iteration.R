# Connection-free: the Spark < 2.4 guard can't run live (CI is on 3.5/4.1), so
# cover it by mocking the version check.
test_that("ml_power_iteration() requires Spark 2.4+", {
  with_mocked_bindings(
    spark_version = function(x) numeric_version("2.2.0"),
    .package = "sparklyr",
    expect_error(
      ml_power_iteration(structure(list(), class = "spark_connection")),
      "only supported in Spark 2.4"
    )
  )
})

skip_connection("ml_power_iteration")
skip_on_livy()
skip_on_arrow_devel()

test_that("ml_power_iteration() default params", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()

  test_default_args(sc, ml_power_iteration)
})

test_that("ml_power_iteration() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()

  test_args <- list(
    k = 3,
    max_iter = 30,
    init_mode = "random",
    src_col = "src_vertex",
    dst_col = "dst_vertex",
    weight_col = "gaussian_similarity"
  )
  test_param_setting(sc, ml_power_iteration, test_args, is_ml_pipeline = FALSE)
})

test_that("ml_power_iteration() works as expected with 'random' initialization mode", {
  test_requires_version("2.4.0")
  sc <- testthat_spark_connection()
  pic_data <- copy_to(sc, gen_pic_data())

  clusters <- ml_power_iteration(
    pic_data,
    k = 2,
    max_iter = 40,
    init_mode = "random",
    src_col = "src",
    dst_col = "dst",
    weight_col = "sim"
  )

  verify_clusters(clusters)
})

test_that("ml_power_iteration() works as expected with 'degree' initialization mode", {
  test_requires_version("2.4.0")
  sc <- testthat_spark_connection()
  pic_data <- copy_to(sc, gen_pic_data())

  clusters <- ml_power_iteration(
    pic_data,
    k = 2,
    max_iter = 10,
    init_mode = "degree",
    src_col = "src",
    dst_col = "dst",
    weight_col = "sim"
  )

  verify_clusters(clusters)
})

test_clear_cache()
