skip_connection("ml_mapping_tables")
skip_on_livy()
skip_on_arrow_devel()

test_that("register_mapping_tables populates the genv class/param mappings", {
  register_mapping_tables()

  expect_true(is.environment(genv_get_param_mapping_r_to_s()))
  expect_true(is.environment(genv_get_param_mapping_s_to_r()))
  expect_true(is.environment(genv_get_ml_class_mapping()))
  expect_true(is.environment(genv_get_ml_package_mapping()))

  # base class_mapping.json entries are loaded and resolve sparklyr's package
  class_mapping <- as.list(genv_get_ml_class_mapping())
  expect_equal(
    class_mapping[["org.apache.spark.ml.feature.StandardScaler"]],
    "ml_standard_scaler"
  )
  pkg_mapping <- as.list(genv_get_ml_package_mapping())
  expect_equal(
    pkg_mapping[["org.apache.spark.ml.feature.StandardScaler"]],
    "sparklyr"
  )

  # param_mapping round-trips R <-> Spark names
  r_to_s <- as.list(genv_get_param_mapping_r_to_s())
  s_to_r <- as.list(genv_get_param_mapping_s_to_r())
  expect_true(length(r_to_s) > 0)
  expect_true(length(s_to_r) > 0)
})

test_clear_cache()
