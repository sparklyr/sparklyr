skip_connection("spark_compile")

test_that("jar file is created", {

  number_of_jars <- 7

  jar_folder <- path.expand("~/testjar")

  s_version <- testthat_spark_env_version()

  major_v <- strsplit(s_version, "\\.")[[1]][[1]]

  if(major_v >= 1) scala_v <- "2.10"
  if(major_v >= 2) scala_v <- "2.11"
  if(major_v >= 3) scala_v <- "2.12"

  jar_name <- sprintf("%s-%s-test.jar", s_version, scala_v)

  scs <- spark_compilation_spec(
    spark_version = s_version,
    scalac_path = find_scalac(scala_v),
    jar_name = jar_name,
    jar_path = find_jar(),
    scala_filter = make_version_filter(s_version)
  )

  Sys.setenv("R_SPARKINSTALL_COMPILE_JAR_PATH" = jar_folder)

  compile_package_jars(scs)

  expect_true(
    file.exists(file.path(jar_folder, jar_name))
  )

  expect_message(
    sparklyr_jar_verify_spark(),
    "- Spark version"
    )

  expect_length(
    sparklyr_jar_spec_list(),
    number_of_jars
  )

  expect_silent(
    download_scalac()
  )

  expect_is(
    make_version_filter(s_version),
    "function"
  )
})
