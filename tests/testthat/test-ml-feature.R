context("ml feature")

sc <- testthat_spark_connection()

test_that("We can instantiate tokenizer object", {
  tokenizer <- ml_tokenizer(sc, input_col = "x", output_col = "y", name = "tok")
  java_class <- tokenizer$stages[["tok"]]$.stage %>%
    invoke("getClass") %>%
    invoke("getName")
  expect_equal(java_class, "org.apache.spark.ml.feature.Tokenizer")
})
