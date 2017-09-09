context("shinyapp")

test_that("'connection_spark_ui' can generate the connection app", {
  sparklyr_ui <- connection_spark_ui()
  expect_equal(class(sparklyr_ui), "shiny.tag")
})
