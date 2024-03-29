skip_connection("dplyr-hive-operators")
skip_on_livy()

sc <- testthat_spark_connection()

test_that("regex relational operators work", {
  skip_databricks_connect()
  test_requires("dplyr")

  hello <- tibble(hello = c(
    "hello my friend",
    "hello my dog",
    "hello my cat"
  ))
  hello_tbl <- testthat_tbl("hello")

  expect_equal(
    hello_tbl %>%
      filter(hello %like% "%cat%") %>%
      collect(),
    hello %>%
      filter(grepl("cat", hello))
  )

  products <- tibble(
    product_id = 1:3,
    product_description = c("fruit", "Fruit", "milk")
  )

  products_tbl <- testthat_tbl("products")

  expect_equal(
    products_tbl %>%
      mutate(category = ifelse(product_description %rlike% "F|fruit",
        "produce", "dairy"
      )) %>%
      collect(),
    products %>%
      mutate(category = ifelse(grepl("F|fruit", product_description),
        "produce", "dairy"
      ))
  )

  expect_equal(
    products_tbl %>%
      mutate(category = ifelse(product_description %regexp% "F|fruit",
        "produce", "dairy"
      )) %>%
      collect(),
    products %>%
      mutate(category = ifelse(grepl("F|fruit", product_description),
        "produce", "dairy"
      ))
  )
})

test_clear_cache()
