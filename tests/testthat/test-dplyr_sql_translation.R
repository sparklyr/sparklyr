skip_connection("dplyr_sql_translation")

sc <- testthat_spark_connection()

df1 <- tibble(a = 1:3, b = letters[1:3])
df2 <- tibble(b = letters[1:3], c = letters[24:26])
df1_tbl <- testthat_tbl("df1")
df2_tbl <- testthat_tbl("df2")

test_that("regex relational operators work", {
  skip_on_livy()
  skip_databricks_connect()
  test_requires("dplyr")

  hello <- tibble(
    hello = c(
      "hello my friend",
      "hello my dog",
      "hello my cat"
    )
  )
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
      mutate(
        category = ifelse(
          product_description %rlike% "F|fruit",
          "produce",
          "dairy"
        )
      ) %>%
      collect(),
    products %>%
      mutate(
        category = ifelse(
          grepl("F|fruit", product_description),
          "produce",
          "dairy"
        )
      )
  )

  expect_equal(
    products_tbl %>%
      mutate(
        category = ifelse(
          product_description %regexp% "F|fruit",
          "produce",
          "dairy"
        )
      ) %>%
      collect(),
    products %>%
      mutate(
        category = ifelse(
          grepl("F|fruit", product_description),
          "produce",
          "dairy"
        )
      )
  )
})

test_that("grepl works as expected", {
  regexes <- c(
    "a|c",
    ".",
    "b",
    "x|z",
    "",
    "y",
    "e",
    "^",
    "$",
    "^$",
    "[0-9]",
    "[a-z]",
    "[b-z]"
  )
  verify_equivalent <- function(actual, expected) {
    # handle an edge case for arrow-enabled Spark connection
    for (col in colnames(df2)) {
      expect_equivalent(
        as.character(actual[[col]]),
        as.character(expected[[col]])
      )
    }
  }
  for (regex in regexes) {
    verify_equivalent(
      df2 %>% dplyr::filter(grepl(regex, b)),
      df2_tbl %>% dplyr::filter(grepl(regex, b)) %>% collect()
    )
    verify_equivalent(
      df2 %>% dplyr::filter(grepl(regex, c)),
      df2_tbl %>% dplyr::filter(grepl(regex, c)) %>% collect()
    )
  }
})

test_that("pmin and pmax work", {
  pmin_df <- data.frame(x = 11:20, y = 1:10)

  tbl_pmin_df <- sdf_copy_to(sc, pmin_df, overwrite = TRUE)

  remote_p <- tbl_pmin_df %>%
    mutate(
      p_min = pmin(x, y),
      p_max = pmax(x, y)
    ) %>%
    collect()

  local_p <- pmin_df %>%
    mutate(
      p_min = pmin(x, y),
      p_max = pmax(x, y)
    )

  expect_true(
    all(remote_p == local_p)
  )

  expect_error(
    {
      collect(mutate(tbl_pmin_df, x = pmin(x, y, na.rm = FALSE)))
    },
    regexp = "na.rm = TRUE"
  )

  expect_error(
    {
      collect(mutate(tbl_pmin_df, x = pmax(x, y, na.rm = FALSE)))
    },
    regexp = "na.rm = TRUE"
  )
})

test_clear_cache()
