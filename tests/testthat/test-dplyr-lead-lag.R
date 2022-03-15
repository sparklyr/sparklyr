skip_on_livy()
skip_on_arrow_devel()

sc <- testthat_spark_connection()

test_that("lead and lag take numeric values for 'n' (#925)", {
  test_requires("dplyr")
  example_df <- tibble(
    ID = c(10L, 1L, 4L, 2L, 8L, 5L, 7L, 9L, 3L, 6L),
    Cat = rep(letters[1:5], 2),
    Numb = c(3, 10, NA, 5, 1, 6, NA, 4, NA, 8)
  )
  example_df_tbl <- copy_to(sc, example_df, overwrite = TRUE)

  expect_equal(
    example_df_tbl %>%
      mutate(Numb1 = lag(Numb, 1, order_by = ID)) %>%
      mutate(Numb2 = lead(Cat, 1, order_by = ID)) %>%
      collect() %>%
      as_tibble(),
    example_df %>%
      mutate(Numb1 = lag(Numb, 1, order_by = ID)) %>%
      mutate(Numb2 = lead(Cat, 1, order_by = ID)) %>%
      arrange(ID)
  )
})
