context("lag & lead")

sc <- testthat_spark_connection()

test_that("lead and lag take numeric values for 'n' (#925)", {
  skip_on_spark_master()
  test_requires("dplyr")
  example_df <- data_frame(ID = 1:10, Cat = rep(letters[1:5], 2),
                          Numb = c(3, 10, NA, 5, 1, 6, NA, 4, NA, 8))
  example_df_tbl <- copy_to(sc, example_df, overwrite = TRUE)

  expect_equal(
    example_df_tbl %>%
      arrange(ID) %>%
      mutate(Numb1 = lag(Numb, 1)) %>%
      mutate(Numb2 = lead(Cat, 1)) %>%
      collect() %>%
      lapply(function(x) ifelse(is.nan(x), NA, x)) %>%
      as_tibble(),
    example_df %>%
      arrange(ID) %>%
      mutate(Numb1 = lag(Numb, 1)) %>%
      mutate(Numb2 = lead(Cat, 1))
  )
})
