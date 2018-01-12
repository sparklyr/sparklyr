context("ml feature interaction")

sc <- testthat_spark_connection()

test_that("ft_interaction() works properly", {
  df <- data.frame(
    V1 = 1, V2 = 2, V3 = 3, V4 = 8, V5 = 4, V6 = 5
  )
  df_tbl <- sdf_copy_to(sc, df, overwrite = TRUE)
  interacted <- df_tbl %>%
    ft_vector_assembler(paste0("V", 1:3), "vec1") %>%
    ft_vector_assembler(paste0("V", 4:6), "vec2") %>%
    ft_interaction(c("V1", "vec1", "vec2"), "interactedCol") %>%
    dplyr::pull(interactedCol)
  expect_equal(
    interacted,
    list(c(8, 4, 5, 16, 8, 10, 24, 12, 15))
  )
})
