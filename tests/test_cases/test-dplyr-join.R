context("dplyr join")
sc <- testthat_spark_connection()

test_that("left_join works as expected", {
  test_requires_version("2.0.0", "dots in column names")
  test_requires("dplyr")

  s1 <- data.frame(x=1:3, y=4:6)
  s2 <- data.frame(x=1:3, y=7:9)

  d1 <- copy_to(sc, s1)
  d2 <- copy_to(sc, s2)

  j1 <- left_join(d1, d2, by='x') %>% collect()
  j2 <- left_join(s1, s2, by='x')

  expect_equal(j1, j2)
})
