skip_connection("dplyr-join")

sc <- testthat_spark_connection()

test_that("left_join works as expected", {
  test_requires_version("2.0.0", "dots in column names")
  test_requires("dplyr")

  s1 <- data.frame(x = 1:3, y = 4:6)
  s2 <- data.frame(x = 1:3, y = 7:9)

  d1 <- sdf_copy_to(sc, s1, overwrite = TRUE)
  d2 <- sdf_copy_to(sc, s2, overwrite = TRUE)

  j1 <- left_join(d1, d2, by = "x") %>%
    dplyr::arrange(x) %>%
    collect()
  j2 <- left_join(s1, s2, by = "x")

  expect_equivalent(j1, j2) %>% dplyr::arrange(x)
})

test_that("left_join works with default suffixes", {
  test_requires("dplyr")

  s1 <- data.frame(
    group = sample(c("A", "B"), size = 10, replace = T),
    value1 = rnorm(10),
    conflict = "df1"
  )
  s2 <- data.frame(
    group = c("A", "B"),
    conflict = "df2"
  )

  d1 <- copy_to(sc, s1, "test1", overwrite = TRUE)
  d2 <- copy_to(sc, s2, "test2", overwrite = TRUE)

  j1 <- left_join(d1, d2, by = "group")
  j2 <- collect(j1)

  expect_equal(colnames(j1), c("group", "value1", "conflict_x", "conflict_y"))

  expect_named(j2, c("group", "value1", "conflict_x", "conflict_y"))
})

test_that("joins works with user-supplied `.` suffixes", {
  test_requires("dplyr")

  s1 <- data.frame(
    group = sample(c("A", "B"), size = 10, replace = T),
    value1 = rnorm(10),
    conflict = "df1"
  )
  s2 <- data.frame(
    group = c("A", "B"),
    conflict = "df2"
  )

  d1 <- copy_to(sc, s1, "test1", overwrite = TRUE)
  d2 <- copy_to(sc, s2, "test2", overwrite = TRUE)

  j1 <- left_join(d1, d2, by = "group")
  j2 <- collect(j1)

  j3 <- left_join(d1, d2, by = "group", suffix = c(".table1", ".table2"))
  j4 <- left_join(d1, d2, by = "group", suffix = c("_table1", "_table2"))

  expect_equal(colnames(j3), colnames(j4))

  expect_message(
    left_join(d1, d2, by = "group", suffix = c(".x", ".y")),
    "Replacing '.' with '_' in suffixes. New suffixes: _x, _y"
  )

  expect_message(
    right_join(d1, d2, by = "group", suffix = c(".x", ".y")),
    "Replacing '.' with '_' in suffixes. New suffixes: _x, _y"
  )

  expect_message(
    full_join(d1, d2, by = "group", suffix = c(".x", ".y")),
    "Replacing '.' with '_' in suffixes. New suffixes: _x, _y"
  )


  expect_message(
    inner_join(d1, d2, by = "group", suffix = c(".x", ".y")),
    "Replacing '.' with '_' in suffixes. New suffixes: _x, _y"
  )
})
