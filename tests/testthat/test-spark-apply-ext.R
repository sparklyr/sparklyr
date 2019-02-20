context("spark apply")
test_requires("dplyr")
sc <- testthat_spark_connection()

iris_tbl <- testthat_tbl("iris")

dates <- data.frame(dates = c(as.Date("2015/12/19"), as.Date(NA), as.Date("2015/12/19")))
dates_tbl <- testthat_tbl("dates")

colnas <- data.frame(c1 = c("A", "B"), c2 = c(NA, NA))
colnas_tbl <- testthat_tbl("colnas")

test_that("'spark_apply' can filter columns", {
  expect_equal(
    iris_tbl %>% spark_apply(function(e) e[1:1]) %>% collect(),
    iris_tbl %>% select(Sepal_Length) %>% collect()
  )
})

test_that("'spark_apply' can add columns", {
  expect_equal(
    iris_tbl %>% spark_apply(function(e) cbind(e, 1), names = c(colnames(iris_tbl), "new")) %>% collect(),
    iris_tbl %>% mutate(new = 1) %>% collect()
  )
})

test_that("'spark_apply' can concatenate", {
  expect_equal(
    iris_tbl %>% spark_apply(function(e) apply(e, 1, paste, collapse = " "), names = "s") %>% collect(),
    iris_tbl %>% transmute(s = paste(Sepal_Length, Sepal_Width, Petal_Length, Petal_Width, Species)) %>% collect()
  )
})

test_that("'spark_apply' can filter", {
  expect_equal(
    iris_tbl %>% spark_apply(function(e) e[e$Species == "setosa",]) %>% collect(),
    iris_tbl %>% filter(Species == "setosa") %>% collect()
  )
})

test_that("'spark_apply' works with 'sdf_repartition'", {
  expect_equal(
    iris_tbl %>% sdf_repartition(2L) %>% spark_apply(function(e) e) %>% collect(),
    iris_tbl %>% collect()
  )
})

test_that("'spark_apply' works with 'group_by' over multiple columns", {

  iris_tbl_ints <- iris_tbl %>%
    mutate(Petal_Width_Int = as.integer(Petal_Width))

  grouped_lm <- spark_apply(
    iris_tbl_ints,
    function(e, species, petal_width) {
      lm(Petal_Width ~ Petal_Length, e)$coefficients[["(Intercept)"]]
    },
    names = "Intercept",
    group_by = c("Species", "Petal_Width_Int")) %>% collect()

  iris_int <- iris %>% mutate(
    Petal_Width_Int = as.integer(Petal.Width),
    GroupBy = paste(Species, Petal_Width_Int, sep = "|")
  )

  lapply(
    unique(iris_int$GroupBy),
    function(group_by_entry) {
      parts <- strsplit(group_by_entry, "\\|")
      species_test <- parts[[1]][[1]]
      petal_width_test <- as.integer(parts[[1]][[2]])

      expect_equal(
        grouped_lm[grouped_lm$Species == species_test & grouped_lm$Petal_Width_Int == petal_width_test, ]$Intercept,
        lm(Petal.Width ~ Petal.Length, iris_int[iris_int$Species == species_test & iris_int$Petal_Width_Int == petal_width_test, ])$coefficients[["(Intercept)"]]
      )
    }
  )
})

test_that("'spark_apply' works over empty partitions", {
  skip_slow("takes too long to measure coverage")
  expect_equal(
    sdf_len(sc, 2, repartition = 4) %>%
      spark_apply(function(e) e) %>%
      collect() %>%
      as.data.frame(),
    data.frame(id = seq_len(2))
  )
})

test_that("'spark_apply' works over 'tryCatch'", {
  skip_slow("takes too long to measure coverage")
  expect_equal(
    sdf_len(sc, 1) %>%
      spark_apply(function(e) {
        tryCatch({
          stop("x")
        }, error = function(e) {
          100
        })
      }) %>%
      pull() %>%
      as.integer(),
    100
  )
})

test_that("'spark_apply' can filter data.frame", {
  skip_slow("takes too long to measure coverage")
  expect_equal(
    sdf_len(sc, 10) %>%
      spark_apply(function(e) as.data.frame(e[e$id > 1,])) %>%
      collect() %>%
      nrow(),
    9
  )
})

test_that("'spark_apply' can filter using dplyr", {
  skip_slow("takes too long to measure coverage")
  expect_equal(
    sdf_len(sc, 10) %>%
      spark_apply(function(e) dplyr::filter(e, id > 1)) %>%
      collect() %>%
      as.data.frame(),
    data.frame(id = c(2:10))
  )
})

test_that("'spark_apply' can return 'NA's", {
  skip_slow("takes too long to measure coverage")
  expect_equal(
    dates_tbl %>%
      spark_apply(function(e) e) %>%
      collect() %>%
      nrow(),
    nrow(dates)
  )
})

test_that("'spark_apply' can return 'NA's for dates", {
  skip_slow("takes too long to measure coverage")
  expect_equal(
    sdf_len(sc, 1) %>%
      spark_apply(function(e) data.frame(dates = c(as.Date("2001/1/1"), NA))) %>%
      collect() %>%
      nrow(),
    2
  )
})

test_that("'spark_apply' can roundtrip dates", {
  skip_slow("takes too long to measure coverage")
  expect_equal(
    dates_tbl %>%
      spark_apply(function(e) as.Date(e[[1]], origin = "1970-01-01")) %>%
      spark_apply(function(e) e) %>%
      collect() %>%
      pull() %>%
      class(),
    "Date"
  )
})

test_that("'spark_apply' can roundtrip Date-Time", {
  skip_slow("takes too long to measure coverage")
  expect_equal(
    dates_tbl %>%
      spark_apply(function(e) as.POSIXct(e[[1]], origin = "1970-01-01")) %>%
      spark_apply(function(e) e) %>%
      collect() %>%
      pull() %>%
      class() %>%
      first(),
    "POSIXct"
  )
})

test_that("'spark_apply' supports grouped empty results", {
  skip_slow("takes too long to measure coverage")
  process_data <- function(DF, exclude) {
    DF <- subset(DF, select = colnames(DF)[!colnames(DF) %in% exclude])
    DF[complete.cases(DF),]
  }

  data <- data.frame(
    grp = rep(c("A", "B", "C"), each = 5),
    x1 = 1:15,
    x2 = c(1:9, rep(NA, 6)),
    stringsAsFactors = FALSE
  )

  data_spark <- sdf_copy_to(sc, data, "grp_data", memory = TRUE)

  collected <- data_spark %>% spark_apply(
    process_data,
    group_by = "grp",
    columns = c("x1", "x2"),
    packages = FALSE,
    context = {exclude <- "grp"}
  ) %>% collect()

  expect_equal(
    collected,
    data %>% group_by(grp) %>% do(process_data(., exclude = "grp"))
  )
})

test_that("'spark_apply' can use anonymous functions", {
  skip_slow("takes too long to measure coverage")
  expect_equal(
    sdf_len(sc, 3) %>% spark_apply(~ .x + 1) %>% collect(),
    data_frame(id = c(2, 3, 4))
  )
})

test_that("'spark_apply' can apply function with 'NA's column", {
  skip_slow("takes too long to measure coverage")
  if (spark_version(sc) < "2.0.0") skip("automatic column types supported in Spark 2.0+")

  expect_equal(
    colnas_tbl %>% mutate(c2 = as.integer(c2)) %>% spark_apply(~ class(.x[[2]])) %>% pull(),
    "integer"
  )

  expect_equal(
    colnas_tbl %>%
      mutate(c2 = as.integer(c2)) %>%
      spark_apply(~ dplyr::mutate(.x, c1 = tolower(c1))) %>%
      collect(),
    colnas_tbl %>%
      mutate(c2 = as.integer(c2)) %>%
      mutate(c1 = tolower(c1))
  )
})
