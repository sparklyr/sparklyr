context("spark apply")
test_requires("dplyr")
sc <- testthat_spark_connection()

iris_tbl <- testthat_tbl("iris")

dates <- data.frame(dates = c(as.Date("2015/12/19"), as.Date(NA), as.Date("2015/12/19")))
dates_tbl <- testthat_tbl("dates")

test_that("'spark_apply' supports grouped empty results", {
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

  for (i in 1:20)
  {
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
  }
})
