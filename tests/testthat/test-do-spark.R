context("do-spark")

test_requires("foreach")
test_requires("iterators")
test_requires("base64enc")

register_test_spark_connection <- function() {
  sc <- testthat_spark_connection()
  registerDoSpark(sc)
}

register_test_spark_connection()

'%test%' <- function(obj, quoted_expr) {
  res <- list()
  for (impl in c("do", "dopar"))
    res[[impl]] <- eval(parse(text = paste(
      "obj %", impl, "% { ", deparse(quoted_expr), " }", sep = ""
    )))
  expect_equal(res$do, res$dopar)
}

test_that("doSpark preserves exception error message", {
  expect_error(
    foreach (x = 1:10) %dopar% {
      if (x == 10) stop("runtime error")
    },
    regexp = "task 10 failed - \"runtime error\""
  )
})

test_that("doSpark works for simple loop", {
  foreach(x = 1:10) %test% quote(x * x)
})

test_that("doSpark works for simple loop with combine function", {
  foreach(x = 1:10, .combine = sum) %test% quote(x * x)
})

test_that("doSpark works for simple loop of matrices", {
  foreach(x = 1:10) %test% quote(as.matrix(x))
})

.test_objs <- list(list(1, "a"), list("b", 4), list(a = 1, b = list(c = 4, d = 5), 6, list(e = list(7))))

test_that("doSpark works for loop with arbitrary R objects", {
  foreach(x = .test_objs) %test% quote(x)
})

test_that("doSpark works for loop with arbitrary R objects with combine function", {
  foreach(x = .test_objs, .combine = c) %test% quote(x)
})

test_that("doSpark works for loop with arbitrary R objects with multicombine", {
  foreach(x = 1:20, .combine = list, .multicombine = TRUE, .maxcombine = 5) %test% quote(x)
})
