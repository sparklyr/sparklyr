context("do-spark")

test_requires("foreach")
test_requires("iterators")
test_requires("base64enc")

register_test_spark_connection <- function() {
  sc <- testthat_spark_connection()
  registerDoSpark(sc)
}

register_test_spark_connection()

u <- 1234
v <- 5678
x <- 123
y <- 456
z <- 789
fn_1 <- function(x) {
  list(fn_1 = list(x = x, u = u))
 }
fn_2 <- function(x) {
  y <- 123456
  inner_fn <- function(x) { list(inner_fn = list(x = x, y = y)) }
  list(fn_2 = list(fn_1(list(fn_1(list(x = x, v = y)), z = z)), y = y, z = z, inner_fn(z)))
}
fn_3 <- function(x) {
  u <- 789
  inner_fn <- function(z) { list(inner_fn = list(u = u, v = v, x = x, y = y, z = z)) }
  inner_fn
}
fn_4 <- fn_3(1357)

'%test%' <- function(obj, quoted_expr) {
  res <- list()
  for (impl in c("do", "dopar"))
    res[[impl]] <- eval(parse(text = paste(
      "obj %", impl, "% { ", deparse(quoted_expr), " }", sep = ""
    )))
  expect_equal(res$do, res$dopar)
}

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

test_that("doSpark works for loop referencing external functions and variables", {
  foreach(x = 1:20, .combine = list) %test% quote(fn_2(list(x, y, z, fn_3(x)(y), fn_4(x))))
})
