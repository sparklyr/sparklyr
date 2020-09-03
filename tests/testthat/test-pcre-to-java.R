context("PCRE to Java regex")

sc <- testthat_spark_connection()

as_utf8 <- function(ascii) {
  invoke_static(
    sc,
    "org.apache.spark.unsafe.types.UTF8String",
    "fromString",
    ascii
  )
}

to_string <- function(utf8) {
  invoke(utf8, "toString")
}

expect_split <- function(x, sep, expected) {
  sep <- sep %>% pcre_to_java() %>% as_utf8()
  actual <- x %>% as_utf8() %>% invoke("split", sep, -1L) %>% lapply(to_string)

  expect_equal(actual, expected)
}

test_that("pcre_to_java converts [:alnum:] correctly", {
  test_requires_version("2.0.0")

  for (delim in c(letters, LETTERS, as.character(seq(0, 9)))) {
    expect_split(sprintf("^%s$", delim), "[[:alnum:]]", list("^", "$"))
    expect_split(sprintf("^%s@|$", delim), "[|[:alnum:]]", list("^", "@", "$"))
  }
})

test_that("pcre_to_java converts [:alpha:] correctly", {
  test_requires_version("2.0.0")

  for (delim in c(letters, LETTERS)) {
    expect_split(sprintf("1%s2", delim), "[[:alpha:]]", list("1", "2"))
    expect_split(sprintf("1%s2|3", delim), "[|[:alpha:]]", list("1", "2", "3"))
  }
})

test_that("pcre_to_java converts [:ascii:] correctly", {
  test_requires_version("2.0.0")

  for (x in c("\u00A9", "\u00B1")) {
    for (delim in c("~", " ", "\033", "a", "Z", "$")) {
      expect_split(sprintf("%s%s%s", x, delim, x), "[[:ascii:]]", list(x, x))
    }
  }
})

test_that("pcre_to_java converts [:blank:] correctly", {
  test_requires_version("2.0.0")

  for (delim in c(" ", "\t")) {
    expect_split(sprintf("^%s$", delim), "[[:blank:]]", list("^", "$"))
    expect_split(sprintf("^%s@|$", delim), "[|[:blank:]]", list("^", "@", "$"))
  }
})

test_that("pcre_to_java converts [:cntrl:] correctly", {
  test_requires_version("2.0.0")

  for (delim in c("\x01", "\x1F", "\x7F")) {
    expect_split(sprintf("^%s$", delim), "[[:cntrl:]]", list("^", "$"))
    expect_split(sprintf("^%s@|$", delim), "[|[:cntrl:]]", list("^", "@", "$"))
  }
})

test_that("pcre_to_java converts [:digit:] correctly", {
  test_requires_version("2.0.0")

  for (delim in as.character(seq(0, 9))) {
    expect_split(sprintf("^%s$", delim), "[[:digit:]]", list("^", "$"))
    expect_split(sprintf("^%s@|$", delim), "[|[:digit:]]", list("^", "@", "$"))
  }
})

test_that("pcre_to_java converts [:graph:] correctly", {
  test_requires_version("2.0.0")

  for (delim in c("A", "Z", "a", "z", "0", "9", "\"", "&", "@", "[", "]", "^", "~")) {
    expect_split(
      sprintf("\x02%s\x03%s ", delim, delim), "[[:graph:]]", list("\x02", "\x03", " ")
    )
    expect_split(
      sprintf("\x02%s\x03\x01\x04%s ", delim, delim),
      "[\x01[:graph:]]",
      list("\x02", "\x03", "\x04", " ")
    )
  }
})

test_that("pcre_to_java converts [:lower:] correctly", {
  test_requires_version("2.0.0")

  for (delim in letters) {
    expect_split(sprintf("A%sB", delim), "[[:lower:]]", list("A", "B"))
    expect_split(sprintf("A%sB|C", delim), "[|[:lower:]]", list("A", "B", "C"))
  }
})

test_that("pcre_to_java converts [:print:] correctly", {
  test_requires_version("2.0.0")

  for (delim in c("A", "Z", "a", "z", "0", "9", "\"", "&", "@", "[", "]", "^", "~")) {
    expect_split(
      sprintf("\x01%s\x19%s\x7F", delim, delim), "[[:print:]]", list("\x01", "\x19", "\x7F")
    )
    expect_split(
      sprintf("\x02%s\x19\x01\x7F%s\x10", delim, delim),
      "[\x01[:print:]]",
      list("\x02", "\x19", "\x7F", "\x10")
    )
  }
})

test_that("pcre_to_java converts [:punct:] correctly", {
  test_requires_version("2.0.0")

  for (delim in c(
                  "!", "\"", "#", "$", "%", "&", "'", "(",
                  ")", "*", "+", ",", "-", ".", "/", ":",
                  ";", "<", "=", ">", "?", "@", "[", "\\",
                  "]", "^", "_", "'", "{", "|", "}", "~")
  ) {
    expect_split(
      sprintf("a%sb%sc", delim, delim), "[[:punct:]]", list("a", "b", "c")
    )
    expect_split(
      sprintf("a%sb%scAd", delim, delim), "[A[:punct:]]", list("a", "b", "c", "d")
    )
  }
})

test_that("pcre_to_java converts [:space:] correctly", {
  test_requires_version("2.0.0")

  for (delim in c(" ", "\t", "\r", "\n", "\v", "\f")) {
    expect_split(
      sprintf("a%sb%sc", delim, delim), "[[:space:]]", list("a", "b", "c")
    )
    expect_split(
      sprintf("a%sb%sc&d", delim, delim), "[&[:space:]]", list("a", "b", "c", "d")
    )
  }
})

test_that("pcre_to_java converts [:upper:] correctly", {
  test_requires_version("2.0.0")

  for (delim in LETTERS) {
    expect_split(sprintf("a%sb", delim), "[[:upper:]]", list("a", "b"))
    expect_split(sprintf("a%sb|c", delim), "[|[:upper:]]", list("a", "b", "c"))
  }
})

test_that("pcre_to_java converts [:word:] correctly", {
  test_requires_version("2.0.0")

  for (delim in c(letters, LETTERS, "_")) {
    expect_split(sprintf("^%s$", delim), "[[:word:]]", list("^", "$"))
    expect_split(sprintf("^%s@|$", delim), "[|[:word:]]", list("^", "@", "$"))
  }
})

test_that("pcre_to_java converts [:xdigits:] correctly", {
  test_requires_version("2.0.0")

  for (delim in c(as.character(seq(0, 9)),
                  "a", "b", "c", "d", "e", "f",
                  "A", "B", "C", "D", "E", "F")
  ) {
    expect_split(sprintf("g%sh", delim), "[[:xdigit:]]", list("g", "h"))
    expect_split(sprintf("G%sH|I", delim), "[|[:xdigit:]]", list("G", "H", "I"))
  }
})
