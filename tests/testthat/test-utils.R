# ---------------------------------------------------------------------------
# Connection-free unit tests for the pure helpers in R/utils.R. Placed above the
# skip_connection() gate so they run without a live Spark session.
# ---------------------------------------------------------------------------

test_that("spark_integ_test_skip() defaults to FALSE", {
  expect_false(
    spark_integ_test_skip(structure(list(), class = "spark_connection"), "x")
  )
})

test_that("is.installed() detects installed packages", {
  expect_true(is.installed("stats"))
  expect_false(is.installed("nonexistentpkgxyz123"))
})

test_that("utils_starts_with() matches prefixes", {
  expect_true(utils_starts_with("hello", "he"))
  expect_false(utils_starts_with("hello", "lo"))
  expect_false(utils_starts_with("hi", "hello")) # lhs shorter than rhs
})

test_that("aliased_path() collapses the home directory to ~", {
  expect_equal(aliased_path(paste0(path.expand("~/"), "foo")), "~/foo")
  expect_equal(aliased_path("/other/path"), "/other/path")
})

test_that("transpose_list() transposes a list of vectors", {
  expect_equal(transpose_list(list(c(1, 2), c(3, 4))), list(c(1, 3), c(2, 4)))
})

test_that("random_string() uses the prefix and is random", {
  expect_match(random_string("pre"), "^pre_")
  expect_false(identical(random_string("p"), random_string("p")))
})

test_that("printf() formats and prints", {
  expect_output(printf("%d-%s", 1L, "a"), "1-a")
})

test_that("regex_replace() applies named replacements in order", {
  expect_equal(regex_replace("a b.c", " " = "_", "[.]" = "-"), "a_b-c")
})

test_that("spark_sanitize_names() cleans, disables, and reports", {
  # opt-out returns names unchanged
  expect_equal(
    spark_sanitize_names("a b", list(sparklyr.sanitize.column.names = FALSE)),
    "a b"
  )
  # default: spaces -> '_', duplicates made unique
  expect_equal(
    spark_sanitize_names(c("a b", "a b"), list()),
    c("a_b", "a_b_1")
  )
  # verbose path emits a message
  expect_message(
    spark_sanitize_names(
      "a b",
      list(sparklyr.sanitize.column.names.verbose = TRUE)
    ),
    "renamed"
  )
})

test_that("spark_normalize_path() normalizes local paths but leaves URLs", {
  expect_equal(spark_normalize_path("hdfs://host/path"), "hdfs://host/path")
  expect_equal(
    spark_normalize_path("foo/bar"),
    normalizePath("foo/bar", mustWork = FALSE)
  )
})

test_that("enumerate() applies f over names and values", {
  expect_equal(
    enumerate(list(a = 1, b = 2), function(nm, val) paste0(nm, val)),
    list(a = "a1", b = "b2")
  )
})

test_that("path_program() finds programs and errors when missing", {
  expect_error(
    path_program("definitely-not-a-real-program-xyz"),
    "required but not available"
  )
  rscript <- Sys.which("Rscript")
  if (nzchar(rscript)) {
    expect_equal(unname(path_program("Rscript")), unname(rscript))
  }
})

test_that("infer_active_package_name() reads the DESCRIPTION Package field", {
  d <- withr::local_tempdir()
  writeLines("Package: foopkg", file.path(d, "DESCRIPTION"))
  with_mocked_bindings(
    package_root = function() d,
    .package = "sparklyr",
    expect_equal(infer_active_package_name(), "foopkg")
  )
})

test_that("remove_class() drops a class", {
  x <- structure(1, class = c("a", "b"))
  expect_equal(class(remove_class(x, "a")), "b")
})

test_that("trim_whitespace() strips surrounding whitespace", {
  expect_equal(trim_whitespace("  a b  "), "a b")
})

test_that("split_separator() differs for livy vs default connections", {
  livy <- split_separator(structure(list(), class = "livy_connection"))
  expect_equal(livy$plain, "|~|")
  def <- split_separator(structure(list(), class = "spark_connection"))
  expect_equal(def$plain, "\3")
})

test_that("resolve_fn() calls functions and passes values through", {
  expect_equal(resolve_fn(function(x) x + 1, 1), 2)
  expect_equal(resolve_fn(42), 42)
})

test_that("is.tbl_spark() checks the class", {
  expect_true(is.tbl_spark(structure(list(), class = "tbl_spark")))
  expect_false(is.tbl_spark(1))
})

test_that("`%<-%` assigns multiple variables and validates length", {
  c(p, q) %<-% list(1, 2)
  expect_equal(p, 1)
  expect_equal(q, 2)
  expect_error(c(p, q) %<-% list(1), "same number")
})

test_that("sort_named_list() orders entries by name", {
  expect_equal(sort_named_list(list(b = 2, a = 1)), list(a = 1, b = 2))
})

test_that("`%>>%` / `%@%` apply a function with a variable arg list", {
  expect_equal((1 %>>% sum) %@% list(2, 3), 6)
})

test_that("`%>|%` forwards a chain of invocations to invoke()", {
  captured <- NULL
  with_mocked_bindings(
    invoke = function(jobj, method, ...) {
      captured <<- list(method = method, args = list(...))
      "<r>"
    },
    .package = "sparklyr",
    {
      res <- "<jobj>" %>|% list(list("m1", "a"), list("m2"))
      expect_equal(res, "<r>")
      expect_equal(captured$method, "%>%")
      expect_equal(captured$args, list(list("m1", "a"), list("m2")))
    }
  )
})

test_that("download_file() raises a low timeout for the call and restores it", {
  withr::local_options(timeout = 60)
  seen <- NULL
  with_mocked_bindings(
    download.file = function(...) {
      seen <<- getOption("timeout")
      0L
    },
    .package = "sparklyr",
    download_file("u", "d")
  )
  expect_equal(seen, 300)
  expect_equal(getOption("timeout"), 60)
})

test_that("get_os() and os_is_windows() report the platform", {
  expect_true(get_os() %in% c("win", "mac", "unix"))
  expect_equal(os_is_windows(), get_os() == "win")
})

test_that("infer_required_r_packages() finds deps via library and attachNamespace", {
  fn_library <- function(x) {
    library(stats)
    x
  }
  expect_true("stats" %in% infer_required_r_packages(fn_library))

  fn_attach <- function(x) {
    attachNamespace("stats")
    x
  }
  expect_true("stats" %in% infer_required_r_packages(fn_attach))
})

test_that("update_jars() runs the jar build pipeline in order", {
  calls <- character()
  rec <- function(name) function(...) calls[[length(calls) + 1]] <<- name
  with_mocked_bindings(
    download_scalac = rec("download_scalac"),
    sparklyr_jar_verify_spark = rec("verify"),
    compile_package_jars = rec("compile"),
    spark_update_embedded_sources = rec("embed"),
    .package = "sparklyr",
    update_jars()
  )
  expect_equal(calls, c("download_scalac", "verify", "compile", "embed"))
})

skip_connection("utils")
skip_on_livy()
skip_on_arrow_devel()

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
  sep <- sep %>%
    pcre_to_java() %>%
    as_utf8()
  actual <- x %>%
    as_utf8() %>%
    invoke("split", sep, -1L) %>%
    lapply(to_string)

  expect_equal(actual, expected)
}

test_that("jarray() works as expected", {
  num_elems <- 1000
  sc <- testthat_spark_connection()
  arr <- jarray(
    sc,
    lapply(seq(num_elems), function(x) invoke_new(sc, "sparklyr.TestValue", x)),
    element_type = "sparklyr.TestValue"
  )

  expect_equal(
    invoke_static(sc, "sparklyr.Test", "readTestValueArray", arr),
    num_elems
  )
})

test_that("jfloat() works as expected", {
  sc <- testthat_spark_connection()
  x <- 1.23e-3
  jflt <- jfloat(sc, x)

  expect_true(inherits(jflt, "spark_jobj"))
  expect_equal(jflt %>% invoke("doubleValue"), x)
  expect_equal(
    invoke_static(sc, "sparklyr.Test", "readFloat", jflt),
    x
  )
})

test_that("jfloat_array() works as expected", {
  sc <- testthat_spark_connection()
  x <- c(-1.23e-3, 0, 1.23e-3)
  jflt_arr <- jfloat_array(sc, x)

  expect_true(inherits(jflt_arr, "spark_jobj"))

  for (method in c("readJFloatArray", "readFloatArray")) {
    expect_equal(
      invoke_static(sc, "sparklyr.Test", method, jflt_arr),
      x,
      tolerance = 5e-08,
      scale = 1
    )
  }
})

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

  for (delim in c(
    "A",
    "Z",
    "a",
    "z",
    "0",
    "9",
    "\"",
    "&",
    "@",
    "[",
    "]",
    "^",
    "~"
  )) {
    expect_split(
      sprintf("\x02%s\x03%s ", delim, delim),
      "[[:graph:]]",
      list("\x02", "\x03", " ")
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

  for (delim in c(
    "A",
    "Z",
    "a",
    "z",
    "0",
    "9",
    "\"",
    "&",
    "@",
    "[",
    "]",
    "^",
    "~"
  )) {
    expect_split(
      sprintf("\x01%s\x19%s\x7F", delim, delim),
      "[[:print:]]",
      list("\x01", "\x19", "\x7F")
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
    "!",
    "\"",
    "#",
    "$",
    "%",
    "&",
    "'",
    "(",
    ")",
    "*",
    "+",
    ",",
    "-",
    ".",
    "/",
    ":",
    ";",
    "<",
    "=",
    ">",
    "?",
    "@",
    "[",
    "\\",
    "]",
    "^",
    "_",
    "'",
    "{",
    "|",
    "}",
    "~"
  )) {
    expect_split(
      sprintf("a%sb%sc", delim, delim),
      "[[:punct:]]",
      list("a", "b", "c")
    )
    expect_split(
      sprintf("a%sb%scAd", delim, delim),
      "[A[:punct:]]",
      list("a", "b", "c", "d")
    )
  }
})

test_that("pcre_to_java converts [:space:] correctly", {
  test_requires_version("2.0.0")

  for (delim in c(" ", "\t", "\r", "\n", "\v", "\f")) {
    expect_split(
      sprintf("a%sb%sc", delim, delim),
      "[[:space:]]",
      list("a", "b", "c")
    )
    expect_split(
      sprintf("a%sb%sc&d", delim, delim),
      "[&[:space:]]",
      list("a", "b", "c", "d")
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

  for (delim in c(
    as.character(seq(0, 9)),
    "a",
    "b",
    "c",
    "d",
    "e",
    "f",
    "A",
    "B",
    "C",
    "D",
    "E",
    "F"
  )) {
    expect_split(sprintf("g%sh", delim), "[[:xdigit:]]", list("g", "h"))
    expect_split(sprintf("G%sH|I", delim), "[|[:xdigit:]]", list("G", "H", "I"))
  }
})

test_that("replicate_colnames() returns a 1-row frame with the same columns", {
  iris_tbl <- testthat_tbl("iris")
  res <- replicate_colnames(iris_tbl)
  expect_setequal(colnames(res), colnames(iris_tbl))
  expect_equal(nrow(res), 1)
})

test_that("simulate_vars_spark() builds a typed proxy with matching columns", {
  iris_tbl <- testthat_tbl("iris")
  res <- simulate_vars_spark(iris_tbl)
  expect_setequal(colnames(res), colnames(iris_tbl))
})

test_that("simulate_vars_spark() can drop grouping columns", {
  iris_tbl <- testthat_tbl("iris")
  grouped <- iris_tbl %>% dplyr::group_by(Species)
  res <- simulate_vars_spark(grouped, drop_groups = TRUE)
  expect_false("Species" %in% colnames(res))
})

test_that("tidyselect_data_proxy.tbl_spark returns a proxy under predicates", {
  iris_tbl <- testthat_tbl("iris")
  proxy <- tidyselect_data_proxy(iris_tbl)
  expect_setequal(colnames(proxy), colnames(iris_tbl))
})

test_that("tidyselect_data_has_predicates.tbl_spark respects the option", {
  iris_tbl <- testthat_tbl("iris")
  expect_true(tidyselect_data_has_predicates(iris_tbl))

  withr::local_options(sparklyr.support.predicates = FALSE)
  expect_false(tidyselect_data_has_predicates(iris_tbl))
})

test_clear_cache()
