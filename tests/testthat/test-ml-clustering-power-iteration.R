
sc <- testthat_spark_connection()

r1 <- 1
n1 <- 80L
r2 <- 4
n2 <- 80L

gen_circle <- function(radius, num_pts) {
  # generate evenly distributed points on a circle centered at the origin
  seq(0, num_pts - 1) %>%
    lapply(
      function(pt) {
        theta <- 2 * pi * pt / num_pts

        radius * c(cos(theta), sin(theta))
      }
    )
}

guassian_similarity <- function(pt1, pt2) {
  dist2 <- sum((pt2 - pt1)^2)

  exp(-dist2 / 2)
}

gen_pic_data <- function() {
  n <- n1 + n2
  pts <- append(gen_circle(r1, n1), gen_circle(r2, n2))
  num_unordered_pairs <- n * (n - 1) / 2

  src <- rep(0L, num_unordered_pairs)
  dst <- rep(0L, num_unordered_pairs)
  sim <- rep(0, num_unordered_pairs)

  idx <- 1
  for (i in seq(2, n)) {
    for (j in seq(i - 1)) {
      src[[idx]] <- i - 1L
      dst[[idx]] <- j - 1L
      sim[[idx]] <- guassian_similarity(pts[[i]], pts[[j]])
      idx <- idx + 1
    }
  }

  tibble::tibble(src = src, dst = dst, sim = sim)
}

verify_clusters <- function(clusters) {
  expect_setequal(
    split(clusters, clusters$cluster) %>%
      lapply(
        function(cluster) {
          cluster %>%
            dplyr::select(id) %>%
            lapply(as.integer)
        }
      ) %>%
      unlist(recursive = FALSE) %>%
      unname(),
    list(seq(0, n1 - 1), seq(n1, n1 + n2 - 1))
  )
}

pic_data <- copy_to(sc, gen_pic_data())

test_that("ml_power_iteration() default params", {
  test_requires_latest_spark()

  test_default_args(sc, ml_power_iteration)
})

test_that("ml_power_iteration() param setting", {
  test_requires_latest_spark()

  test_args <- list(
    k = 3,
    max_iter = 30,
    init_mode = "random",
    src_col = "src_vertex",
    dst_col = "dst_vertex",
    weight_col = "gaussian_similarity"
  )
  test_param_setting(sc, ml_power_iteration, test_args, is_ml_pipeline = FALSE)
})

test_that("ml_power_iteration() works as expected with 'random' initialization mode", {
  test_requires_version("2.4.0")

  clusters <- ml_power_iteration(
    pic_data,
    k = 2,
    max_iter = 40,
    init_mode = "random",
    src_col = "src",
    dst_col = "dst",
    weight_col = "sim"
  )

  verify_clusters(clusters)
})

test_that("ml_power_iteration() works as expected with 'degree' initialization mode", {
  test_requires_version("2.4.0")

  clusters <- ml_power_iteration(
    pic_data,
    k = 2,
    max_iter = 10,
    init_mode = "degree",
    src_col = "src",
    dst_col = "dst",
    weight_col = "sim"
  )

  verify_clusters(clusters)
})
