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

  dplyr::tibble(src = src, dst = dst, sim = sim)
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
