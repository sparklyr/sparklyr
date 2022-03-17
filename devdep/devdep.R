library(purrr)
library(dplyr)
library(git2r)
library(fs)

ld <- pak::local_deps()

all_deps <- ld %>%
  transpose() %>%
  map_dfr(~ {
    pd <- packageDescription(.x$package)
    br <- pd$BugReport
    br2 <- ifelse(is.null(br), "", br)
    rp <- substr(br2, 1, nchar(br2) - 7)
    url <- pd$URL
    url2 <- ifelse(is.null(url), "", url)
    tibble(
      package = pd$Package,
      bug_report = br2,
      url = url2,
      repo = rp
    )
  })

prep_deps <- all_deps %>%
  mutate(
    repo = ifelse(package == "base64enc", "https://github.com/s-u/base64enc", repo),
    repo = ifelse(package == "assertthat", "https://github.com/hadley/assertthat", repo),
    repo = ifelse(package == "codetools", "https://gitlab.com/luke-tierney/codetools", repo),
    repo = ifelse(package == "uuid", "https://github.com/s-u/uuid", repo),
    repo = ifelse(package == "forge", "https://github.com/rstudio/forge", repo)
    ) %>%
  select(package, repo) %>%
  filter(package != "sparklyr") %>%
  arrange(repo)

missing_deps <- filter(prep_deps, repo == "")

if(nrow(missing_deps) > 0) stop("Missing repos")

latest_sha <- prep_deps %>%
  transpose() %>%
  map_dfr(~ {
    repo <- .x$repo
    if(substr(repo, 1, 19) == "https://github.com/") {
      rl <- remote_ls(repo)
      sha <- rl[1]
      org <- "github"
    } else {
      sha <- ""
      org <- ""
    }
    tibble(
      package = .x$package,
      repo = .x$repo,
      sha = sha,
      org = org
    )
  })

latest_sha

latest_file <- "devdep/latest_sha.csv"
if(file_exists(latest_file)) {
  from_file <- read.csv(latest_file) %>%
    select(package, repo, sha)

  dep_install <- latest_sha %>%
    select(sha) %>%
    anti_join(from_file, by = "sha")

  } else {
  dep_install <- latest_sha
}

write.csv(latest_sha, latest_file)

pkg_folder <- "devdep/packages"

if(!dir_exists(pkg_folder)) dir_create(pkg_folder)

dep_install %>%
  filter(org == "github") %>%
  transpose() %>%
  walk(~ remotes::install_github(.x$repo, lib = pkg_folder))


.libPaths(here::here(pkg_folder))


devtools::test(filter = "^dbi$")





