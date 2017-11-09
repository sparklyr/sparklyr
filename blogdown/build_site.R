
rebuild_site <- function(overwrite = FALSE){
  source("blogdown/build_helpers.R")
  process_content(overwrite = overwrite)
  process_reference(overwrite = overwrite)
  reset_public()

  old_wd <- getwd()

  setwd(file.path(old_wd, "blogdown"))

  blogdown::serve_site()


  on.exit(setwd(old_wd))
}


