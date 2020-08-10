spark_yarn_get_conf_property <- function(property, fails = TRUE) {
  confDir <- Sys.getenv("YARN_CONF_DIR")
  if (nchar(confDir) == 0) {

    # some systems don't set YARN_CONF_DIR but do set HADOOP_CONF_DIR
    confDir <- Sys.getenv("HADOOP_CONF_DIR")
    if (nchar(confDir) == 0) {
      if (fails) stop("Yarn Cluster mode requires YARN_CONF_DIR or HADOOP_CONF_DIR to be set.") else return(NULL)
    }
  }

  yarnSite <- file.path(confDir, "yarn-site.xml")
  if (!file.exists(yarnSite)) {
    if (fails) stop("Yarn Cluster mode requires yarn-site.xml to exist under YARN_CONF_DIR") else return(NULL)
  }

  yarnSiteXml <- xml2::read_xml(yarnSite)

  yarnPropertyValue <- xml2::xml_text(xml2::xml_find_all(
    yarnSiteXml,
    paste0("//name[.='", property, "']/parent::property/value")
  ))

  get_variables_in_value_strings <- function(strings) {
    var.regex <- "\\$\\{([^$]+)\\}"
    parsed <- gregexpr(var.regex, strings, perl = TRUE)
    lapply(seq_along(parsed), function(j) {
      p <- parsed[[j]]
      unlist(sapply(seq_along(p), function(i) {
        if (p[i] == -1) {
          return(NULL)
        }
        st <- attr(p, "capture.start")[i, ]
        substring(strings[j], st, st + attr(p, "capture.length")[i, ] - 1)
      }))
    })
  }

  while (length(vars <- get_variables_in_value_strings(yarnPropertyValue)) && length(vars[[1]])) {
    for (var in vars[[1]]) {
      value <- xml2::xml_text(xml2::xml_find_all(
        yarnSiteXml,
        paste0("//name[.='", var, "']/parent::property/value")
      ))

      yarnPropertyValue <- gsub(paste0("${", var, "}"), value, yarnPropertyValue, fixed = TRUE)
    }
  }

  yarnPropertyValue
}
