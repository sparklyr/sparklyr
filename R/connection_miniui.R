#" @import shiny
#" @import miniUI
#' @export
#' @keywords internal
connection_spark_ui <- function() {
  componentVersionSelectChoices <- function(name) {
    selected <- spark_default_version()[[name]]
    choiceValues <- unique(spark_installed_versions()[[name]])
    choiceNames <- choiceValues
    choiceNames <- lapply(
      choiceNames,
      function(e) if (e == selected) paste(e, "(Default)") else e
    )

    names(choiceValues) <- choiceNames

    choiceValues
  }

  miniPage(
    miniContentPanel(
      selectInput(
        "master",
        "Master:",
        choices = c(
          "local" = "local",
          "Cluster..." = "cluster"
        )
      ),
      selectInput(
        "dbinterface",
        "DB Interface:",
        choices = c(
          "dplyr" = "dplyr",
          "(None)" = "none"
        )
      ),
      selectInput(
        "sparkversion",
        "Spark version:",
        choices = componentVersionSelectChoices("spark"),
        selected = spark_default_version()$spark
      ),
      selectInput(
        "hadoopversion",
        "Hadoop version:",
        choices = componentVersionSelectChoices("hadoop"),
        selected = spark_default_version()$hadoop
      ),
      textOutput("code")
    )
  )
}

#' @export
#' @keywords internal
connection_spark_server <- function(input, output, session) {
  output$code <- renderText(paste(
    "library(sparklyr)\n",
    if(input$dbinterface == "dplyr") "library(dplyr)\n" else "",
    "sc ",
    "<- ",
    "spark_connect(master = \"",
    input$master,
    "\")",
    sep = ""
  ))
}
