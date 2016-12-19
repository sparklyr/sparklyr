#' @import shiny
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

  tags$div(
    tags$head(
      tags$style(
        HTML("
          body {
            background: none;

            font-family : \"Lucida Sans\", \"DejaVu Sans\", \"Lucida Grande\", \"Segoe UI\", Verdana, Helvetica, sans-serif;
            font-size : 12px;
            -ms-user-select : none;
            -moz-user-select : none;
            -webkit-user-select : none;
            user-select : none;

            margin: 0;
            margin-top: 5px;
          }

          .shiny-input-container {
            min-width: 100%;
            margin-bottom: 7px;
          }

          .shiny-input-container > .control-label {
            display: inline-block;
            width: 195px;
          }

          .shiny-input-container > div {
            display: inline-block;
            width: 300px;
          }

          #shiny-disconnected-overlay {
            display: none;
          }
        ")
      )
    ),
    selectInput(
      "master",
      "Master:",
      choices = c(
        "local" = "local",
        "Cluster..." = "cluster"
        # TODO: Changing spark versions filters the right hadoop version
        # TODO: If Spark not installed, prompt install
        # TODO: If running as desktop: Error
        # TODO: If java not installed: ComponentsNotInstalledDialogs.showJavaNotInstalled(context.getJavaInstallUrl());
        # TODO: If running as server and no SPARK_HOME: Error ComponentsNotInstalledDialogs.showSparkHomeNotDefined()
        # TODO: Selection opens "Connect to Cluster", "Spark master: ". "spark://local:7077"
        # TODO: Support rstudio.spark.connections option
        # TODO: Provide UI to choose master connection
        # TODO: Need to store dialog preferences somwhere (say, selecting dplyr) (see connectionsDbInterface)
      ),
      selectize = FALSE
    ),
    selectInput(
      "dbinterface",
      "DB Interface:",
      choices = c(
        "dplyr" = "dplyr",
        "(None)" = "none"
      ),
      selectize = FALSE
    ),
    div(
      style = "height: 10px"
    ),
    selectInput(
      "sparkversion",
      "Spark version:",
      choices = componentVersionSelectChoices("spark"),
      selected = spark_default_version()$spark,
      selectize = FALSE
    ),
    selectInput(
      "hadoopversion",
      "Hadoop version:",
      choices = componentVersionSelectChoices("hadoop"),
      selected = spark_default_version()$hadoop,
      selectize = FALSE
    )
  )
}

connection_spark_server <- function(input, output, session) {
  hasDefaultSparkVersion <- reactive({
    input$sparkversion == spark_default_version()$spark
  })

  hasDefaultHadoopVersion <- reactive({
    input$hadoopversion == spark_default_version()$hadoop
  })

  codeReactive <- reactive({
    paste(
      "library(sparklyr)\n",
      if(input$dbinterface == "dplyr") "library(dplyr)\n" else "",
      "sc ",
      "<- ",
      "spark_connect(master = \"",
      input$master,
      "\"",
      if (!hasDefaultSparkVersion()) paste(", version = \"", input$sparkversion, "\"", sep = "") else "",
      if (!hasDefaultHadoopVersion()) paste(", hadoop_version = \"", input$hadoopversion, "\"", sep = "") else "",
      ")",
      sep = ""
    )
  })

  observe({
    if (exists(".rs.updateNewConnectionDialog")) {
      update <- get(".rs.updateNewConnectionDialog")
      update(codeReactive())
    }
  })
}

#' A Shiny app that can be used to construct a \code{spark_connect} statement
#'
#' @export
#'
#' @import shiny
#'
#' @keywords internal
connection_spark_shinyapp <- function() {
  shinyApp(connection_spark_ui, connection_spark_server)
}
