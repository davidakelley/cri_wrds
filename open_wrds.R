# Estabish and ensure that we have an active connection to WRDS.
#
# If there is a connection, test to make sure it's active.
#
# If there's no connection, create one.
#
# If there's a function named open_wrds_connection, call that function. If not,
# open the Shiny app to collect credentials.
#
# David Kelley, 2019

require(RPostgres)
require(shiny)

# Shiny app to collect login info
loginApp <- shinyApp(
  fluidPage(
    titlePanel("Input WRDS Credentials"),
    textInput("username", "User name"),
    passwordInput("password", "Password"),
    shiny::actionButton("submit", "Submit")
  ),

  function(input, output) {
    observe({
      if (input$submit != 0)
        stopApp(list(
          username = isolate(input$username),
          password = isolate(input$password)
        ))
    })
  }
)

# Test if we need to create a new connection
if (exists("wrds") && !dbIsValid(wrds)) {
  dbDisconnect(wrds)
  create_connection = TRUE
} else if (!exists("wrds")) {
  create_connection = TRUE
} else {
  create_connection = FALSE
}

if (create_connection && !exists("open_wrds_connection")) {
  # Get login info
  login <- runApp(loginApp)

  # Create connection
  wrds <- dbConnect(Postgres(),
                    host='wrds-pgdata.wharton.upenn.edu',
                    port=9737,
                    user=login$username,
                    password=login$password,
                    dbname='wrds',
                    sslmode='require')

  # Remove login info
  rm(login)

} else if (create_connection && exists("open_wrds_connection")) {
  wrds = open_wrds_connection()
}

rm(loginApp)
