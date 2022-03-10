library(targets)

getEunomiaDatabaseDetails <- function(cdmDatabaseSchema = "main",
                                      workDatabaseSchema = "main") {
  return(list(connectionDetails = Eunomia::getEunomiaConnectionDetails(),
              cdmDatabaseSchema = cdmDatabaseSchema,
              workDatabaseSchema = workDatabaseSchema))
}

getJobContext <- function(cdmDatabase) {
  exampleSpecificationPath <- "D:/git/anthonysena/CohortDiagnosticsModule/extras/sample_analysis_specification.json"
  exampleSpecification <- readChar(exampleSpecificationPath, file.info(exampleSpecificationPath)$size)
  jobContext <- jsonlite::fromJSON(txt = exampleSpecification)
  # Probably worth logging the job context at this point before sensitive info
  # is added to the context
  #ParallelLogger::loadSettingsFromJson()
  ParallelLogger::logDebug("jobContext = ", jobContext)
  # Add the connection details
  jobContext$cdmDatabase <- cdmDatabase
  invisible(jobContext) 
}

moduleValidate <- function(jobContext) {
  CohortDiagnosticsModule::validate(jobContext = jobContext)
}

moduleExecute <- function(validate, jobContext) {
  CohortDiagnosticsModule::execute(jobContext = jobContext)
}

moduleexportResults <- function(step, jobContext) {
  CohortDiagnosticsModule::exportResults(jobContext = jobContext)
}

moduleImportData <- function(step, jobContext) {
  CohortDiagnosticsModule::importData(jobContext = jobContext)
}

cleanup <- function(jobContext) {
  unlink(jobContext$executionSettings$outputFolder, recursive = TRUE)
}

# Set target-specific options such as packages.
tar_option_set(packages = c("CohortDiagnosticsModule", "Eunomia"),
               debug = c("validate", "exec")) # Debug the execution function

# End this file with a list of target objects.
list(
  tar_target(cdmDatabase, getEunomiaDatabaseDetails()),
  tar_target(jobContext, getJobContext(cdmDatabase)),
  tar_target(validate, moduleValidate(jobContext)),
  tar_target(exec, moduleExecute(validate, jobContext)),
  tar_target(export, moduleexportResults(exec, jobContext)), # Having to add prefix to keep ordering
  tar_target(import, moduleImportData(export, jobContext))
)

# To execute for debugging
#tar_make(callr_function = NULL, names = any_of(c("exec")), shortcut = FALSE)
