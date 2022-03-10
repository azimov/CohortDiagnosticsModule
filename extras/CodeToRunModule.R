# First start by constructing this jobContext object
# This would be done upstream by something like targets
exampleSpecificationPath <- "extras/sample_analysis_specification.json"
connectionDetails <- Eunomia::getEunomiaConnectionDetails()
jobContext <- CohortDiagnosticsModule::CDJobContext$new(jsonSpecificationPath = exampleSpecificationPath,
                                                        cdmDatabase = list(connectionDetails = list(server = connectionDetails$server(), dbms = "sqlite"),
                                                                           databaseId = "eunomia",
                                                                           cdmDatabaseSchema = "main",
                                                                           workDatabaseSchema = "main"),
                                                        useSecurePassword = FALSE)

# Probably worth logging the job context at this point before sensitive info
# is added to the context
#ParallelLogger::loadSettingsFromJson()
ParallelLogger::logDebug("jobContext = ", jobContext)

# Now validate that the module has the necessary info
# in the job context to execute
CohortDiagnosticsModule::validate(jobContext = jobContext)

# Execute the module
CohortDiagnosticsModule::execute(jobContext = jobContext)

# Export the results
CohortDiagnosticsModule::exportResults(jobContext = jobContext)

# For cleanup after the test
unlink(jobContext$executionSettings$outputFolder, recursive = TRUE, force = TRUE)

# Run again using a "real" database
rsConnectionDetails <- list(dbms = "redshift",
                            server = paste(keyring::key_get("OHDA_PROD_1_SERVER"), "ims_australia_lpd", sep = "/"),
                            user = keyring::key_get("OHDA_PROD_1_USERNAME"),
                            extraSettings = "ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory")


jobContext <- CohortDiagnosticsModule::CDJobContext$new(jsonSpecificationPath = exampleSpecificationPath,
                                                        cdmDatabase = list(connectionDetails = rsConnectionDetails,
                                                                           cdmDatabaseSchema = "cdm_ims_australia_lpd_v1945",
                                                                           workDatabaseSchema = "scratch_jgilber2"),
                                                        keyringPasswordStore = "OHDA_PROD_1_PASSWORD",
                                                        useSecurePassword = TRUE)

# Now validate that the module has the necessary info
# in the job context to execute
CohortDiagnosticsModule::validate(jobContext = jobContext)

# Execute the module
CohortDiagnosticsModule::execute(jobContext = jobContext)

# Export the results
CohortDiagnosticsModule::exportResults(jobContext = jobContext)

# For cleanup after the test
unlink(jobContext$executionSettings$outputFolder, recursive = TRUE, force = TRUE)
