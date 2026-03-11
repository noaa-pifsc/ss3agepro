

#' Creates a Stock Synthesis base model and bootstrap runs
#'
#' Sets up a Stock Synthesis Base model including individual bootstrap runs.
#' Function includes helper functions to to help with running SS in parallel
#'
#' @param seed Pusedorandom Number Seed. Default is 123
#' @param bootstrap_outdir Path where the bootstrap file and bootstrap runs will be saved
#' @template n_boot
#' @template basemodel_dir
#' @template ss3_exe
#'
#' @export
#' @import r4ss
#' @import parallel
#' @import stringr
#' @importFrom utils write.table
#' @importFrom this.path this.proj
#' @importFrom this.path here
#' @importFrom magrittr %>%
#' @importFrom dplyr filter
#' @importFrom dplyr select
#' @importFrom rlang .data
#' @importFrom rlang eval_tidy
#' @importFrom data.table data.table
#' @importFrom data.table rbindlist
#'
#' @examples
#' \dontrun{
#' #Import Basemodel from inst/01_base and output Bootstraps to inst/bsn_output
#' basemodel_dir <- file.path(find.package("ss3agepro"),"01_base")
#' output_dir <- file.path(tempdir())# ,"inst","bsn_output")
#'
#' setup_ss_bootstrap(basemodel_dir, bootstrap_outdir = output_dir, n_boot = 10)
#'
#' }
#'
#'
#' @keywords Bootstrap
#'
setup_ss_bootstrap <- function (basemodel_dir,
                                bootstrap_outdir,
                                n_boot = 100,
                                seed = 123,
                                ss3_exe = "ss3") {

  ## TODO: Option to clean up Previous Bootstrap files.

  checkmate::assert_directory_exists(basemodel_dir)
  checkmate::assert_directory_exists(bootstrap_outdir)
  checkmate::assert_numeric(n_boot)

  # Key directory: boot_dir
  boot_dir <- bootstrap_outdir

  #By Default, create a pseudo random number seed if seed is NULL
  if(!is.null(seed)){
    set.seed(seed)
  }

  #Run Model to SS Once to generate data bootstrap files
  ss_model_bootstrap(basemodel_dir, boot_dir, n_boot, ss3_exe)



  # Set up each bootstrap run in its own folder, to help with running SS in parallel
  Lt <- ss_model_n_boot(basemodel_dir, boot_dir, n_boot)

  run_parallel(Lt)

  # Copy n_boot sso files back to bootstrap directory
  copy_sso_n_boot(boot_dir, n_boot)

  message("\nOUTPUT BOOTSTRAP FILES\n")
  ## TODO: set BSN filename string parameter
  # Bootstrap Data Table written as "bootstrap.bsn" under the bootstrap directory
  bsn_file <- file.path(bootstrap_outdir,"bootstrap.bsn")
  write_bsn_file(bootstrap_outdir, bsn_file, n_boot)


}

#' Create SS data for bootstrap.
#'
#' Embedded Helper function to copy base model stock synthesis data for each
#' bootstrap and runs.
#'
#' @template basemodel_dir
#' @template boot_dir
#' @template n_boot
#' @template ss3_exe
#'
#' @keywords internal
#' @export
#'
ss_model_bootstrap <- function (basemodel_dir,
                                 boot_dir,
                                 n_boot = 1,
                                 ss3_exe = "ss3.exe") {

  checkmate::assert_directory_exists(basemodel_dir)
  checkmate::assert_numeric(n_boot)

  cli::cli_progress_step("Ensure Stock Synthesis binary exists")
  #Check of In Case ss3 binary exists in basemodel_dir, if not
  #ss3 binary saved to basebmodel_dir
  if(!checkmate::test_file_exists(ss3_exe)){
    get_ss3_exe(dir = basemodel_dir)
  }

  Sys.sleep(.5)
  cli::cli_progress_step("Copying basemodel Stock Syntheisis files to {boot_dir}")
  file.copy(
    list.files(
      basemodel_dir,
      pattern = paste0(
        "ss.par|data.ss|control.ss|starter.ss|forecast.ss", ss3_exe,
        sep="|"),
      full.names = TRUE),
    to = boot_dir)

  Sys.sleep(0.01)
  cli::cli_progress_step("Reading {file.path(boot_dir, \"starter.ss\")}")
  start <- r4ss::SS_readstarter(file = file.path(boot_dir, "starter.ss"))
  start$N_bootstraps <- n_boot + 2
  Sys.sleep(0.01)
  cli::cli_progress_step("Overwrite {file.path(boot_dir, \"starter.ss\")} with (n_boot + 2) bootstraps")
  r4ss::SS_writestarter(start, dir = boot_dir, overwrite = T)

  Sys.sleep(0.01)
  cli::cli_progress_step("Run Model")
  #r4ss::run(dir = boot_dir, exe = ss3_exe, extras = "-nohess",
  #          skipfinished = FALSE, show_in_console = F)
  run_r4ss_with_spinner(boot_dir, ss3_exe)
  cli::cli_progress_done()
}


#' Run r4ss with a spinner on Rconsole
#'
#' Allows to display a splnner ojection on the R Console to indicate running
#' Stock Synthesis process This is helped by running the r4ss process in \
#' another R session.
#'
#' @importFrom callr r_bg
#'
#' @param out_dir Ouptut directory where the stock synthesis files will be saved.
#' @template ss3_exe
#'
#' @keywords internal
#'
run_r4ss_with_spinner <- function(out_dir, ss3_exe) {

  # "Background Process" to run Stock Synthesis Run
  bg_process <- callr::r_bg(function(outdir) {
    # runs r4ss in a seperate R session
    r4ss::run(dir = outdir, exe = ss3_exe, extras = "-nohess",
              skipfinished = FALSE, show_in_console = F)
  }, args = list(outdir = out_dir))

  r4ss_spinner <- cli::make_spinner(template = "Running Stock Synthesis process ... {spin} ")

  while(bg_process$is_alive()){
    r4ss_spinner$spin()
    Sys.sleep(0.1) #Spinner Speed
  }

  r4ss_spinner$finish() #Remove spinner when ss4 process is done

  #Validation
  if(bg_process$get_exit_status() == 0) {
    cli::cli_alert_success("Stock synthesis Run complete!")
  }else{
    cli::cli_alert_danger("Model run failed. Check your Report.sso or warning files.")
  }

}


#' Create Bootstrap runs
#'
#' Set up each bootstrap run in its own folder, to help with running SS in
#' parallel. The Base Model Stock Synthesis Files are copied to each
#' Bootstrap run.
#'
#' Each Bootstrap run starter file will be saved as Data Boot Run File.
#'
#' @param n_boot number of bootstraps
#' @param basemodel_dir basemodel directory
#' @param boot_dir boostrap directory
#' @param ss3_exe ss3_exe
#'
#' @keywords Bootstrap
#' @export
#'
ss_model_n_boot <- function(basemodel_dir,
                              boot_dir,
                              n_boot,
                              ss3_exe = "ss3.exe") {

  #validate Base Model Report
  checkmate::assert_file_exists(file.path(boot_dir,"Report.sso"))

  Sys.sleep(.5)
  cli::cli_progress_step("Report Stock Syntheisis Output File exists")

  #Validate Base Model CompReport
  checkmate::assert_file_exists(file.path(boot_dir,"CompReport.sso"))

  Sys.sleep(.5)
  cli::cli_progress_step("Comp Report Stock Syntheisis Output File exists")

  #Validate Base Model covar
  checkmate::assert_file_exists(file.path(boot_dir,"covar.sso"))

  Sys.sleep(.5)
  cli::cli_progress_step("Co-Variance Stock Syntheisis Output File exists")

  #Validate Base Model warning
  checkmate::assert_file_exists(file.path(boot_dir,"warning.sso"))

  Sys.sleep(.5)
  cli::cli_progress_step("Warning Stock Syntheisis Output File exists")


  #create the bootstrap data file numbers (pad with leading 0s)
  #Rename to string_n_boot
  boot_num <- stringr::str_pad(seq(1, n_boot, by = 1), 3, pad = "0")



  # Set up each bootstrap run in its own folder, to help with running SS in parallel
  io_spinner <- cli::make_spinner(template = "Running n_boot process ... {spin} ")
  Lt <- vector("list",n_boot)
  for(i in 1:n_boot){

    #TODO: Rename aBootDir to n_boot_subdir
    aBootDir <- file.path(boot_dir, paste0("Boot",i))
    dir.create(aBootDir, showWarnings = FALSE)


    # For each n_boot Copy original SS files to "abootDir"
    file.copy(
      list.files(
        basemodel_dir,
        pattern = paste0("ss.par|control.ss|starter.ss|forecast.ss", ss3_exe,
                         sep="|"),
        full.names = TRUE),
      to = aBootDir)

    # Copy the bootstrapped data files
    # TODO: Make option to remove base caes data_boot ss files
    file.copy(file.path(boot_dir,paste0("data_boot_", boot_num[i], ".ss")),to=aBootDir)
    file.remove(file.path(boot_dir,paste0("data_boot_", boot_num[i], ".ss")))

    # Change Starter file to point to Bootstrap data file
    # TOGO: Parallel THIS
    starter <- r4ss::SS_readstarter(file = file.path(basemodel_dir, "starter.ss")) # read starter file
    starter[["datfile"]] <- paste0("data_boot_", boot_num[i], ".ss")
    r4ss::SS_writestarter(starter, dir = aBootDir, overwrite = TRUE)

    Lt[[i]] <- append(Lt[[i]], aBootDir)

    io_spinner$spin()
    Sys.sleep(0.1) #Spinner Speed
  }
  io_spinner$finish()
  cli::cli_progress_done()
  return(Lt)

}


#' Copy Bootstrap Run Output Files to Bootstrap Directory
#'
#' Copies the Stock Synthesis Report File, Composition Data, Cumulative
#' Summaries, and Log of Warnings from each Bootstrap run to the main
#' bootstrap directory. Each copied bootstrap run file will be renamed to
#' have a bootstrap run identifier.
#'
#'
#' @template boot_dir
#' @template n_boot
#' @param copy_compReport Copy compReport Bootstrap Run to Bootstrap
#' Directory? Default is TRUE
#' @param copy_covar Copy covar Bootstrap Run to Bootstrap Directory? Default
#' is TRUE
#' @param copy_warning Copy warning Bootstrap Run to Bootstrap Directory?
#' Default is TRUE.
#'
#' @keywords Bootstrap
#' @export
#'
copy_sso_n_boot <- function(boot_dir,
                            n_boot,
                            copy_compReport = TRUE,
                            copy_covar = TRUE,
                            copy_warning = TRUE) {

  #validate target boot_dir path
  checkmate::assert_directory_exists(boot_dir)

  for(i in 1:n_boot){

    n_boot_dir <- file.path(boot_dir, paste0("Boot",i))

    #Validate Bootstrap Run directory exists
    checkmate::assert_directory_exists(n_boot_dir)

    file.copy(file.path(n_boot_dir, "Report.sso"),
              paste(boot_dir, "/Report_", i, ".sso", sep = ""),
              overwrite = TRUE)

    if(copy_compReport){
      file.copy(file.path(n_boot_dir, "CompReport.sso"),
                paste(boot_dir, "/CompReport_", i, ".sso", sep = ""),
                overwrite = TRUE)
    }

    if(copy_covar){
      file.copy(file.path(n_boot_dir, "covar.sso"),
                paste(boot_dir, "/covar_", i, ".sso", sep = ""),
                overwrite = TRUE)
    }

    if(copy_warning){
      file.copy(file.path(n_boot_dir, "warning.sso"),
                paste(boot_dir, "/warning_",    i, ".sso", sep = ""),
                overwrite = TRUE)
    }

  }


}




#' Write an Age Based Bootstrap File
#'
#' Writes an Age Based Bootstrap File. Bootstrap timeseries is based on the
#' end of year model and derived quantities for each bootstrap run. Bootstrap
#' data table output is written to file.
#'
#' @param bsn_outfile Character file nameutil string or file connection used in
#' write Bootstrap data tables via [utils::write.table()]. Defaults to
#' "boot.bsn".
#' @template boot_dir
#' @template n_boot
#'
#' @keywords Bootstrap
#' @export
#'
write_bsn_file <- function(boot_dir, bsn_outfile = "boot.bsn" , n_boot = 1){

  checkmate::assert_directory_exists(boot_dir)

  endyr <- suppressWarnings(endyr_model(boot_dir))

  # set data.table parameters to bypass R CMD CHECK's "No Visible Binding" exception
  Yr <- NULL

  AgeStr.List <- list()
  for(i in 1:n_boot){

    aBootDir    <- file.path(boot_dir,paste0("Boot",i))
    anOutput    <- r4ss::SS_output(dir=aBootDir)
    anAgeStr    <- data.table::data.table(anOutput$natage)
    FinalAgeStr <- anAgeStr[Yr==endyr&anAgeStr$'Beg/Mid'=="B"] |> select(-("Area":"Era"))

    AgeStr.List <- append(AgeStr.List,list(FinalAgeStr))
  }

  BootAgeStr <- data.table::rbindlist(AgeStr.List)

  write.table(BootAgeStr,
              file = bsn_outfile,
              row.names=F, col.names=F)

  message(paste0("Bootstrap Table Written to ", bsn_outfile))

}

#' Extracts end year of model
#'
#' Embedded Helper function to return end year of model (endyr) and derived
#' quants
#'
#' @template boot_dir
#'
#' @import stringr
#' @importFrom r4ss SS_output
#' @importFrom dplyr select
#' @importFrom dplyr filter
#' @importFrom rlang .data
#' @importFrom magrittr %>%
#'
#' @keywords Bootstrap
#' @keywords internal
#'
endyr_model <- function (boot_dir) {

  base.model <- r4ss::SS_output(boot_dir)

  drvquants <-
    base.model$derived_quants %>%
    dplyr::filter(stringr::str_detect(.data$Label,"F_")) %>%
    dplyr::select("Label")

  endyr <-
    max( as.numeric(
      stringr::str_sub(drvquants$Label, stringr::str_length(drvquants$Label)-3,
                       stringr::str_length(drvquants$Label)) ),
      na.rm=TRUE)

  return(endyr)
}

