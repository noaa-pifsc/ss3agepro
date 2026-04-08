


#' Runs Stock Synthesis in Parallel
#'
#' Runs instances of Stock Synthesis in Parallel via `r4ss::run` for each
#' directory in the input directory list.
#'
#' @import parallel
#' @import checkmate
#' @importFrom r4ss run
#' @importFrom future plan multisession sequential
#' @importFrom future.apply future_lapply
#' @importFrom parallelly availableCores
#' @import progressr
#'
#' @param ss_dirlist Filelist
#' @param ss3_exe ss3_exe
#'
#' @examples
#' \dontrun{
#' run_parallel()
#' }
#'
#'
run_parallel <- function(ss_dirlist, ss3_exe = "ss3.exe") {

  #Validate file list is a list type
  checkmate::assert_list(ss_dirlist, any.missing = FALSE)

  #Validate "Directories" in ss_dirlist exists
  checkmate::assert_directory_exists(unlist(ss_dirlist))

  progressr::with_progress({

    steps <- progressr::progressor(steps = length(ss_dirlist))

    # Force Progress Bar to appear at 0% before r4ss run
    steps(amount = 0, message = "Starting Stock Synthesis Bootstrap Runs ...")

    #parallel Loop
    results_list <- future.apply::future_lapply(ss_dirlist, function(x) {

      output <- tryCatch({
        #out <- r4ss::run(dir = x[[1]], exe = ss3_exe, skipfinished = FALSE)
        suppress_r4ss_messages({
          out <- r4ss::run(dir = x, exe = ss3_exe, extras = "-nohess", skipfinished = FALSE, show_in_console = FALSE)
        })

        x

      }, error = function(e) {
        paste0("Failed at ", x, "\n, Reason: ", e$message)
      })

      steps()

      return(output)

    },
    future.seed = TRUE,
    future.packages = c("r4ss"),
    future.scheduling = 1,
    future.conditions = character(0)) #

    return(results_list)

  })

}


#' @rdname run_parallel
#' @template n_cores
#' @export
#'
run_r4ss_parallel <- function(ss_dirlist, n_cores = 1, ss3_exe = "ss3.exe") {

  #Validate "Directories" in ss_dirlist exists
  checkmate::assert_list(ss_dirlist, any.missing = FALSE)
  checkmate::assert_directory_exists(unlist(ss_dirlist))
  checkmate::assert_numeric(n_cores, len = 1, lower = 1, upper = parallelly::availableCores()-1)

  # Capture original user enivroment
  original_plan <- future::plan()
  original_handlers <- progressr::handlers()

  # RESET user core environment after exiting this function.
  on.exit({
    future::plan(original_plan)
    progressr::handlers(original_handlers)
  }, add = TRUE)

  #backend
  progressr::handlers(
    progressr::handler_cli(
      format = "Running Models (This nay take awhile) : {cli::pb_bar} {cli::pb_current}/{cli::pb_total} [{cli::pb_percent}] | ETA: {cli::pb_eta}"
    )
  )
  progressr::handlers(global = TRUE)

  # Apply User's Core Choice
  if(n_cores > 1){
    future::plan(future::multisession, workers = n_cores)
    cli::cli_alert_info("Running {length(ss_dirlist)} subdirector{?y/ies} on Stock Synthesis in parallel across {n_cores} core{?s} ...")
  }else{
    future::plan(future::sequential)
    cli::cli_alert_info("Running {length(ss_dirlist)} subdirector{?y/ies} on Stock Synthesis sequentially on 1 core ...")
  }

  out <- run_parallel(ss_dirlist, ss3_exe)

  return(out)

}


#' Suppress r4ss verbose messages
#'
#' Helper Function to suppress the verbose messages the "chatty" r4ss during
#' the run_parallel function.
#'
#' @param exp Function Call
#'
suppress_r4ss_messages <- function(exp) {
  utils::capture.output(
    utils::capture.output(
      suppressMessages(suppressWarnings(
        outres <- exp
      )),
      type = "message"
    ),
    type = "output"
  )
  return(invisible(outres))
}
