# Data Delta Log: logging for dataset creation or update

## Operations

  - Create log file if it doesn't exist
  - Allow raw and processed versions of dataset
      + Identify variables in each
  - Insert output from processing script into the log file
  - Generate MD5 hash for data version verification

## Command arguments

  - Source file path (optional)
  - Processing script path (optional)
  - Processed file path (required)

## Created algorithmically

  - Log file name is processed file base name with .log extension
