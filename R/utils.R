wait_file_exists <- function(filename, retries = 50) {
  while(!file.exists(filename) && retries >= 0) {
    retries <- retries  - 1;
    Sys.sleep(0.1)
  }

  file.exists(filename)
}

# From: https://github.com/apache/spark/blob/d6dc12ef0146ae409834c78737c116050961f350/R/pkg/R/deserialize.R
readInt <- function(con) {
  readBin(con, integer(), n = 1, endian = "big")
}

# From: https://github.com/apache/spark/blob/d6dc12ef0146ae409834c78737c116050961f350/R/pkg/R/deserialize.R
readString <- function(con) {
  stringLen <- readInt(con)
  raw <- readBin(con, raw(), stringLen, endian = "big")
  string <- rawToChar(raw)
  Encoding(string) <- "UTF-8"
  string
}
