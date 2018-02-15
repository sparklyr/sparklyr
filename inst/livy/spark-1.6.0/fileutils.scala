//
// This file was automatically generated using livy_sources_refresh()
// Changes to this file will be reverted.
//

class FileUtils {
  import java.io.File
  import java.util.UUID

  def ensureDir(base: String, path: String): File = {
    var dir: File = new File(base, path)
    if (!dir.exists()) {
      dir.mkdirs()
    }

    dir
  }

  def createTempDir: File = {
    val workerFolderName: String = "sparkworker"
    val root: String = System.getProperty("java.io.tmpdir")

    ensureDir(root, workerFolderName)
    ensureDir(root, workerFolderName + File.separator + UUID.randomUUID.toString)
  }
}
