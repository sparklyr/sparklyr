package sparklyr

import java.io.File
import java.util.UUID

object FileUtils {
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
