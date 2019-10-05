package steve.hdfs.app.hdfs

import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class HDFSClient {
  val conf: Configuration = new Configuration()
  var fileSystem: FileSystem = _

  def this(url: String, userName: String) = {
    this()
    conf.set("dfs.replication", "1")
    conf.set("dfs.blocksize", "4m")
    fileSystem = FileSystem.get(new URI(url), conf, userName)
  }

  def uploadFile(localPath: String, HDFSPath: String, deleteSrc: Boolean, overwrite: Boolean): Unit = {
    val local = new Path(localPath)
    val hdfs = new Path(HDFSPath)
    fileSystem.copyFromLocalFile(deleteSrc, overwrite, local, hdfs)
  }

  def close(): Unit = {
    fileSystem.close()
  }
}