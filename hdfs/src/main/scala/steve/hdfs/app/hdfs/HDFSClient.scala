package steve.hdfs.app.hdfs

import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.collection.mutable.ListBuffer

class HDFSClient {
  val conf: Configuration = new Configuration()
  var fileSystem: FileSystem = _

  def this(url: String, userName: String) = {
    this()
    conf.set("dfs.replication", "1")
    conf.set("dfs.blocksize", "4m")
    fileSystem = FileSystem.get(new URI(url), conf, userName)
  }

  def uploadFile(src: String, dst: String, deleteSrc: Boolean, overwrite: Boolean): Unit = {
    val local = new Path(src)
    val hdfs = new Path(dst)
    fileSystem.copyFromLocalFile(deleteSrc, overwrite, local, hdfs)
  }

  def downloadFile(src: String, dst: String): Unit = {
    fileSystem.copyToLocalFile(new Path(src), new Path(dst))
  }

  def moveFile(src: String, dst: String): Unit = {
    fileSystem.rename(new Path(src), new Path(dst))
  }

  def getFileNameList(src: String): List[String] = {
    val fileInfoIter = fileSystem.listFiles(new Path(src), false)
    val fileNames:ListBuffer[String] = new ListBuffer[String]
    while (fileInfoIter.hasNext) {fileNames.append(fileInfoIter.next().getPath.getName)}
    fileNames.toList
  }

  def mkdirs(dir: String): Unit = {
    fileSystem.create(new Path(dir), true)
  }

  def deletePath(dir: String, recursion: Boolean): Unit = {
    fileSystem.delete(new Path(dir), recursion)
  }

  def close(): Unit = {
    fileSystem.close()
  }
}