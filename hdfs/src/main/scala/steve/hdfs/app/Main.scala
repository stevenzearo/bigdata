package steve.hdfs.app

import steve.hdfs.app.hdfs.HDFSClient

object Main {
  def main(args: Array[String]) :Unit = {
    val client: HDFSClient = new HDFSClient("hdfs://localhost:8020", "steve")
//    client.uploadFile("/home/steve/IdeaProjects/bigdata/hdfs/src/main/resources/test.txt", "/test/upload/", deleteSrc = false, overwrite = true)
//    client.downloadFile("/test/upload", "/home/steve/IdeaProjects/bigdata/hdfs/src/main/resources/download")
    client.getFileNameList("/").foreach(println)
    client.close()
  }
}
