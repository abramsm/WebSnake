package com.clearspring.websnake.tasks

import java.io.File
import java.security.MessageDigest
import java.util.{ArrayList, List}

import com.clearspring.websnake.jobs.GetURLTextJob
import org.gridgain.grid.{GridJobResult, GridTaskSplitAdapter, GridJob}
import scala.collection.JavaConversions._

class GetURLTextTask extends GridTaskSplitAdapter[String, String] {
  def split(gridSize: Int, urlFile: String) = {
    val file = new File(urlFile)
    val outDir = file.getName()
    val jobs: List[GridJob] = new ArrayList[GridJob]()
    val urls = scala.io.Source.fromFile(urlFile).mkString.split('\n');
    for (url <- urls) {
      val job: GridJob = new GetURLTextJob(url, outDir)
      jobs.add(job)
    }
    jobs
  }

  def reduce(results: List[GridJobResult]) = {
    for (result <- results) {
      val job: GetURLTextJob = result.getJob()
      val data: String = result.getData()
      printToFile(job.getOutDir(), job.getURL(), data.toString.replace("\n", " "))
    }
    ""
  }

  def printToFile(outDir: String, url: String, content: String) {
    val dir = new File(outDir)
    if (!dir.exists())
      dir.mkdir()

    val file = new File(outDir + File.separator + md5Hash(url))
    val printWriter = new java.io.PrintWriter(file)

    try {
      printWriter.println(content)
    }
    finally {
      printWriter.close()
    }
  }

  // http://code-redefined.blogspot.com/2009/05/md5-sum-in-scala.html
  def md5Hash(s: String) = {
    MessageDigest.getInstance("MD5").digest(s.getBytes).map(0xFF & _).map {
      "%02x".format(_)
    }.foldLeft("") {
      _ + _
    }
  }
}