package com.clearspring.websnake.app

import java.lang.String
import org.gridgain.scalar._
import scalar._
import java.io.File
import com.clearspring.websnake.tasks.{GetURLTextTaskParams, GetURLTextTask}

object Main
{
  System.setProperty("sun.net.client.defaultReadTimeout", "3000");
  System.setProperty("sun.net.client.defaultConnectTimeout", "3000");

  def main(args: Array[String]): Unit =
  {

    scalar
    {
      val baseDir = args(0)
      val files = (new File(args(1))).listFiles()
      for (f <- files)
      {
        val params = new GetURLTextTaskParams(baseDir, f.getPath)
        grid.execute(classOf[GetURLTextTask], params).get
      }

    }
  }

}
