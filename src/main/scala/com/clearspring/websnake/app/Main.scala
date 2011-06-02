package com.clearspring.websnake.app

import java.lang.String
import org.gridgain.scalar._
import scalar._
import com.clearspring.websnake.tasks.{GetURLTextTask}
import java.io.File

object Main
{
  System.setProperty("sun.net.client.defaultReadTimeout", "3000");
  System.setProperty("sun.net.client.defaultConnectTimeout", "3000");

  def main(args: Array[String]): Unit =
  {

    scalar
    {
      //      val files = (new File("/tmp/data")).listFiles()
      val files = (new File("/Users/abramsm/work/augustus/local/mini0n/a815e87c-c6e7-46c2-b916-85bf241a46fb/0/live/split/110531")).listFiles()
      for (f <- files)
      {
        grid.execute(classOf[GetURLTextTask], f.getPath).get
      }

    }
  }

}
