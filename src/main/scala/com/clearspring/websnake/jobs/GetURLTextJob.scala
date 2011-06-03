package com.clearspring.websnake.jobs

import org.gridgain.grid.GridJobAdapterEx
import org.htmlparser.beans.StringBean
import java.io.IOException
import com.clearspring.websnake.app.Util

class GetURLTextJob(url: String, outDir: String) extends GridJobAdapterEx
{
  def getURL(): String =
  {
    url
  }

  def getOutDir(): String =
  {
    outDir
  }

  def execute(): String =
  {
    println("Getting URL: " + url + " and outdir = " + outDir)
    try
    {
      val sb = new StringBean()
      sb.setLinks(false);
      sb.setReplaceNonBreakingSpaces(true);
      sb.setCollapse(true);
      sb.setURL(url)
      val strings = sb.getStrings

      var ret = ""
      if (Util.isAscii(strings)) {
        ret = url + ":::" +
      }

      ret
    }
    catch
    {
      case e: IOException => {
        e.printStackTrace()
        ""
      }
      case e: StringIndexOutOfBoundsException => {
        println("Caught SIOOBE url: <" + url + ">")
        ""
      }
      case e => {
        e.printStackTrace()
        ""
      }
    }
  }
}