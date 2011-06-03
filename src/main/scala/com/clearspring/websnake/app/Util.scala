package com.clearspring.websnake.app

import java.nio.charset.{Charset, CharsetEncoder}
import util.matching.Regex

object Util {
  val asciiEncoder: CharsetEncoder = Charset.forName("ISO-8859-1").newEncoder()
  val stopWords = scala.io.Source.fromFile("resources/stopwords.txt").mkString.split('\n')

  var stopWordsRegex = "(?i)"
  for (stopWord <- stopWords) {
    stopWordsRegex += stopWord + "|"
  }
  stopWordsRegex = stopWordsRegex.substring(0, stopWordsRegex.length - 1)

  def stripStopWords(input: String) = {
    var replaced = input
    for (stopWord <- stopWords) {
      replaced = replaced.replaceAll("(?i)" + stopWord, "")
    }
    replaced
  }

  def isAscii(testString: String) = {
    try {
      asciiEncoder.canEncode(testString)
    } catch {
      case e: IllegalStateException => false
    }
  }
}