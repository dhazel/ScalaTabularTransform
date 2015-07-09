import org.scalatest._
import org.scalatest.matchers.ShouldMatchers

import java.io.{File => IOFile}
import java.io.FileInputStream
import java.io.InputStream
import java.io.FileReader

import java.nio.file.Paths
import java.nio.file.Path

import scala.util.Try
import scala.util.Success
import scala.util.parsing.input.StreamReader

import java.net.URL

import me.dhazel.TabularTransform._



class FileProcessingSpec extends FlatSpec
with ShouldMatchers with FileFactory {
  val xls = new Xls
  val xlsx = new Xlsx


  "Xls" should "extract rows and columns" in {
    val rows = xls.toIterator(getPath("TestFile1.xls"))
    rows.next should be (
      List("this","is ","a ","test ","file"))
    rows.next should be (
      List("with","a","few","lines","in","it"))
  }

  "Xlsx" should "extract rows and columns" in {
    val rows = xlsx.toIterator(getPath("TestFile1.xlsx"))
    rows.next should be (
      List("this","is ","a ","test ","file"))
    rows.next should be (
      List("with","a","few","lines","in","it"))
  }

}


trait FileFactory {
  // Pulls a file from the test resources
  def getPath(fileName: String): Path = {
    Paths.get(getClass.getResource(fileName).toURI)
  }
}

