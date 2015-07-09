package me.dhazel.TabularTransform



import java.nio.file.Path
import java.nio.file.Files
import java.nio.file.StandardCopyOption.REPLACE_EXISTING

import java.io.InputStream
import java.util.{Iterator => JIterator}

import org.apache.poi.xssf.eventusermodel.XSSFReader
import org.apache.poi.xssf.eventusermodel.ReadOnlySharedStringsTable
import org.apache.poi.xssf.model.SharedStringsTable
import org.apache.poi.xssf.usermodel.XSSFRichTextString
import org.apache.poi.openxml4j.opc.OPCPackage

import org.xml.sax.Attributes
import org.xml.sax.ContentHandler
import org.xml.sax.InputSource
import org.xml.sax.SAXException
import org.xml.sax.XMLReader
import org.xml.sax.helpers.DefaultHandler
import org.xml.sax.helpers.XMLReaderFactory

import scala.io.Source

import scala.xml.pull.XMLEventReader
import scala.xml.pull.EvEntityRef
import scala.xml.pull.EvElemStart
import scala.xml.pull.EvElemEnd
import scala.xml.pull.EvText
import scala.xml.EntityRef



class Xlsx extends TabularTransform {
  def toIterator(xlsx: Path, sheetNumber: Int = 0): Iterator[List[String]] = {
    val pkg = OPCPackage.open(xlsx.toFile)

    // Map of alphabetic characters to their sequence number
    val alphaMap = ('A' to 'Z').zip(0 to 25).toMap

    // Makes a list of arbitrary length and fill
    def makeList[T](length: Int, fill: T): List[T] = {
      if (length <= 0) List()
      else fill :: makeList(length - 1, fill)
    }

    def getSheets(reader: XSSFReader): Stream[InputStream] = {
      val sheets = reader.getSheetsData
      def sheetStream(sheetsIter: JIterator[InputStream]): Stream[InputStream] = {
        if ( ! sheetsIter.hasNext ) Stream()
        else sheetsIter.next #:: sheetStream(sheetsIter)
      }
      sheetStream(sheets)
    }

    // See http://svn.apache.org/repos/asf/poi/trunk/src/examples/src/org/apache/poi/xssf/eventusermodel/XLSX2CSV.java
    //  for the example from which this function was built.
    def getSheetRows(sheet: InputStream, sst: ReadOnlySharedStringsTable): Iterator[List[String]] = {
      // Retrieves strings by index from the Shared Strings Table
      def getSstText(index: Int): String = {
        (new XSSFRichTextString(sst.getEntryAt(index))).toString
      }

      class CellType
      case object NumericCell extends CellType // the default type if none is specified
      case object SstCell extends CellType
      case object BooleanCell extends CellType
      case object InlineStringCell extends CellType
      case object ErrorCell extends CellType
      case object FormulaCell extends CellType

      def getRows(reader: XMLEventReader): Stream[List[String]] = {
        def getRow(
          columnAcc: List[Char],
          rowAcc: List[String],
          cellType: CellType,
          reader: XMLEventReader)
        : (List[String], XMLEventReader) = {
          if ( ! reader.hasNext ) (rowAcc, reader)
          else reader.next match {
            case EvElemStart(_, label, attributes, _) => label match {
              case "c" => {
                val cell = attributes("r").toString
                val column = cell(0)
                val cellTypeIndicator = if ( attributes("t") == null ) "none"
                  else attributes("t").toString
                val cellType = cellTypeIndicator match {
                  case "b"         => BooleanCell
                  case "e"         => ErrorCell
                  case "inlineStr" => InlineStringCell
                  case "s"         => SstCell
                  case "str"       => FormulaCell
                  case _           => NumericCell
                }
                if ( columnAcc.isEmpty ) {
                  getRow( // new row, fill in any empty leading columns
                    List(column),
                    rowAcc ++ makeList(alphaMap(column), ""),
                    cellType,
                    reader)
                }
                else {
                  getRow( // fill in any skipped columns
                    List(column),
                    rowAcc ++ makeList((alphaMap(column) - alphaMap(columnAcc(0)) - 1), ""),
                    cellType,
                    reader)
                }
              }
              case "v" => getRow(
                columnAcc,
                rowAcc :+ getValue("", cellType, reader),
                cellType,
                reader)
              case _ => getRow(columnAcc, rowAcc, cellType, reader)
            }
            case EvElemEnd(_, label) => label match {
              case "row" => (rowAcc, reader)
              case "c" => getRow( // if column was present but empty, fill it in
                columnAcc,
                rowAcc ++ makeList((alphaMap(columnAcc(0)) + 1 - rowAcc.length), ""),
                cellType,
                reader)
              case "v" => throw new Exception("Value node ended outside of 'getValue'. This should never happen")
              case _     => getRow(columnAcc, rowAcc, cellType, reader)
            }
            case _ => getRow(columnAcc, rowAcc, cellType, reader)
          }
        }
        def getValue(
          valueAcc: String,
          cellType: CellType,
          reader: XMLEventReader)
        : String = {
          def addElem(acc: String): String = getValue(acc, cellType, reader)
          def oneElem(acc: String, note: String = ""): String = {
            if ( valueAcc != "" ) throw new Exception(s"$note : Single value expected, multiple values found")
            else addElem(acc)
          }

          if ( ! reader.hasNext ) throw new Exception("Unexpected reader termination")
          else reader.next match {
            case EvElemEnd(_, label) => label match {
              case "v" => valueAcc
              case _ => throw new Exception(s"Unexpected node ($label) nested in a value node")
            }
            case EvText(text) => {
              cellType match {
                case NumericCell      => oneElem(text.toString, "NumericCell")
                case SstCell          => oneElem(getSstText(text.toInt), "SstCell")
                case BooleanCell      => oneElem((if(text.charAt(0) == '0') "False" else "True"), "BooleanCell")
                case InlineStringCell => oneElem(new XSSFRichTextString(text.toString).toString, "InlineStringCell")
                case ErrorCell        => oneElem("Error: " + text.toString, "ErrorCell")
                case FormulaCell      => addElem(valueAcc + text.toString)
                case _                => oneElem("Unknown Datatype")
              }
            }
            case EvEntityRef(entity) => {
              addElem(valueAcc + (EntityRef(entity)).text)
            }
            case _ => throw new Exception("Unexpected value sub-element")
          }
        }
        val (row, reader1) = getRow(List(), List(), NumericCell, reader)
        if ( row.isEmpty ) Stream()
        else row #:: getRows(reader1)
      }
      getRows(new XMLEventReader(Source.fromInputStream(sheet))).toIterator
    }

    val sheets = getSheets(new XSSFReader(pkg))

    getSheetRows(sheets(sheetNumber), new ReadOnlySharedStringsTable(pkg))
  }
}
