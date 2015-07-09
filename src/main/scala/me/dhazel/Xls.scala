package me.dhazel.TabularTransform



import scala.util.Try
import scala.util.Success
import scala.util.Failure

import java.nio.file.Path

import org.apache.poi.poifs.filesystem.DocumentInputStream
import org.apache.poi.poifs.filesystem.NPOIFSFileSystem
import org.apache.poi.hssf.eventusermodel.HSSFRequest
import org.apache.poi.hssf.eventusermodel.HSSFListener
import org.apache.poi.hssf.eventusermodel.MissingRecordAwareHSSFListener
import org.apache.poi.hssf.eventusermodel.FormatTrackingHSSFListener
import org.apache.poi.hssf.record.RecordFactoryInputStream

import org.apache.poi.hssf.record.BOFRecord
import org.apache.poi.hssf.record.BlankRecord
import org.apache.poi.hssf.record.BoolErrRecord
import org.apache.poi.hssf.record.BoundSheetRecord
import org.apache.poi.hssf.record.FormulaRecord
import org.apache.poi.hssf.record.LabelRecord
import org.apache.poi.hssf.record.LabelSSTRecord
import org.apache.poi.hssf.record.NoteRecord
import org.apache.poi.hssf.record.NumberRecord
import org.apache.poi.hssf.record.RKRecord
import org.apache.poi.hssf.record.Record
import org.apache.poi.hssf.record.SSTRecord
import org.apache.poi.hssf.record.StringRecord
import org.apache.poi.hssf.eventusermodel.dummyrecord.MissingCellDummyRecord



class Xls extends TabularTransform {
  def toIterator(xls: Path, sheetNumber: Int = 0): Iterator[List[String]] = {
    // Makes a list of arbitrary length and fill
    def makeList[T](length: Int, fill: T): List[T] = {
      if (length <= 0) List()
      else fill :: makeList(length - 1, fill)
    }

    type RecordStream = Stream[Record]

    val fs = new NPOIFSFileSystem(xls.toFile)

    object BaseHSSFListener extends HSSFListener {
      def processRecord(record: Record): Unit = 0
    }

    val listener = new MissingRecordAwareHSSFListener(BaseHSSFListener)
    val formatListener = new FormatTrackingHSSFListener(listener)

    val request = new HSSFRequest

    request.addListenerForAllRecords(formatListener)

    // Normally the workbook will be called "Workbook", but some weird Xls
    //  generators use "WORKBOOK"
    val workbookNames = List("Workbook", "WORKBOOK")

    if ( fs.getRoot.hasEntry("Book") ) throw new Exception(
      "Excel 5.0/7.0/95 (BIFF5) file detected, UNSUPPORTED")

    // pulls the workbook from the list of names, else uses "UnknownWorkbook",
    //  which is helpful for interpreting the error log if none are found
    def getDocumentStream: DocumentInputStream = fs.getRoot.createDocumentInputStream(
      (workbookNames.filter(fs.getRoot.hasEntry(_)) ::: List("UnknownWorkbook"))(0))

    // Not storing a stream ensures that the GC can clean out head elements
    //  after use. See "records" below
    //val recordStream = new RecordFactoryInputStream(in, false)

    def getRecords(inStream: DocumentInputStream): RecordStream = {
      def getRecordsIter(inStream: RecordFactoryInputStream): RecordStream = {
        val next = inStream.nextRecord
        if (next == null) Stream()
        else next #:: getRecordsIter(inStream)
      }
      getRecordsIter(new RecordFactoryInputStream(inStream, false))
    }

    def getSstRecord(records: RecordStream): List[SSTRecord] = {
      if (records.isEmpty) List()
      else if (records.head.getSid == SSTRecord.sid) {
        List(records.head.asInstanceOf[SSTRecord])
      }
      else getSstRecord(records.tail)
    }

    case class Sheet(
      val recordStream: RecordStream,
      val name: String,
      val hidden: Boolean)

    case class SheetMeta(
      val name: String,
      val hidden: Boolean,
      val bofOffset: Int)

    def getSheets(records: RecordStream, metaRecords: List[SheetMeta])
      : Stream[Sheet] = {
        def isWorksheetRecord(record: Record): Boolean = record.getSid match {
          case BOFRecord.sid => {
            record.asInstanceOf[BOFRecord].getType == BOFRecord.TYPE_WORKSHEET
          }
          case _ => false
        }
        def dropToStartingRecord(records: RecordStream): RecordStream = {
          if (records.isEmpty) Stream()
          else if (isWorksheetRecord(records.head)) records.tail
          else dropToStartingRecord(records.tail)
        }
        def getSheet(records: RecordStream): RecordStream = {
          if (records.isEmpty) Stream()
          else if (isWorksheetRecord(records.head)) Stream()
          else records.head #:: getSheet(records.tail)
        }
        def recordIter(records: RecordStream, meta: List[SheetMeta])
          : Stream[Sheet] = {
            if (meta.isEmpty || records.isEmpty) Stream()
            else Sheet(getSheet(records), meta.head.name, meta.head.hidden) #::
              recordIter(dropToStartingRecord(records), meta.tail)
        }
        recordIter(dropToStartingRecord(records), metaRecords)
    }

    def getSheetMetaRecords(records: RecordStream): List[SheetMeta] = {
      def isMetaWorksheetRecord(record: Record): Boolean = record.getSid match {
        case BoundSheetRecord.sid => true
        case _ => false
      }
      // order the records so they will match what we encounter in the file
      (for {
        r0 <- records
        if ( isMetaWorksheetRecord(r0) )
        record = r0.asInstanceOf[BoundSheetRecord]
      } yield {
        SheetMeta(
          record.getSheetname,
          (record.isHidden || record.isVeryHidden),
          record.getPositionOfBof)
      }).toList.sortWith(_.bofOffset < _.bofOffset)
    }

    case class RecordValue(
      val row: Int,
      val column: Int,
      val value: String)

    def getRows(sheet: RecordStream, sstRecordContainer: List[SSTRecord]): Stream[List[String]] = {
      def isDataRecord(record: Record): Boolean = record.getSid match {
        case BlankRecord.sid => true
        case BoolErrRecord.sid => true
        //case FormulaRecord.sid => true    // formulas are ignored
        //case StringRecord.sid => true     // formulas are ignored
        case LabelRecord.sid => true
        case LabelSSTRecord.sid => true
        case NoteRecord.sid => true
        case NumberRecord.sid => true
        case RKRecord.sid => true
        case _ => {
          if (record.isInstanceOf[MissingCellDummyRecord]) true
          //if (record.isInstanceOf[LastCellOfRowDummyRecord]) true // ignored
          else false
        }
      }

      def getRecordValue(record: Record): RecordValue = record.getSid match {
        case BlankRecord.sid => {
          val brec = record.asInstanceOf[BlankRecord]
          RecordValue(brec.getRow, brec.getColumn, "")
        }
        case BoolErrRecord.sid => {
          val berec = record.asInstanceOf[BoolErrRecord]
          RecordValue(berec.getRow, berec.getColumn, "")
        }
        case LabelRecord.sid => {
          val lrec = record.asInstanceOf[LabelRecord]
          RecordValue(lrec.getRow, lrec.getColumn, lrec.getValue.toString)
        }
        case LabelSSTRecord.sid => {
          val lsrec = record.asInstanceOf[LabelSSTRecord]

          if(sstRecordContainer.isEmpty) { // TODO this will need to be fixed later
            RecordValue(
              lsrec.getRow,
              lsrec.getColumn,
              "(No SST Record, can't identify string)")
          } else {
            RecordValue(
              lsrec.getRow,
              lsrec.getColumn,
              sstRecordContainer(0).getString(lsrec.getSSTIndex).toString)
          }
        }
        case NoteRecord.sid => {
          val nrec = record.asInstanceOf[NoteRecord]
          RecordValue(nrec.getRow, nrec.getColumn, "(TODO)")
        }
        case NumberRecord.sid => {
          val numrec = record.asInstanceOf[NumberRecord]
          val number = numrec.getValue
          if ( (number % 1.0) == 0 ) {
            RecordValue(numrec.getRow, numrec.getColumn, number.toLong.toString)
          }
          else {
            RecordValue(numrec.getRow, numrec.getColumn, number.toString)
          }
        }
        case RKRecord.sid => {
          val rkrec = record.asInstanceOf[RKRecord]
          RecordValue(rkrec.getRow, rkrec.getColumn, "(TODO)")
        }
        case _ => {
          if (record.isInstanceOf[MissingCellDummyRecord]) {
            val mc = record.asInstanceOf[MissingCellDummyRecord]
            RecordValue(mc.getRow, mc.getColumn, "")
          }
          else throw new Exception("Unexpected record type: " + record)
        }
      }

      def getRecordValues(recordStream: RecordStream): Stream[RecordValue] = {
        if (recordStream.isEmpty) Stream()
        else getRecordValue(recordStream.head) #:: getRecordValues(recordStream.tail)
      }

      def getFilledRow(row: Stream[RecordValue]): List[String] = {
        def getFilledRowIter(nextPosition: Int, row: Stream[RecordValue]): List[String] = {
          if (row.isEmpty) List()
          else if (row.head.column > nextPosition) {
            makeList(row.head.column - nextPosition, "") :::
              getFilledRowIter(row.head.column, row)
          } else row.head.value :: getFilledRowIter(nextPosition + 1, row.tail)
        }

        if (row.isEmpty) List()
        else if (row.head.column != 0) {
          makeList(row.head.column, "") ::: getFilledRowIter(row.head.column, row)
        } else getFilledRowIter(row.head.column, row)
      }

      (for {
        record <- getRecordValues(sheet.filter(isDataRecord(_)))
        // TODO: toStream below is covering some poor memory usage
      } yield record).groupBy(_.row).toStream.sortBy(_._1).map(a => getFilledRow(a._2))
    }

    val sheets = getSheets(
      getRecords(getDocumentStream),
      getSheetMetaRecords(getRecords(getDocumentStream))
    ).filter( sheet => {
      // Automatically remove sheets that we do not consider data.
      // Hidden sheets are uncommon, but they have been known to contain data.
      //(! sheet.hidden) && (sheet.name != "QuickBooks Export Tips")
      (sheet.name != "QuickBooks Export Tips")
    })

    getRows(
      sheets(sheetNumber).recordStream,
      getSstRecord(getRecords(getDocumentStream))).toIterator
  }
}
