package me.dhazel.TabularTransform


import java.nio.file.Path



abstract class TabularTransform {
  def toIterator(file: Path, sheetNumber: Int): Iterator[List[String]]
}


