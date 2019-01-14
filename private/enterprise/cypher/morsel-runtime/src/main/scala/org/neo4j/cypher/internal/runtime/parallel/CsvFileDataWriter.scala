/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.parallel

import java.io.{BufferedWriter, File, FileWriter}

/**
  * DataPointWriter which accepts DataPoints, formats as CSV, and prints to file.
  */
class CsvFileDataWriter(file: File) extends CsvDataWriter {

  private val out = new BufferedWriter(new FileWriter(file))
  writeRow(header)

  def flush(): Unit = out.flush()

  override def writeRow(row: String): Unit =
    out.write(row)
}
