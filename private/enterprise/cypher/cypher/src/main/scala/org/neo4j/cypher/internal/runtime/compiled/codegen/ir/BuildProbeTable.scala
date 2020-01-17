/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir

import org.neo4j.cypher.internal.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.Variable
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.CountingJoinTableType
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.JoinTableType
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.LongToCountTable
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.LongToListTable
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.LongsToCountTable
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.LongsToListTable
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.RecordingJoinTableType
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.SimpleTupleDescriptor

sealed trait BuildProbeTable extends Instruction {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) =
    generator.allocateProbeTable(name, tableType)

  protected val name: String

  def joinData: JoinData
  def tableType: JoinTableType

  override protected def children = Seq.empty
}

object BuildProbeTable {

  def apply(id: String, name: String, nodes: IndexedSeq[Variable], valueSymbols: Map[String, Variable])(implicit context: CodeGenContext): BuildProbeTable = {
    if (valueSymbols.isEmpty) BuildCountingProbeTable(id, name, nodes)
    else BuildRecordingProbeTable(id, name, nodes, valueSymbols)
  }
}

case class BuildRecordingProbeTable(id: String, name: String, nodes: IndexedSeq[Variable], valueSymbols: Map[String, Variable])
                                   (implicit context: CodeGenContext)
  extends BuildProbeTable {

  override def body[E](generator: MethodStructure[E])(implicit ignored: CodeGenContext): Unit = {
    generator.trace(id, Some(this.getClass.getSimpleName)) { body =>
      val tuple = body.newTableValue(context.namer.newVarName(), tupleDescriptor)
      fieldToVarName.foreach {
        case (fieldName, localName) => body.putField(tupleDescriptor, tuple, fieldName, localName.incoming.name)
      }
      body.updateProbeTable(tupleDescriptor, name, tableType, keyVars = nodes.map(_.name), tuple)
    }
  }

  override protected def operatorId = Set(id)

  private val fieldToVarName = valueSymbols.map {
    case (variable, incoming) => (context.namer.newVarName(), VariableData(variable, incoming,  incoming.copy(name = context.namer.newVarName())))
  }

  private val varNameToField = fieldToVarName.map {
    case (fieldName, localName) => localName.outgoing.name -> fieldName
  }

  private val tupleDescriptor = SimpleTupleDescriptor(fieldToVarName.mapValues(c => c.outgoing.codeGenType))

  override val tableType: RecordingJoinTableType = if (nodes.size == 1) LongToListTable(tupleDescriptor, varNameToField)
  else LongsToListTable(tupleDescriptor, varNameToField)

  val joinData: JoinData = JoinData(fieldToVarName, name, tableType, id)
}

case class BuildCountingProbeTable(id: String, name: String, nodes: IndexedSeq[Variable]) extends BuildProbeTable {

  override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext) =
    generator.trace(id, Some(this.getClass.getSimpleName)) { body =>
      body.updateProbeTableCount(name, tableType, nodes.map(_.name))
    }

  override protected def operatorId = Set(id)

  override val tableType: CountingJoinTableType = if (nodes.size == 1) LongToCountTable else LongsToCountTable

  override def joinData = {
    JoinData(Map.empty, name, tableType, id)
  }
}
case class VariableData(variable: String, incoming: Variable, outgoing: Variable)
case class JoinData(vars: Map[String, VariableData], tableVar: String, tableType: JoinTableType, id: String)
