/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric

import org.neo4j.collection.RawIterator
import org.neo4j.internal.kernel.api.exceptions.ProcedureException
import org.neo4j.internal.kernel.api.procs.FieldSignature.inputField
import org.neo4j.internal.kernel.api.procs.{Neo4jTypes, ProcedureSignature, QualifiedName, UserFunctionSignature}
import org.neo4j.kernel.api.ResourceTracker
import org.neo4j.kernel.api.procedure.{CallableProcedure, CallableUserFunction, Context}
import org.neo4j.procedure.Mode
import org.neo4j.procedure.impl.GlobalProceduresRegistry
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

trait ProcedureRegistryTestSupport {


  def userFunction(name: String, args: Seq[String])(body: => AnyValue): CallableUserFunction.BasicUserFunction =
    new CallableUserFunction.BasicUserFunction(
      new UserFunctionSignature(
        new QualifiedName(Array[String](), name),
        ListBuffer(args: _*).map(inputField(_, Neo4jTypes.NTAny)).asJava,
        Neo4jTypes.NTAny,
        null, Array[String](), name, false
      )
    ) {
      override def apply(ctx: Context, input: Array[AnyValue]): AnyValue = body
    }

  private def procedure(name: String, args: Seq[String], out: Seq[String])(values: => Seq[Array[AnyValue]]): CallableProcedure =
    new CallableProcedure.BasicProcedure(new ProcedureSignature(
      new QualifiedName(Array[String](), name),
      ListBuffer(args: _*).map(inputField(_, Neo4jTypes.NTAny)).asJava,
      ListBuffer(out: _*).map(inputField(_, Neo4jTypes.NTAny)).asJava,
      Mode.DEFAULT, false, null, Array[String](), name, null, false, false, false
    )) {
      override def apply(ctx: Context, input: Array[AnyValue], resourceTracker: ResourceTracker): RawIterator[Array[AnyValue], ProcedureException] =
        RawIterator.of(values: _*)
    }

  val procedures: GlobalProceduresRegistry = {
    val reg = new GlobalProceduresRegistry()
    reg.register(userFunction("const0", Seq())(Values.intValue(0)))
    reg.register(userFunction("const1", Seq())(Values.intValue(1)))
    reg.register(procedure("myProcedure", Seq(), Seq("a", "b"))(Seq(Array(Values.intValue(1), Values.intValue(10)))))
    reg
  }
}
