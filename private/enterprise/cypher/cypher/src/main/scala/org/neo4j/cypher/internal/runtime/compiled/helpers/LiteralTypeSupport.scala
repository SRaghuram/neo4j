/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.helpers

import org.neo4j.cypher.internal.compiler.helpers.IsList
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions.{AnyValueType, BoolType, CypherCodeGenType, ListReferenceType, LongType, ReferenceType, RepresentationType, ValueType}
import org.neo4j.cypher.internal.runtime.interpreted.IsMap
import org.neo4j.cypher.internal.util.symbols._

object LiteralTypeSupport {
  def deriveCypherType(obj: Any): CypherType = obj match {
    case _: String                          => CTString
    case _: Char                            => CTString
    case _: Integer|_:java.lang.Long|_:Int|_:Long|_:Short|_:Byte => CTInteger
    case _: Number                          => CTFloat
    case _: Boolean                         => CTBoolean
    case IsMap(_)                           => CTMap
    case IsList(coll) if coll.isEmpty       => CTList(CTAny)
    case IsList(coll)                       => CTList(coll.map(deriveCypherType).reduce(_ leastUpperBound _))
    case _                                  => CTAny
  }

  def deriveCodeGenType(obj: Any): CypherCodeGenType = deriveCodeGenType(deriveCypherType(obj))

  def deriveCodeGenType(ct: CypherType): CypherCodeGenType = ct match {
    case ListType(innerCt) => CypherCodeGenType(CTList(innerCt), ListReferenceType(toRepresentationType(innerCt)))
    case _ => CypherCodeGenType(ct, toRepresentationType(ct))
  }

  private def toRepresentationType(ct: CypherType): RepresentationType = ct match {
    case CTInteger => LongType
    case CTFloat => expressions.FloatType
    case CTBoolean => BoolType
    case CTNode => LongType
    case CTRelationship => LongType
    case _ => ValueType
  }

  def selectRepresentationType(ct: CypherType, reprTypes: Seq[RepresentationType]): RepresentationType =
    // TODO: Handle ListReferenceType(_)?
    reprTypes.reduce[RepresentationType]({
      case (ReferenceType, _) =>
        ReferenceType
      case (_, ReferenceType) =>
        ReferenceType
      case (AnyValueType, _) =>
        AnyValueType
      case (_, AnyValueType) =>
        AnyValueType
      case (ValueType, _) =>
        ValueType
      case (_, ValueType) =>
        ValueType
      case (LongType, LongType) =>
        ct match {
          case CTNode | CTRelationship | CTInteger =>
            LongType // All elements have the same
          case _ =>
            // We cannot mix longs from different value domains, so we have to fallback on ReferenceType
            // e.g. literal list of [node, relationship, node]
            ReferenceType
        }
      case (t1, t2) =>
        if (t1 != t2)
          toRepresentationType(ct)
        else
          t1
    })
}
