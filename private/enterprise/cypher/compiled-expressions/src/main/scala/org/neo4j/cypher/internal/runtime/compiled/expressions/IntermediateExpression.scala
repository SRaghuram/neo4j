/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions

import org.neo4j.codegen.api.{Field, IntermediateRepresentation, LocalVariable}

case class IntermediateExpression(ir: IntermediateRepresentation, fields: Seq[Field],
                                  variables: Seq[LocalVariable], nullCheck: Set[IntermediateRepresentation])
