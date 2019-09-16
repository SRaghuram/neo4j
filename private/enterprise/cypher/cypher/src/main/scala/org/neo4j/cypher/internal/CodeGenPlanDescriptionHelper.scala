/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import org.neo4j.codegen.api.CodeGeneration.CodeSaver
import org.neo4j.cypher.internal.plandescription.Argument
import org.neo4j.cypher.internal.plandescription.Arguments.{ByteCode, SourceCode}

object CodeGenPlanDescriptionHelper {
  def metadata(saver: CodeSaver): Seq[Argument] =
    saver.sourceCode.map {
      case (className, sourceCode) => SourceCode(className, sourceCode)
    } ++ saver.bytecode.map {
      case (className, byteCode) => ByteCode(className, byteCode)
    }
}
