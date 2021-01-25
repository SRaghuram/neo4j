/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning.ast

import org.neo4j.cypher.internal.runtime.ast.BooleanRuntimeExpression

case class HasLabelsFromSlot(offset: Int, resolvedLabelTokens: Seq[Int], lateLabels: Seq[String]) extends BooleanRuntimeExpression
