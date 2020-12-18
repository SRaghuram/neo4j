/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators;

/**
 * This is used by {@link CompiledStreamingOperatorWithAccumulatorsTemplate} to generate an operator class that
 * extends the implementations in {@link _CompiledStreamingOperatorWithAccumulators}
 * Having an empty Java class here gives better early compiler messages if something goes wrong with Java-Scala interoperability
 * than if we use the Scala trait directly in generated code.
 */
public abstract class CompiledStreamingOperatorWithAccumulators implements _CompiledStreamingOperatorWithAccumulators
{
}
