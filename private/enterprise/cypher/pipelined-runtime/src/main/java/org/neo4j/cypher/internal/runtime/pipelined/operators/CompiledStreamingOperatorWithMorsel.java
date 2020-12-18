package org.neo4j.cypher.internal.runtime.pipelined.operators;

/**
 * This is used by {@link CompiledStreamingOperatorWithMorselTemplate} to generate an operator class that
 * extends the implementations in {@link _CompiledStreamingOperatorWithMorsel}
 * Having an empty Java class here gives better early compiler messages if something goes wrong with Java-Scala interoperability
 * than if we use the Scala trait directly in generated code.
 */
public abstract class CompiledStreamingOperatorWithMorsel implements _CompiledStreamingOperatorWithMorsel
{
}
