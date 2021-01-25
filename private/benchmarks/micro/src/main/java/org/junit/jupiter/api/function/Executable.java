/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package org.junit.jupiter.api.function;

// I just had to add this to circumvent TCK-API dependency on JUnit. Should really remove that dep in TCK-API.
@FunctionalInterface
public interface Executable
{
    void execute() throws Throwable;
}
