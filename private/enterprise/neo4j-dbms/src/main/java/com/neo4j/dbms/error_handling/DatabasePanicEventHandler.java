/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.error_handling;

@FunctionalInterface
public interface DatabasePanicEventHandler
{
    /**
     * Since resources may be very limited during a panic any implementation of this interface should be as simple as possible.
     * Don't use up any extra memory or create new threads.
     * @param panic information about the cause and nature of the panic
     */
    void onPanic( DatabasePanicEvent panic );
}
