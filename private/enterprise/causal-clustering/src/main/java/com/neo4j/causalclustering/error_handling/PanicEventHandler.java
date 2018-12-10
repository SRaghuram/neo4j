/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.error_handling;

@FunctionalInterface
public interface PanicEventHandler
{
    /**
     * Since resources may be very limited during a panic any implementation of this interface should be as simple as possible.
     * Don't use up any extra memory or create new threads.
     */
    void onPanic();
}
