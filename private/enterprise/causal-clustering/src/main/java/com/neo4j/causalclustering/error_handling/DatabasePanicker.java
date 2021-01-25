/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.error_handling;

/**
 * Provides a way to 'bake in' a database ID and pass a functional interface to panic sites that do not have access to the current database id.
 *
 * @deprecated Use Panicker instead and ensure that your components are aware of the database that they are operating on.
 */
@Deprecated
@FunctionalInterface
public interface DatabasePanicker
{
    void panic( DatabasePanicReason reason, Throwable error );
}
