/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.error_handling;

@FunctionalInterface
public interface Panicker
{
    /**
     * Panics an individual database or the whole DBMS depending on the reason for the panic.
     *
     * @param panicEvent the reason for the database panic including the exception that caused us to trigger a database panic
     */
    void panic( PanicEvent panicEvent );

    interface Reason
    {
        String getDescription();
    }

    interface PanicEvent
    {
        Reason getReason();

        Throwable getCause();
    }
}
