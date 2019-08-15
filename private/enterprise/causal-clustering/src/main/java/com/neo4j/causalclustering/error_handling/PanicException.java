/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.error_handling;

class PanicException extends IllegalStateException
{
    static final String MESSAGE = "Clustering components have encountered a critical error";

    static final PanicException EXCEPTION = new PanicException();

    private PanicException()
    {
        super( MESSAGE );
    }
}
