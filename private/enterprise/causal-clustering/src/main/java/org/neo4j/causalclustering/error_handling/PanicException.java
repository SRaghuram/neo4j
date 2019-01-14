/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.error_handling;

public class PanicException extends IllegalStateException
{
    public static final PanicException EXCEPTION = new PanicException();

    private PanicException()
    {
        super( "Cluster has panicked" );
    }
}
