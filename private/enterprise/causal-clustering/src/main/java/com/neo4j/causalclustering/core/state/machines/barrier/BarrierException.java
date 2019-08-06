/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.barrier;

public class BarrierException extends Exception
{
    BarrierException( String message, Throwable cause )
    {
        super( message, cause );
    }

    BarrierException( String message )
    {
        super( message );
    }
}
