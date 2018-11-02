/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.replication;

public class ReplicationFailureException extends Exception
{
    // needs to be public due to reflection
    @SuppressWarnings( "WeakerAccess" )
    public ReplicationFailureException( String message, Throwable cause )
    {
        super( message, cause );
    }

    ReplicationFailureException( String message )
    {
        super( message );
    }
}
