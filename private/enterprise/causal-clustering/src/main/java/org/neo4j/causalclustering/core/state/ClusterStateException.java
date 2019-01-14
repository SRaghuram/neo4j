/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state;

public class ClusterStateException extends Exception
{
    ClusterStateException( String message )
    {
        super( message );
    }

    ClusterStateException( Throwable cause )
    {
        super( cause );
    }
}
