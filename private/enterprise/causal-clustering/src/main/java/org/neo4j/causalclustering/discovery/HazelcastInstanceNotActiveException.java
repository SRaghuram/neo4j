/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.discovery;

/**
 * A checked exception to make exceptions visible.
 */
class HazelcastInstanceNotActiveException extends Exception
{
    HazelcastInstanceNotActiveException( String message )
    {
        super( message );
    }

    HazelcastInstanceNotActiveException( Throwable cause )
    {
        super( cause );
    }
}
