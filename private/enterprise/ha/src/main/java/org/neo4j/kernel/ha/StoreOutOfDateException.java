/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha;

/**
 * Thrown to point out that branching of data has occured for one or
 * more instances in a cluster. Branching is when one machine has
 * different (not meaning outdated) than the current master.
 *
 * @author Mattias Persson
 */
public class StoreOutOfDateException extends StoreUnableToParticipateInClusterException
{
    public StoreOutOfDateException()
    {
        super();
    }

    public StoreOutOfDateException( String message, Throwable cause )
    {
        super( message, cause );
    }
}
