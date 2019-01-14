/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.discovery;

public class NoRetriesStrategy extends RetryStrategy
{
    public NoRetriesStrategy()
    {
        super( 0, 0 );
    }
}
