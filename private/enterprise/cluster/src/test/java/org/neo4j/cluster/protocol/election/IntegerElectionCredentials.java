/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.election;

public class IntegerElectionCredentials implements ElectionCredentials
{
    private final int credential;

    public  IntegerElectionCredentials( int credential )
    {
        this.credential = credential;
    }

    @Override
    public int compareTo( ElectionCredentials o )
    {
        return o instanceof IntegerElectionCredentials
               ? Integer.compare( credential, ((IntegerElectionCredentials) o).credential ) : 0;
    }
}
