/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.election;

import java.io.Serializable;
import java.net.URI;

public class ServerIdElectionCredentials implements ElectionCredentials, Serializable
{
    private URI credentials;

    public ServerIdElectionCredentials( URI credentials )
    {
        this.credentials = credentials;
    }

    @Override
    public int compareTo( ElectionCredentials o )
    {
        // Alphabetically lower URI means higher prio
        return -credentials.toString().compareTo( ((ServerIdElectionCredentials) o).credentials.toString() );
    }
}
