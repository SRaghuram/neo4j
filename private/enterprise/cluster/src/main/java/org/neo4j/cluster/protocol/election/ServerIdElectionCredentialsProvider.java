/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.election;

import java.net.URI;

import org.neo4j.cluster.BindingListener;

/**
 * ElectionCredentialsProvider that provides the server URI as credentials
 * for elections. Natural comparison of the URI string is used.
 */
public class ServerIdElectionCredentialsProvider
        implements ElectionCredentialsProvider, BindingListener
{
    private volatile URI me;

    @Override
    public void listeningAt( URI me )
    {
        this.me = me;
    }

    @Override
    public ElectionCredentials getCredentials( String role )
    {
        return new ServerIdElectionCredentials( me );
    }
}
