/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.election;

/**
 * For clients who should never be elected as coordinators or masters, use this election credentials provider.
 *
 */
public class NotElectableElectionCredentialsProvider
    implements ElectionCredentialsProvider
{
    @Override
    public ElectionCredentials getCredentials( String role )
    {
        return new NotElectableElectionCredentials();
    }
}
