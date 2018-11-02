/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.election;

/**
 * Implementations of this provide the credential for the local node to be used in elections.
 */
public interface ElectionCredentialsProvider
{
    ElectionCredentials getCredentials( String role );
}
