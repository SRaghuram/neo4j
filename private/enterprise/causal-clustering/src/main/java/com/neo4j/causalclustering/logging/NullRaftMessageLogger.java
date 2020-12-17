/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.logging;

import org.neo4j.kernel.database.NamedDatabaseId;

import static com.neo4j.causalclustering.core.consensus.RaftMessages.RaftMessage;

public class NullRaftMessageLogger<MEMBER> implements RaftMessageLogger<MEMBER>
{
    @Override
    public void logOutbound( NamedDatabaseId databaseId, MEMBER remote, RaftMessage message  )
    {
    }

    @Override
    public void logInbound( NamedDatabaseId databaseId, MEMBER remote, RaftMessage message )
    {
    }
}

