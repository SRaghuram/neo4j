/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.logging;

import com.neo4j.causalclustering.core.consensus.RaftMessages.RaftMessage;

import org.neo4j.kernel.database.NamedDatabaseId;

public interface RaftMessageLogger<MEMBER>
{
    void logOutbound( NamedDatabaseId databaseId, MEMBER me, RaftMessage message, MEMBER remote );

    void logInbound( NamedDatabaseId databaseId, MEMBER remote, RaftMessage message, MEMBER me );
}
