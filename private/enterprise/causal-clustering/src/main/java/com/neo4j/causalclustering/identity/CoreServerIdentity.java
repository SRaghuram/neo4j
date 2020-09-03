/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;

public interface CoreServerIdentity extends ServerIdentity
{
    RaftMemberId raftMemberId( DatabaseId databaseId );

    RaftMemberId raftMemberId( NamedDatabaseId databaseId );

    void createMemberId( NamedDatabaseId databaseId, RaftMemberId raftMemberId );

    RaftMemberId loadMemberId( NamedDatabaseId databaseId );
}
