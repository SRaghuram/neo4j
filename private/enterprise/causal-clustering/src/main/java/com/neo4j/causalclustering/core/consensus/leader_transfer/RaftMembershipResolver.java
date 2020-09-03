/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.identity.RaftMemberId;

import java.util.Set;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;

interface RaftMembershipResolver
{
    Set<ServerId> votingServers( NamedDatabaseId databaseId );

    RaftMemberId resolveRaftMemberForServer( NamedDatabaseId databaseId, ServerId to );

    ServerId resolveServerForRaftMember( RaftMemberId memberId );
}
