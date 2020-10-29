/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.member;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.util.Map;
import java.util.Set;

import org.neo4j.dbms.DatabaseState;
import org.neo4j.kernel.database.DatabaseId;

/**
 * Immutable snapshot of information about this discovery member
 */
public interface DiscoveryMember
{
    Map<DatabaseId,LeaderInfo> databaseLeaderships();

    Set<DatabaseId> discoverableDatabases();

    Map<DatabaseId,DatabaseState> databaseStates();

    Map<DatabaseId,RaftMemberId> databaseMemberships();
}
