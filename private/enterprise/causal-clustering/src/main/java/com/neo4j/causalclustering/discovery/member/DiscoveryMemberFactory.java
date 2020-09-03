/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.member;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;

import java.util.Map;

import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.kernel.database.DatabaseId;

@FunctionalInterface
public interface DiscoveryMemberFactory
{
    DiscoveryMember createSnapshot( DatabaseStateService databaseStateService, Map<DatabaseId,LeaderInfo> databaseLeaderships );
}
