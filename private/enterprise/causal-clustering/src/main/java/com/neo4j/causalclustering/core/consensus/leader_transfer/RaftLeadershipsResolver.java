/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.identity.CoreServerIdentity;
import com.neo4j.dbms.database.ClusteredDatabaseContext;

import java.util.List;
import java.util.function.Supplier;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.NamedDatabaseId;

import static java.util.stream.Collectors.toList;

class RaftLeadershipsResolver implements Supplier<List<NamedDatabaseId>>
{
    private final DatabaseManager<ClusteredDatabaseContext> databaseManager;
    private CoreServerIdentity myIdentity;

    RaftLeadershipsResolver( DatabaseManager<ClusteredDatabaseContext> databaseManager, CoreServerIdentity myIdentity )
    {
        this.databaseManager = databaseManager;
        this.myIdentity = myIdentity;
    }

    List<NamedDatabaseId> myLeaderships()
    {
        return databaseManager.registeredDatabases().values().stream()
                              .filter( this::amLeader )
                              .map( ClusteredDatabaseContext::databaseId )
                              .collect( toList() );
    }

    @Override
    public List<NamedDatabaseId> get()
    {
        return myLeaderships();
    }

    private boolean amLeader( ClusteredDatabaseContext context )
    {
        return context.leaderLocator()
                      .flatMap( leaderLocator -> leaderLocator.getLeaderInfo()
                              .map( LeaderInfo::memberId )
                              .map( leaderId -> leaderId.equals( myIdentity.raftMemberId( context.databaseId() ) ) ) )
                      .orElse( false );
    }
}
