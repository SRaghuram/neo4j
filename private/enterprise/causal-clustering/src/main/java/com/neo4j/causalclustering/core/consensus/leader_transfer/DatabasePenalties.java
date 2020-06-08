/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.identity.MemberId;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;

class DatabasePenalties
{
    private final Map<MemberId,ExpiringSet<DatabaseId>> memberSuspensions = new ConcurrentHashMap<>();
    private final Duration suspensionTime;
    private final Clock clock;

    DatabasePenalties( Duration suspensionTime, Clock clock )
    {
        this.clock = clock;
        this.suspensionTime = suspensionTime;
    }

    void issuePenalty( MemberId member, NamedDatabaseId namedDatabaseId )
    {
        memberSuspensions.compute( member, ( memberId, suspendedDatabases ) ->
        {
            suspendedDatabases = suspendedDatabases == null ? new ExpiringSet<>( suspensionTime, clock ) : suspendedDatabases;
            suspendedDatabases.add( namedDatabaseId.databaseId() );
            return suspendedDatabases;
        } );
        dropEmptyMembers();
    }

    private void dropEmptyMembers()
    {
        for ( MemberId member : memberSuspensions.keySet() )
        {
            memberSuspensions.computeIfPresent( member, ( memberId, suspendedDatabases ) -> suspendedDatabases.isEmpty() ? null : suspendedDatabases );
        }
    }

    public boolean notSuspended( DatabaseId databaseId, MemberId member )
    {
        var suspendedDatabases = memberSuspensions.get( member );
        return !(suspendedDatabases != null && suspendedDatabases.contains( databaseId ));
    }
}
