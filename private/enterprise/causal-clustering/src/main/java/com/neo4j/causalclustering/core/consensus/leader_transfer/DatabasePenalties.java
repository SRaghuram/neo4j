/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.identity.MemberId;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;

class DatabasePenalties
{
    private final Map<MemberId,ExpiringSet<DatabaseId>> memberSuspensions = new ConcurrentHashMap<>();
    private final long suspensionTime;
    private Clock clock;

    DatabasePenalties( long suspensionTime, TimeUnit timeUnit, Clock clock )
    {
        this.clock = clock;
        this.suspensionTime = timeUnit.toMillis( suspensionTime );
    }

    void issuePenalty( MemberId member, NamedDatabaseId namedDatabaseId )
    {
        memberSuspensions.compute( member, ( memberId, suspendedDatabases ) ->
        {
            if ( suspendedDatabases == null )
            {
                suspendedDatabases = new ExpiringSet<>( suspensionTime, clock );
            }
            suspendedDatabases.add( namedDatabaseId.databaseId() );
            return suspendedDatabases;
        } );
    }

    void clean()
    {
        for ( MemberId member : memberSuspensions.keySet() )
        {
            memberSuspensions.computeIfPresent( member, ( memberId, suspendedDatabases ) ->
            {
                if ( !suspendedDatabases.nonEmpty() )
                {
                    return null;
                }
                return suspendedDatabases;
            } );
        }
    }

    public boolean notSuspended( DatabaseId databaseId, MemberId member )
    {
        return !( memberSuspensions.containsKey( member ) && memberSuspensions.get( member ).contains( databaseId ) );
    }
}
