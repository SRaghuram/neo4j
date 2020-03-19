/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.identity.MemberId;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;

class DatabasePenalties
{
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Map<MemberId,SuspendedDatabases> memberSuspensions = new HashMap<>();
    private final long suspensionTime;
    private Clock clock;

    DatabasePenalties( long suspensionTime, TimeUnit timeUnit, Clock clock )
    {
        this.clock = clock;
        this.suspensionTime = timeUnit.toMillis( suspensionTime );
    }

    void issuePenalty( MemberId member, NamedDatabaseId namedDatabaseId )
    {
        readWriteLock.writeLock().lock();
        try
        {
            memberSuspensions.compute( member, ( memberId, suspendedDatabases ) -> {
                if ( suspendedDatabases == null )
                {
                    suspendedDatabases = new SuspendedDatabases( suspensionTime, clock );
                }
                suspendedDatabases.suspendDatabase( namedDatabaseId );
                return suspendedDatabases;
            } );
        }
        finally
        {
            readWriteLock.writeLock().unlock();
        }
    }

    void clean()
    {
        readWriteLock.writeLock().lock();
        try
        {
            memberSuspensions.values().forEach( SuspendedDatabases::update );
            memberSuspensions.entrySet().removeIf( entry -> entry.getValue().isEmpty() );
        }
        finally
        {
            readWriteLock.writeLock().unlock();
        }
    }

    public Set<NamedDatabaseId> suspendedDatabases( MemberId memberId )
    {
        readWriteLock.readLock().lock();
        try
        {
            return memberSuspensions.containsKey( memberId ) ? memberSuspensions.get( memberId ).suspendedDatabases() : Set.of();
        }
        finally
        {
            readWriteLock.readLock().unlock();
        }
    }

    public boolean notSuspended( DatabaseId databaseId, MemberId member )
    {
        return suspendedDatabases( member ).stream().noneMatch( db -> db.databaseId().equals( databaseId ) );
    }

    private static class SuspendedDatabases
    {
        private final long suspensionTime;
        private final Clock clock;
        private final Map<NamedDatabaseId,Long> suspendedDatabases = new HashMap<>();

        private SuspendedDatabases( long suspensionTime, Clock clock )
        {
            this.suspensionTime = suspensionTime;
            this.clock = clock;
        }

        boolean isEmpty()
        {
            return suspendedDatabases.isEmpty();
        }

        void update()
        {
            suspendedDatabases.entrySet().removeIf( entry -> (entry.getValue() + suspensionTime) < clock.millis() );
        }

        void suspendDatabase( NamedDatabaseId namedDatabaseId )
        {
            var timeOfSuspension = clock.millis();
            suspendedDatabases.put( namedDatabaseId, timeOfSuspension );
        }

        public Set<NamedDatabaseId> suspendedDatabases()
        {
            update();
            return suspendedDatabases.keySet();
        }
    }
}
