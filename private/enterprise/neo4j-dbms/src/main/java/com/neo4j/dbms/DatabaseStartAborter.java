/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.dbms.OperatorState;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseStartupController;
import org.neo4j.kernel.database.NamedDatabaseId;

import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

/**
 * Component which polls the system database to see if a given database should still be started.
 * This utility is not used to STOP a database under normal circumstances. That is still handled by
 * the {@link DbmsReconciler}. Instead it is used for bailing out of blocking logic taking place
 * during e.g. {@link Database#start()}.
 */
public class DatabaseStartAborter implements DatabaseStartupController
{
    private final Duration ttl;
    private final Map<NamedDatabaseId,CachedDatabaseState> databaseStates;
    private final AvailabilityGuard globalAvailabilityGuard;
    private final EnterpriseSystemGraphDbmsModel dbmsModel;
    private final Clock clock;

    public DatabaseStartAborter( AvailabilityGuard globalAvailabilityGuard, EnterpriseSystemGraphDbmsModel dbmsModel, Clock clock, Duration ttl )
    {
        this.databaseStates = new ConcurrentHashMap<>();
        this.globalAvailabilityGuard = globalAvailabilityGuard;
        this.dbmsModel = dbmsModel;
        this.clock = clock;
        this.ttl = ttl;
    }

    /**
     * Checks the desired state of the given database against the system database and the global availability guard, to see if the start currently being
     * executed should be aborted. The results of these checks are cached for the duration of the ttl, to avoid spamming the system database with read
     * queries when starting many databases.
     *
     * Note that for the system database only the global availability guard is checked. It is assumed that if you wish to fully stop the system database
     * you must also stop the neo4j process.
     *
     * @param namedDatabaseId the database whose desired state we should check in the system db.
     * @return whether the database start should be aborted.
     */
    @Override
    public boolean shouldAbort( NamedDatabaseId namedDatabaseId )
    {
        if ( globalAvailabilityGuard.isShutdown() )
        {
            return true;
        }
        else if ( Objects.equals( namedDatabaseId, NAMED_SYSTEM_DATABASE_ID ) )
        {
            return false;
        }

        var cached = databaseStates.compute( namedDatabaseId, ( id, cachedState ) ->
        {
            if ( cachedState == null || cachedState.isTimeToDie() )
            {
                return getFreshState( namedDatabaseId );
            }

            return cachedState;
        } );

        return cached.state() == EnterpriseOperatorState.STOPPED || cached.state() == EnterpriseOperatorState.DROPPED;
    }

    /**
     * When a database is eventually started, it should be removed from the Aborter's cache
     */
    public void started( NamedDatabaseId namedDatabaseId )
    {
        databaseStates.remove( namedDatabaseId );
    }

    private CachedDatabaseState getFreshState( NamedDatabaseId namedDatabaseId )
    {
        var message = String.format( "Failed to check if starting %s should abort as it doesn't exist in the system db!", namedDatabaseId );
        var state = dbmsModel.getStatus( namedDatabaseId ).orElseThrow( () -> new IllegalStateException( message ) );
        return new CachedDatabaseState( clock.instant(), state );
    }

    private class CachedDatabaseState
    {
        private final OperatorState state;
        private final Instant createdAt;

        private CachedDatabaseState( Instant createdAt, OperatorState state )
        {
            this.state = state;
            this.createdAt = createdAt;
        }

        OperatorState state()
        {
            return state;
        }

        boolean isTimeToDie()
        {
            var elapsed = Duration.between( this.createdAt, clock.instant() );
            return elapsed.compareTo( ttl ) >= 0;
        }
    }
}
