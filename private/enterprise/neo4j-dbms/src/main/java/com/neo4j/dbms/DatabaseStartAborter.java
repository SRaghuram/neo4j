/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.dbms.database.EnterpriseDatabase;
import com.neo4j.dbms.error_handling.DatabasePanicEvent;
import com.neo4j.dbms.error_handling.DatabasePanicEventHandler;
import org.eclipse.collections.api.multimap.set.MutableSetMultimap;
import org.eclipse.collections.impl.factory.Multimaps;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.dbms.OperatorState;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.database.DatabaseStartupController;
import org.neo4j.kernel.database.NamedDatabaseId;

import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

/**
 * This utility is not used to STOP a database under normal circumstances. That is still handled by
 * the {@link DbmsReconciler}. Instead it is used for bailing out of blocking logic taking place
 * during e.g. {@link EnterpriseDatabase#start()}.
 *
 * There are two classes of abort:
 *
 * - An abort caused by a change to a Database's state in the system database. I.e. a user has changed
 *   the desired state of the database from {@code online} to {@code offline} whilst the database is
 *   still starting. This is known as a user abort
 * - An abort caused by a {@link DatabasePanicEvent}. This is known as a system abort
 *
 * The first class of aborts may be prevented/ignored under certain circumstances.
 * Aborts due to panics are *never* ignored.
 */
public class DatabaseStartAborter implements DatabaseStartupController, DatabasePanicEventHandler
{
    public enum PreventReason
    {
        STORE_COPY
    }

    private final Duration ttl;
    private final Map<NamedDatabaseId,CachedDesiredState> cachedDesiredStates;
    private final MutableSetMultimap<NamedDatabaseId,PreventReason> abortPreventionSets;
    private final AvailabilityGuard globalAvailabilityGuard;
    private final EnterpriseSystemGraphDbmsModel dbmsModel;
    private final Clock clock;

    public DatabaseStartAborter( AvailabilityGuard globalAvailabilityGuard, EnterpriseSystemGraphDbmsModel dbmsModel, Clock clock, Duration ttl )
    {
        this.cachedDesiredStates = new ConcurrentHashMap<>();
        this.abortPreventionSets = Multimaps.mutable.set.<NamedDatabaseId,PreventReason>empty().asSynchronized();
        this.globalAvailabilityGuard = globalAvailabilityGuard;
        this.dbmsModel = dbmsModel;
        this.clock = clock;
        this.ttl = ttl;
    }

    public void preventUserAborts( NamedDatabaseId namedDatabaseId, PreventReason reason )
    {
        abortPreventionSets.put( namedDatabaseId, reason );
    }

    public void allowUserAborts( NamedDatabaseId namedDatabaseId, PreventReason reason )
    {
        abortPreventionSets.remove( namedDatabaseId, reason );
    }

    private boolean isUserAllowedToAbort( NamedDatabaseId namedDatabaseId )
    {
        return !abortPreventionSets.containsKey( namedDatabaseId );
    }

    @Override
    public void onPanic( DatabasePanicEvent panic )
    {
        cachedDesiredStates.put( panic.databaseId(), CachedDesiredState.panickedState() );
    }

    public void resetFor( NamedDatabaseId namedDatabaseId )
    {
        cachedDesiredStates.remove( namedDatabaseId );
        abortPreventionSets.removeAll( namedDatabaseId );
    }

    /**
     * Checks the desired state of the given database against the system database, the global availability guard and any record of a previous panic.
     * These checks determine if the {@link EnterpriseDatabase#start()} currently being executed should be aborted.
     * Results are cached for the duration of the instance's ttl, to avoid spamming the system database with read queries when starting many databases.
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
        if ( Objects.equals( namedDatabaseId, NAMED_SYSTEM_DATABASE_ID ) )
        {
            return shouldAbortSystem();
        }

        var desiredState = getDesiredState( namedDatabaseId );

        return desiredState.shouldAbortDueToPanic() ||
               isUserAllowedToAbort( namedDatabaseId ) &&
               ( globalAvailabilityGuard.isShutdown() || desiredState.shouldAbort() );
    }

    private boolean shouldAbortSystem()
    {
        var isPanicked =  Optional.ofNullable( cachedDesiredStates.get( NAMED_SYSTEM_DATABASE_ID ) )
                                  .map( CachedDesiredState::shouldAbortDueToPanic )
                                  .orElse( false );
        return isPanicked || globalAvailabilityGuard.isShutdown();
    }

    /**
     * When a database is eventually started, it should be removed from the Aborter's cache
     */
    public void started( NamedDatabaseId namedDatabaseId )
    {
        cachedDesiredStates.remove( namedDatabaseId );
    }

    private CachedDesiredState getDesiredState( NamedDatabaseId namedDatabaseId )
    {
        return cachedDesiredStates.compute( namedDatabaseId, ( id, cachedState ) ->
        {
            if ( cachedState == null || cachedState.isTimeToDie( clock, ttl ) )
            {
                return getFreshDesiredState( namedDatabaseId );
            }
            return cachedState;
        } );
    }

    private CachedDesiredState getFreshDesiredState( NamedDatabaseId namedDatabaseId )
    {
        var message = String.format( "Failed to check if starting %s should abort as it doesn't exist in the system db!", namedDatabaseId );
        var state = dbmsModel.getStatus( namedDatabaseId ).orElseThrow( () -> new IllegalStateException( message ) );
        return CachedDesiredState.state( clock.instant(), state );
    }

    private static class CachedDesiredState
    {
        private final OperatorState state;
        private final Instant createdAt;
        private final boolean panickedTombstone;

        static CachedDesiredState state( Instant createdAt, OperatorState state )
        {
            return new CachedDesiredState( createdAt, state, false );
        }

        static CachedDesiredState panickedState()
        {
            return new CachedDesiredState( null, null, true );
        }

        private CachedDesiredState( Instant createdAt, OperatorState state, boolean panickedTombstone )
        {
            this.state = state;
            this.createdAt = createdAt;
            this.panickedTombstone = panickedTombstone;
        }

        boolean shouldAbortDueToPanic()
        {
            return panickedTombstone;
        }

        boolean shouldAbort()
        {
            return panickedTombstone ||
                   state == EnterpriseOperatorState.STOPPED ||
                   state == EnterpriseOperatorState.DROPPED ||
                   state == EnterpriseOperatorState.DROPPED_DUMPED;
        }

        boolean isTimeToDie( Clock clock, Duration ttl )
        {
            if ( panickedTombstone )
            {
                return false; // panicked states never expire
            }
            var elapsed = Duration.between( this.createdAt, clock.instant() );
            return elapsed.compareTo( ttl ) >= 0;
        }
    }
}
