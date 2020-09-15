/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.dbms.database.MultiDatabaseManager;

import java.util.function.Consumer;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;

import static com.neo4j.dbms.EnterpriseOperatorState.DIRTY;
import static com.neo4j.dbms.EnterpriseOperatorState.DROPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.DROPPED_DUMPED;
import static com.neo4j.dbms.EnterpriseOperatorState.INITIAL;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.STORE_COPYING;

/**
 * The class defines the functions that can be combined to perform state transitions in a {@link TransitionsTable}.
 * Streams of these functions are then executed by a {@link DbmsReconciler}.
 *
 * Each visible method of this class should return a {@link Transition} object.
 */
class ReconcilerTransitions
{
    static final Consumer<NamedDatabaseId> nothing =  ignored -> {};
    private final MultiDatabaseManager<? extends DatabaseContext> databaseManager;

    ReconcilerTransitions( MultiDatabaseManager<? extends DatabaseContext> databaseManager )
    {
        this.databaseManager = databaseManager;
    }

    private static Transition validateFactory( MultiDatabaseManager<? extends DatabaseContext> databaseManager )
    {
        return Transition.from( DROPPED, INITIAL )
                .doTransition( databaseManager::validateDatabaseCreation )
                .ifSucceeded( INITIAL )
                .ifFailedThenDo( nothing, INITIAL );
    }

    private static Transition createFactory( MultiDatabaseManager<? extends DatabaseContext> databaseManager )
    {
        return Transition.from( DROPPED, INITIAL )
                         .doTransition( databaseManager::createDatabase )
                         .ifSucceeded( STOPPED )
                         .ifFailedThenDo( nothing, DIRTY );
    }

    private Transition startFactory( MultiDatabaseManager<? extends DatabaseContext> databaseManager )
    {
        return Transition.from( STOPPED )
                         .doTransition( databaseManager::startDatabase )
                         .ifSucceeded( STARTED )
                         .ifFailedThenDo( nothing, STOPPED );
    }

    private Transition stopFactory( MultiDatabaseManager<? extends DatabaseContext> databaseManager )
    {
        return Transition.from( STARTED, STORE_COPYING )
                         .doTransition( databaseManager::stopDatabase )
                         .ifSucceeded( STOPPED )
                         .ifFailedThenDo( nothing, STOPPED );
    }

    private Transition dropFactory( MultiDatabaseManager<? extends DatabaseContext> databaseManager, boolean dumpData )
    {
        var succeededState = dumpData ? DROPPED_DUMPED : DROPPED;
        Consumer<NamedDatabaseId> transition = dumpData ? databaseManager::dropDatabaseDumpData : databaseManager::dropDatabase;
        return Transition.from( STOPPED, DIRTY )
                         .doTransition( transition )
                         .ifSucceeded( succeededState )
                         .ifFailedThenDo( nothing, DIRTY );
    }

    private static Transition prepareDropFactory( MultiDatabaseManager<? extends DatabaseContext> databaseManager )
    {
        Consumer<NamedDatabaseId> transition = id -> databaseManager.getDatabaseContext( id )
                                                                    .map( DatabaseContext::database )
                                                                    .ifPresent( Database::prepareToDrop );
        // Failed state for prepareDrop marked as unknown as the only possible failure is that the database manage fails to find database with given id
        return Transition.from( STARTED )
                         .doTransition( transition )
                         .ifSucceeded( STARTED )
                         .ifFailedThenDo( databaseManager::stopDatabase, DIRTY );
    }

    final Transition stop()
    {
        return stopFactory( databaseManager );
    }

    final Transition prepareDrop()
    {
        return prepareDropFactory( databaseManager );
    }

    final Transition drop()
    {
        return dropFactory( databaseManager, false );
    }

    final Transition dropDumpData()
    {
        return dropFactory( databaseManager, true );
    }

    final Transition start()
    {
        return startFactory( databaseManager );
    }

    final Transition create()
    {
        return createFactory( databaseManager );
    }

    final Transition validate()
    {
        return validateFactory( databaseManager );
    }
}
