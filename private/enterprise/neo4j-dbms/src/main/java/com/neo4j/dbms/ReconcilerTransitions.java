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
import static com.neo4j.dbms.EnterpriseOperatorState.INITIAL;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.STORE_COPYING;
import static com.neo4j.dbms.EnterpriseOperatorState.UNKNOWN;

/**
 * The class defines the functions that can be combined to perform state transitions in a {@link TransitionsTable}.
 * Streams of these functions are then executed by a {@link DbmsReconciler}.
 *
 * Each visible method of this class should return a {@link Transition} object.
 */
class ReconcilerTransitions
{
    static final Consumer<NamedDatabaseId> nothing =  ignored -> {};

    private final Transition create;
    private final Transition start;
    private final Transition stop;
    private final Transition drop;
    private final Transition prepareDrop;

    ReconcilerTransitions( MultiDatabaseManager<? extends DatabaseContext> databaseManager )
    {
        this.create = createFactory( databaseManager );
        this.start = startFactory( databaseManager );
        this.stop = stopFactory( databaseManager );
        this.drop = dropFactory( databaseManager );
        this.prepareDrop = prepareDropFactory( databaseManager );
    }

    private static Transition createFactory( MultiDatabaseManager<? extends DatabaseContext> databaseManager )
    {
        return Transition.from( INITIAL )
                         .doTransition( databaseManager::createDatabase )
                         .ifSucceeded( STOPPED )
                         .ifFailedThenDo( nothing, DIRTY );
    }

    private static Transition startFactory( MultiDatabaseManager<? extends DatabaseContext> databaseManager )
    {
        return Transition.from( STOPPED )
                         .doTransition( databaseManager::startDatabase )
                         .ifSucceeded( STARTED )
                         .ifFailedThenDo( nothing, STOPPED );
    }

    private static Transition stopFactory( MultiDatabaseManager<? extends DatabaseContext> databaseManager )
    {
        return Transition.from( STARTED, STORE_COPYING )
                         .doTransition( databaseManager::stopDatabase )
                         .ifSucceeded( STOPPED )
                         .ifFailedThenDo( nothing, STOPPED );
    }

    private static Transition dropFactory( MultiDatabaseManager<? extends DatabaseContext> databaseManager )
    {
        return Transition.from( STOPPED )
                         .doTransition( databaseManager::dropDatabase )
                         .ifSucceeded( DROPPED )
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
                         .ifFailedThenDo( nothing, UNKNOWN );
    }

    final Transition stop()
    {
        return stop;
    }

    final Transition prepareDrop()
    {
        return prepareDrop;
    }

    final Transition drop()
    {
        return drop;
    }

    final Transition start()
    {
        return start;
    }

    final Transition create()
    {
        return create;
    }
}
