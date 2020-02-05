/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.dbms.database.MultiDatabaseManager;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;

import static com.neo4j.dbms.EnterpriseOperatorState.DROPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;

/**
 * The class defines the functions that can be combined to perform state transitions in a {@link TransitionsTable}.
 * Streams of these functions are then executed by a {@link DbmsReconciler}.
 *
 * Each visible method of this class should satisfy the {@link TransitionsTable.Transition} functional interface.
 */
class ReconcilerTransitions
{
     final MultiDatabaseManager<? extends DatabaseContext> databaseManager;

    ReconcilerTransitions( MultiDatabaseManager<? extends DatabaseContext> databaseManager )
    {
        this.databaseManager = databaseManager;
    }

    final EnterpriseDatabaseState stop( NamedDatabaseId namedDatabaseId )
    {
        databaseManager.stopDatabase( namedDatabaseId );
        return new EnterpriseDatabaseState( namedDatabaseId, STOPPED );
    }

    final EnterpriseDatabaseState prepareDrop( NamedDatabaseId namedDatabaseId )
    {
        databaseManager.getDatabaseContext( namedDatabaseId )
                .map( DatabaseContext::database )
                .ifPresent( Database::prepareToDrop );
        return new EnterpriseDatabaseState( namedDatabaseId, STARTED );
    }

    final EnterpriseDatabaseState drop( NamedDatabaseId namedDatabaseId )
    {
        databaseManager.dropDatabase( namedDatabaseId );
        return new EnterpriseDatabaseState( namedDatabaseId, DROPPED );
    }

    final EnterpriseDatabaseState start( NamedDatabaseId namedDatabaseId )
    {
        databaseManager.startDatabase( namedDatabaseId );
        return new EnterpriseDatabaseState( namedDatabaseId, STARTED );
    }

    final EnterpriseDatabaseState create( NamedDatabaseId namedDatabaseId )
    {
        databaseManager.createDatabase( namedDatabaseId );
        return new EnterpriseDatabaseState( namedDatabaseId, STOPPED );
    }
}
