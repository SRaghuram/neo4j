/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.dbms.database.MultiDatabaseManager;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.dbms.DatabaseState;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.kernel.database.NamedDatabaseId;

public class EnterpriseDatabaseStateService implements DatabaseStateService
{
    private final DbmsReconciler reconciler;
    private final MultiDatabaseManager<?> databaseManager;

    EnterpriseDatabaseStateService( DbmsReconciler reconciler, MultiDatabaseManager<?> databaseManager )
    {
        this.reconciler = reconciler;
        this.databaseManager = databaseManager;
    }

    @Override
    public DatabaseState stateOfDatabase( NamedDatabaseId namedDatabaseId )
    {
        return stateOrUnknown( namedDatabaseId );
    }

    @Override
    public Optional<Throwable> causeOfFailure( NamedDatabaseId namedDatabaseId )
    {
        return stateOrUnknown( namedDatabaseId ).failure();
    }

    @Override
    public Map<NamedDatabaseId,DatabaseState> stateOfAllDatabases()
    {
        return databaseManager.registeredDatabases().keySet().stream()
                              .map( this::stateOrInitial )
                              .collect( Collectors.toUnmodifiableMap( EnterpriseDatabaseState::databaseId, Function.identity() ) );
    }

    private EnterpriseDatabaseState stateOrUnknown( NamedDatabaseId namedDatabaseId )
    {
        return reconciler.getReconcilerEntryOrDefault( namedDatabaseId, () -> EnterpriseDatabaseState.unknown( namedDatabaseId ) );
    }

    private EnterpriseDatabaseState stateOrInitial( NamedDatabaseId namedDatabaseId )
    {
        return reconciler.getReconcilerEntryOrDefault( namedDatabaseId, () -> EnterpriseDatabaseState.initial( namedDatabaseId ) );
    }
}
