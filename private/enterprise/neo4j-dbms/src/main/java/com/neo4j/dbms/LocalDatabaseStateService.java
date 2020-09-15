/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Optional;

import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.OperatorState;
import org.neo4j.kernel.database.NamedDatabaseId;

public class LocalDatabaseStateService implements DatabaseStateService
{
    private DbmsReconciler reconciler;

    LocalDatabaseStateService( DbmsReconciler reconciler )
    {
        this.reconciler = reconciler;
    }

    @Override
    public OperatorState stateOfDatabase( NamedDatabaseId namedDatabaseId )
    {
        return reconciler.getReconcilerEntryOrDefault( namedDatabaseId, () -> EnterpriseDatabaseState.unknown( namedDatabaseId ) ).operatorState();
    }

    @Override
    public Optional<Throwable> causeOfFailure( NamedDatabaseId namedDatabaseId )
    {
        return reconciler.getReconcilerEntryOrDefault( namedDatabaseId, () -> EnterpriseDatabaseState.unknown( namedDatabaseId ) ).failure();
    }
}
