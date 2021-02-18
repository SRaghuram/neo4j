/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.error_handling;

import com.neo4j.dbms.ClusterInternalDbmsOperator;

import org.neo4j.internal.helpers.Exceptions;

public class StopDatabaseHandler implements DatabasePanicEventHandler
{
    private final ClusterInternalDbmsOperator internalOperator;

    StopDatabaseHandler( ClusterInternalDbmsOperator internalOperator )
    {
        this.internalOperator = internalOperator;
    }

    @Override
    public void onPanic( DatabasePanicEvent panic )
    {
        internalOperator.stopOnPanic( panic.databaseId(),
                                      new IllegalStateException( getPanicMessage( panic ), panic.getCause() ) );
    }

    String getPanicMessage( DatabasePanicEvent panic )
    {
        return Exceptions.findCauseOrSuppressed( panic.cause, throwable -> throwable != null && throwable.getMessage() != null )
                         .map( throwable -> panic.reason.getDescription() + ": " + throwable.getMessage() )
                         .orElse( panic.reason.getDescription() );
    }
}
