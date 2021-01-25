/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.error_handling;

import com.neo4j.dbms.ClusterInternalDbmsOperator;

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
        internalOperator.stopOnPanic( panic.databaseId(), panic.getCause() );
    }
}
