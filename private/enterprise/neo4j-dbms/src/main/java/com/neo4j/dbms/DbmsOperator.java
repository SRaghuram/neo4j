/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public abstract class DbmsOperator
{
    private OperatorConnector connector;
    final Map<String,EnterpriseDatabaseState> desired = new ConcurrentHashMap<>();

    /**
     * Connects the operator to the reconciler via a connector
     */
    final void connect( OperatorConnector connector )
    {
        Objects.requireNonNull( connector );
        this.connector = connector;
        connector.register( this );
    }

    protected Map<String,EnterpriseDatabaseState> desired0()
    {
        return desired;
    }

    /**
     * @return the states that the operator desires for each database it cares about
     */
    final Map<String,EnterpriseDatabaseState> desired()
    {
        return Map.copyOf( desired0() );
    }

    final ReconcilerResult trigger( ReconcilerRequest request )
    {
        if ( connector == null )
        {
            return ReconcilerResult.EMPTY;
        }
        return connector.trigger( request );
    }
}
