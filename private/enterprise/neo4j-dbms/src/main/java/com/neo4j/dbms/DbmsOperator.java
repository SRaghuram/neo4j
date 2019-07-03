/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.kernel.database.DatabaseId;

public abstract class DbmsOperator
{
    private OperatorConnector connector;
    final Map<DatabaseId,OperatorState> desired = new ConcurrentHashMap<>();

    final void connect( OperatorConnector connector )
    {
        Objects.requireNonNull( connector );
        this.connector = connector;
        connector.register( this );
    }

    protected Map<DatabaseId,OperatorState> desired0()
    {
        return desired;
    }

    final Map<DatabaseId,OperatorState> desired()
    {
        return Collections.unmodifiableMap( desired0() );
    }

    final Reconciliation trigger( boolean force )
    {
        if ( connector == null )
        {
            return Reconciliation.EMPTY;
        }
        return connector.trigger( force );
    }
}
