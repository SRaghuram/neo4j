/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.util.VisibleForTesting;
import java.util.concurrent.ConcurrentHashMap;

public abstract class DbmsOperator
{
    private final AtomicReference<OperatorConnector> connected = new AtomicReference<>();
    final Map<String,EnterpriseDatabaseState> desired = new ConcurrentHashMap<>();

    /**
     * Connects the operator to the reconciler via a connector
     */
    final void connect( OperatorConnector connector )
    {
        Objects.requireNonNull( connector );
        this.connected.compareAndSet( null, connector );
    }

    /**
     * Disconnects the operator from the reconciler. Future triggers will return `ReconcilerResult.EMPTY`
     */
    final void disconnect( OperatorConnector connector )
    {
        this.connected.compareAndSet( connector, null );
    }

    @VisibleForTesting
    final Optional<OperatorConnector> connected()
    {
        return Optional.ofNullable( connected.get() );
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
        var connector = this.connected.get();
        if ( connector == null )
        {
            return ReconcilerResult.EMPTY;
        }
        return connector.trigger( request );
    }
}
