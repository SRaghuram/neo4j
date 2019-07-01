/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class OperatorConnector
{
    private final DbmsReconciler reconciler;
    private final List<DbmsOperator> operators = new CopyOnWriteArrayList<>();

    public OperatorConnector( DbmsReconciler reconciler )
    {
        this.reconciler = reconciler;
    }

    void register( DbmsOperator operator )
    {
        operators.add( operator );
    }

    public Reconciliation trigger()
    {
        return trigger( false );
    }

    /**
     * Trigger forces the {@link DbmsReconciler} to transition each database from its current state
     * to its state as desired by the various {@code operators}. This operation is asynchronous by
     * default, though you may optionally block using the returned {@link Reconciliation} instance.
     *
     * If the {@link DbmsReconciler} has previously failed to transition a database to a desired state,
     * it will not try *any* future transitions unless the force parameter is set to true. By
     * default, only the {@link LocalDbmsOperator} sets force to true when calling {@code trigger()}.
     *
     * @param force Whether or not to force the reconciler to try transitions for databases which previously failed
     * @return the collection of database reconciliation operations caused by this trigger call
     */
    public Reconciliation trigger( boolean force )
    {
        return reconciler.reconcile( new ArrayList<>( operators ), force );
    }

    List<DbmsOperator> operators()
    {
        return operators;
    }
}
