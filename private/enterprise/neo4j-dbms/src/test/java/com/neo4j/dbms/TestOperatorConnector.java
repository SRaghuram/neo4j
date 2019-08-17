/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.database.DatabaseId;

public class TestOperatorConnector extends OperatorConnector
{
    private List<Pair<Map<DatabaseId,OperatorState>,ReconcilerRequest>> triggerCalls;
    private DbmsOperator operator;
    private DbmsReconciler reconciler;

    TestOperatorConnector( DbmsReconciler reconciler )
    {
        super( reconciler );
        this.reconciler = reconciler;
        triggerCalls = new ArrayList<>();
    }

    @Override
    void register( DbmsOperator operator )
    {
        this.operator = operator;
    }

    @Override
    public Reconciliation trigger( ReconcilerRequest request )
    {
        var desired = new HashMap<>( operator.desired() );
        triggerCalls.add( Pair.of( desired, request ) );
        return reconciler.reconcile( Collections.singletonList( operator ), request );
    }

    List<Pair<Map<DatabaseId,OperatorState>,ReconcilerRequest>> triggerCalls()
    {
        return triggerCalls;
    }
}
