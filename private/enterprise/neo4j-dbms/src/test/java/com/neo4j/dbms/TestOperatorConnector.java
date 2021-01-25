/*
 * Copyright (c) "Neo4j"
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

public class TestOperatorConnector extends OperatorConnector
{
    private List<Pair<Map<String,EnterpriseDatabaseState>,ReconcilerRequest>> triggerCalls;
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
    public ReconcilerResult trigger( ReconcilerRequest request )
    {
        var desired = new HashMap<>( operator.desired() );
        triggerCalls.add( Pair.of( desired, request ) );
        return reconciler.reconcile( Collections.singletonList( operator ), request );
    }

    List<Pair<Map<String,EnterpriseDatabaseState>,ReconcilerRequest>> triggerCalls()
    {
        return triggerCalls;
    }
}
