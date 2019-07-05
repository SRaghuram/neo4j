/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.database.DatabaseId;

public class TestOperatorConnector extends OperatorConnector
{
    private List<Pair<Map<DatabaseId,OperatorState>,Boolean>> triggerCalls;

    public TestOperatorConnector( DbmsReconciler reconciler )
    {
        super( reconciler );
        triggerCalls = new ArrayList<>();
    }

    @Override
    public Reconciliation trigger( boolean force )
    {
        var desiredStates = DbmsReconciler.desiredStates( operators(), OperatorState::minByPrecedence );
        triggerCalls.add( Pair.of( desiredStates, force ) );
        return super.trigger( force );
    }

    List<Pair<Map<DatabaseId,OperatorState>,Boolean>> triggerCalls()
    {
        return triggerCalls;
    }
}
