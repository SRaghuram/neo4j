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
    private final ReconcilingDatabaseOperator reconciler;
    private final List<Operator> operators = new CopyOnWriteArrayList<>();

    public OperatorConnector( ReconcilingDatabaseOperator reconciler )
    {
        this.reconciler = reconciler;
    }

    void register( Operator operator )
    {
        operators.add( operator );
    }

    void trigger()
    {
        reconciler.reconcile( new ArrayList<>( operators ) );
    }
}
