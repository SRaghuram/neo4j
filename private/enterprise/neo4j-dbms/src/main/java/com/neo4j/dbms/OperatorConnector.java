/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Collection;
import java.util.Set;

import static java.util.function.Predicate.not;

public class OperatorConnector
{
    private final DbmsReconciler reconciler;
    protected volatile Set<DbmsOperator> operators;

    OperatorConnector( DbmsReconciler reconciler )
    {
        this.reconciler = reconciler;
        this.operators = Set.of();
    }

    synchronized void setOperators( final Collection<DbmsOperator> operators )
    {
        var replacementOperators = Set.copyOf( operators );
        var removedOperators = this.operators.stream().filter( not( replacementOperators::contains ) );
        var newOperators = replacementOperators.stream().filter( not( this.operators::contains ) );

        removedOperators.forEach( op -> op.disconnect( this ) );
        newOperators.forEach( op -> op.connect( this ) );
        this.operators = replacementOperators;
    }

    /**
     * Trigger forces the {@link DbmsReconciler} to transition each database from its current state
     * to its state as desired by the various {@code operators}. This operation is asynchronous by
     * default, though you may optionally block using the returned {@link ReconcilerResult} instance.
     *
     * If the {@link DbmsReconciler} has previously failed to transition a database to a desired state,
     * it will not try *any* future transitions unless the force parameter is set to true. By
     * default, only the {@link LocalDbmsOperator} sets force to true when calling {@code trigger()}.
     *
     * @param request a request that contains information about the requested reconciliation attempt.
     * @return the collection of database reconciliation operations caused by this trigger call
     */
    public synchronized ReconcilerResult trigger( ReconcilerRequest request )
    {
        return reconciler.reconcile( operators, request );
    }
}
