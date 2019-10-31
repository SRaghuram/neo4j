/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import com.neo4j.causalclustering.core.state.machines.id.IdGenerationException;

import java.util.List;
import java.util.function.Predicate;

import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.graphdb.WriteOperationsNotAllowedException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.EpochException;

import static org.neo4j.kernel.api.exceptions.Status.Transaction.LockSessionExpired;

class TransientFailurePredicate implements Predicate<Throwable>
{
    private static final List<Class<? extends Throwable>> transientFailureClasses = List.of(
            EpochException.class,
            NoLeaderFoundException.class,
            WriteOperationsNotAllowedException.class,
            TransientTransactionFailureException.class,
            IdGenerationException.class );

    @Override
    public boolean test( Throwable error )
    {
        if ( isLockExpired( error ) )
        {
            return true;
        }
        return transientFailureClasses.stream().anyMatch( clazz -> clazz.isInstance( error ) );
    }

    private static boolean isLockExpired( Throwable error )
    {
        return error instanceof TransactionFailureException &&
               ((TransactionFailureException) error.getCause()).status() == LockSessionExpired;
    }
}
