/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.transaction.stats;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.ToLongFunction;

import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.kernel.impl.transaction.stats.TransactionCounters;

public class GlobalTransactionStats implements TransactionCounters
{
    private final CopyOnWriteArrayList<TransactionCounters> databasesCounters = new CopyOnWriteArrayList<>();

    /**
     * next major release will nto gonna be able to provide this metric on a server level
     */
    @Override
    @Deprecated
    public long getPeakConcurrentNumberOfTransactions()
    {
        return databasesCounters.stream().mapToLong( TransactionCounters::getPeakConcurrentNumberOfTransactions ).max().orElse( 0 );
    }

    @Override
    public long getNumberOfStartedTransactions()
    {
        return sumCounters( TransactionCounters::getNumberOfStartedTransactions );
    }

    @Override
    public long getNumberOfCommittedTransactions()
    {
        return sumCounters( TransactionCounters::getNumberOfCommittedTransactions );
    }

    @Override
    public long getNumberOfCommittedReadTransactions()
    {
        return sumCounters( TransactionCounters::getNumberOfCommittedReadTransactions );
    }

    @Override
    public long getNumberOfCommittedWriteTransactions()
    {
        return sumCounters( TransactionCounters::getNumberOfCommittedWriteTransactions );
    }

    @Override
    public long getNumberOfActiveTransactions()
    {
        return sumCounters( TransactionCounters::getNumberOfActiveTransactions );
    }

    @Override
    public long getNumberOfActiveReadTransactions()
    {
        return sumCounters( TransactionCounters::getNumberOfActiveReadTransactions );
    }

    @Override
    public long getNumberOfActiveWriteTransactions()
    {
        return sumCounters( TransactionCounters::getNumberOfActiveWriteTransactions );
    }

    @Override
    public long getNumberOfTerminatedTransactions()
    {
        return sumCounters( TransactionCounters::getNumberOfTerminatedTransactions );
    }

    @Override
    public long getNumberOfTerminatedReadTransactions()
    {
        return sumCounters( TransactionCounters::getNumberOfTerminatedReadTransactions );
    }

    @Override
    public long getNumberOfTerminatedWriteTransactions()
    {
        return sumCounters( TransactionCounters::getNumberOfTerminatedWriteTransactions );
    }

    @Override
    public long getNumberOfRolledBackTransactions()
    {
        return sumCounters( TransactionCounters::getNumberOfRolledBackTransactions );
    }

    @Override
    public long getNumberOfRolledBackReadTransactions()
    {
        return sumCounters( TransactionCounters::getNumberOfRolledBackReadTransactions );
    }

    @Override
    public long getNumberOfRolledBackWriteTransactions()
    {
        return sumCounters( TransactionCounters::getNumberOfRolledBackWriteTransactions );
    }

    public DatabaseTransactionStats createDatabaseTransactionMonitor()
    {
        DatabaseTransactionStats transactionStats = new DatabaseTransactionStats();
        databasesCounters.add( transactionStats );
        return transactionStats;
    }

    private long sumCounters( ToLongFunction<TransactionCounters> mappingFunction )
    {
        return databasesCounters.stream().mapToLong( mappingFunction ).sum();
    }
}
