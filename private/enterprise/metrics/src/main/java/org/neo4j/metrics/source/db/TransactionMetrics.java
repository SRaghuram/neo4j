/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.source.db;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.util.function.Supplier;

import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.stats.TransactionCounters;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.metrics.metric.MetricsCounter;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".Database transaction metrics" )
public class TransactionMetrics extends LifecycleAdapter
{
    @Documented( "The total number of started transactions" )
    private final String txStarted;
    @Documented( "The highest peak of concurrent transactions" )
    private final String txPeakConcurrent;

    @Documented( "The number of currently active transactions" )
    private final String txActive;
    @Documented( "The number of currently active read transactions" )
    private final String readTxActive;
    @Documented( "The number of currently active write transactions" )
    private final String writeTxActive;

    @Documented( "The total number of committed transactions" )
    private final String txCommitted;
    @Documented( "The total number of committed read transactions" )
    private final String readTxCommitted;
    @Documented( "The total number of committed write transactions" )
    private final String writeTxCommitted;

    @Documented( "The total number of rolled back transactions" )
    private final String txRollbacks;
    @Documented( "The total number of rolled back read transactions" )
    private final String readTxRollbacks;
    @Documented( "The total number of rolled back write transactions" )
    private final String writeTxRollbacks;

    @Documented( "The total number of terminated transactions" )
    private final String txTerminated;
    @Documented( "The total number of terminated read transactions" )
    private final String readTxTerminated;
    @Documented( "The total number of terminated write transactions" )
    private final String writeTxTerminated;

    @Documented( "The ID of the last committed transaction" )
    private final String lastCommittedTxId;
    @Documented( "The ID of the last closed transaction" )
    private final String lastClosedTxId;

    private final MetricRegistry registry;
    private final TransactionCounters transactionCounters;
    private final Supplier<TransactionIdStore> transactionIdStoreSupplier;

    public TransactionMetrics( String metricsPrefix, MetricRegistry registry,
            Supplier<TransactionIdStore> transactionIdStoreSupplier, TransactionCounters transactionCounters )
    {
        txStarted = name( metricsPrefix, "started" );
        txPeakConcurrent = name( metricsPrefix, "peak_concurrent" );
        txActive = name( metricsPrefix, "active" );
        readTxActive = name( metricsPrefix, "active_read" );
        writeTxActive = name( metricsPrefix, "active_write" );
        txCommitted = name( metricsPrefix, "committed" );
        readTxCommitted = name( metricsPrefix, "committed_read" );
        writeTxCommitted = name( metricsPrefix, "committed_write" );
        txRollbacks = name( metricsPrefix, "rollbacks" );
        readTxRollbacks = name( metricsPrefix, "rollbacks_read" );
        writeTxRollbacks = name( metricsPrefix, "rollbacks_write" );
        txTerminated = name( metricsPrefix, "terminated" );
        readTxTerminated = name( metricsPrefix, "terminated_read" );
        writeTxTerminated = name( metricsPrefix, "terminated_write" );
        lastCommittedTxId = name( metricsPrefix, "last_committed_tx_id" );
        lastClosedTxId = name( metricsPrefix, "last_closed_tx_id" );
        this.registry = registry;
        this.transactionIdStoreSupplier = transactionIdStoreSupplier;
        this.transactionCounters = transactionCounters;
    }

    @Override
    public void start()
    {
        registry.register( txStarted, new MetricsCounter( transactionCounters::getNumberOfStartedTransactions ) );
        registry.register( txPeakConcurrent, new MetricsCounter( transactionCounters::getPeakConcurrentNumberOfTransactions ) );

        registry.register( txActive, (Gauge<Long>) transactionCounters::getNumberOfActiveTransactions );
        registry.register( readTxActive, (Gauge<Long>) transactionCounters::getNumberOfActiveReadTransactions );
        registry.register( writeTxActive, (Gauge<Long>) transactionCounters::getNumberOfActiveWriteTransactions );

        registry.register( txCommitted, new MetricsCounter( transactionCounters::getNumberOfCommittedTransactions ) );
        registry.register( readTxCommitted, new MetricsCounter( transactionCounters::getNumberOfCommittedReadTransactions ) );
        registry.register( writeTxCommitted, new MetricsCounter( transactionCounters::getNumberOfCommittedWriteTransactions ) );

        registry.register( txRollbacks, new MetricsCounter(  transactionCounters::getNumberOfRolledBackTransactions ) );
        registry.register( readTxRollbacks, new MetricsCounter( transactionCounters::getNumberOfRolledBackReadTransactions ) );
        registry.register( writeTxRollbacks, new MetricsCounter( transactionCounters::getNumberOfRolledBackWriteTransactions ) );

        registry.register( txTerminated, new MetricsCounter( transactionCounters::getNumberOfTerminatedTransactions ) );
        registry.register( readTxTerminated, new MetricsCounter( transactionCounters::getNumberOfTerminatedReadTransactions ) );
        registry.register( writeTxTerminated, new MetricsCounter( transactionCounters::getNumberOfTerminatedWriteTransactions ) );

        registry.register( lastCommittedTxId, new MetricsCounter( () -> transactionIdStoreSupplier.get().getLastCommittedTransactionId() ) );
        registry.register( lastClosedTxId, new MetricsCounter( () -> transactionIdStoreSupplier.get().getLastClosedTransactionId() ) );
    }

    @Override
    public void stop()
    {
        registry.remove( txStarted );
        registry.remove( txPeakConcurrent );

        registry.remove( txActive );
        registry.remove( readTxActive );
        registry.remove( writeTxActive );

        registry.remove( txCommitted );
        registry.remove( readTxCommitted );
        registry.remove( writeTxCommitted );

        registry.remove( txRollbacks );
        registry.remove( readTxRollbacks );
        registry.remove( writeTxRollbacks );

        registry.remove( txTerminated );
        registry.remove( readTxTerminated );
        registry.remove( writeTxTerminated );

        registry.remove( lastCommittedTxId );
        registry.remove( lastClosedTxId );
    }
}
