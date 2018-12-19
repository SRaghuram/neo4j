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
    private static final String TRANSACTION_PREFIX = "transaction";

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
        this.txStarted = name( metricsPrefix, TRANSACTION_PREFIX, "started" );
        this.txPeakConcurrent = name( metricsPrefix, TRANSACTION_PREFIX, "peak_concurrent" );
        this.txActive = name( metricsPrefix, TRANSACTION_PREFIX, "active" );
        this.readTxActive = name( metricsPrefix, TRANSACTION_PREFIX, "active_read" );
        this.writeTxActive = name( metricsPrefix, TRANSACTION_PREFIX, "active_write" );
        this.txCommitted = name( metricsPrefix, TRANSACTION_PREFIX, "committed" );
        this.readTxCommitted = name( metricsPrefix, TRANSACTION_PREFIX, "committed_read" );
        this.writeTxCommitted = name( metricsPrefix, TRANSACTION_PREFIX, "committed_write" );
        this.txRollbacks = name( metricsPrefix, TRANSACTION_PREFIX, "rollbacks" );
        this.readTxRollbacks = name( metricsPrefix, TRANSACTION_PREFIX, "rollbacks_read" );
        this.writeTxRollbacks = name( metricsPrefix, TRANSACTION_PREFIX, "rollbacks_write" );
        this.txTerminated = name( metricsPrefix, TRANSACTION_PREFIX, "terminated" );
        this.readTxTerminated = name( metricsPrefix, TRANSACTION_PREFIX, "terminated_read" );
        this.writeTxTerminated = name( metricsPrefix, TRANSACTION_PREFIX, "terminated_write" );
        this.lastCommittedTxId = name( metricsPrefix, TRANSACTION_PREFIX, "last_committed_tx_id" );
        this.lastClosedTxId = name( metricsPrefix, TRANSACTION_PREFIX, "last_closed_tx_id" );
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
