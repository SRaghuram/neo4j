/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.tx;

import com.neo4j.causalclustering.catchup.CatchupInboundEventListener;
import com.neo4j.causalclustering.catchup.v3.tx.TxPullRequest;
import com.neo4j.causalclustering.messaging.CatchupProtocolMessage;

import java.net.SocketAddress;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

import static java.lang.String.format;

public class TxPullRequestBatchingLogger extends LifecycleAdapter implements CatchupInboundEventListener
{
    private static final Duration DEFAULT_BATCH_TIME = Duration.ofMinutes( 5 );

    private final Log log;
    private final JobScheduler scheduler;
    private final Duration batchTime;
    private JobHandle<?> recurringJob;
    private volatile boolean started;

    private final Batcher batcher = new Batcher();

    public TxPullRequestBatchingLogger( LogProvider logProvider, JobScheduler scheduler, Duration batchTime )
    {
        this.log = logProvider.getLog( getClass() );
        this.scheduler = scheduler;
        this.batchTime = batchTime == null ? DEFAULT_BATCH_TIME : batchTime;
    }

    @Override
    public void onTxPullRequest( SocketAddress remoteAddress, TxPullRequest msg )
    {
        if ( started )
        {
            var addressDbPairIsNew = batcher.tryRegister( remoteAddress, msg );
            if ( addressDbPairIsNew )
            {
                logNewAddressDbPair( remoteAddress, msg );
            }
            else
            {
                batcher.update( remoteAddress, msg );
            }
        }
        else
        {
            logSimple( remoteAddress, msg );
        }
    }

    @Override
    public void onCatchupProtocolMessage( SocketAddress remoteAddress, CatchupProtocolMessage msg )
    {
        logSimple( remoteAddress, msg );
    }

    @Override
    public void onOtherMessage( SocketAddress remoteAddress, Object msg )
    {
        log.info( "Handling unexpected message type '%s' with message '%s' from [%s]", msg.getClass().getSimpleName(), msg, remoteAddress );
    }

    @Override
    public void start()
    {
        recurringJob = scheduler.scheduleRecurring( Group.CATCHUP_SERVER,
                                                    this::logBatchedEntries,
                                                    batchTime.toMillis(),
                                                    batchTime.toMillis(),
                                                    TimeUnit.MILLISECONDS );
        started = true;
    }

    @Override
    public void stop()
    {
        recurringJob.cancel();
        started = false;
        logBatchedEntries();
    }

    private void logBatchedEntries()
    {
        var output = batcher.flush();
        if ( output.length() != 0 )
        {
            log.info( "TxPullRequests batched latest %s:%s", batchTimeToString(), output );
        }
    }

    private void logSimple( SocketAddress remoteAddress, CatchupProtocolMessage msg )
    {
        log.info( "Handling %s [From: %s]", msg.describe(), remoteAddress );
    }

    private void logNewAddressDbPair( SocketAddress address, TxPullRequest msg )
    {
        var databaseId = msg.databaseId();
        var prevTxId = msg.previousTxId();
        log.info( "New TxPullRequest from %s for %s (above transaction id: %d)." +
                  " TxPullRequests for this database and address will be batched the next %s",
                  address, databaseId, prevTxId, batchTimeToString() );
    }

    public String batchTimeToString()
    {
        var seconds = batchTime.toSeconds();

        var lessThanASecond = seconds == 0;
        if ( lessThanASecond )
        {
            return formatTimeString( batchTime.toMillis(), "millisecond" );
        }

        var isEvenHours = seconds % 3600 == 0;
        if ( isEvenHours )
        {
            return formatTimeString( batchTime.toHours(), "hour" );
        }

        var isEvenMinutes = seconds % 60 == 0;
        if ( isEvenMinutes )
        {
            return formatTimeString( batchTime.toMinutesPart(), "minute" );
        }

        return formatTimeString( seconds, "second" );
    }

    private String formatTimeString( long magnitude, String unit )
    {
        return magnitude == 1 ? format( "1 %s", unit ) : format( "%d %ss", magnitude, unit );
    }

    private static class Batcher
    {
        private final Map<AddressDatabase,BatchedTransactions> batchingMap = new ConcurrentHashMap<>();

        void update( SocketAddress remoteAddress, TxPullRequest msg )
        {
            var addressDb = new AddressDatabase( remoteAddress, msg.databaseId() );
            batchingMap.computeIfPresent( addressDb, ( key, batchedTxs ) -> batchedTxs.addTransaction( msg.previousTxId() ) );
        }

        boolean tryRegister( SocketAddress remoteAddress, TxPullRequest msg )
        {
            var addressDb = new AddressDatabase( remoteAddress, msg.databaseId() );
            var oldValue = batchingMap.putIfAbsent( addressDb, new BatchedTransactions() );
            return oldValue == null;
        }

        String flush()
        {
            var output = new StringBuilder();
            var linesPerAddress = flushToAddressLines();
            linesPerAddress.forEach( output::append );
            return output.toString();
        }

        private List<StringBuilder> flushToAddressLines()
        {
            var linePerAddressMap = new HashMap<SocketAddress,StringBuilder>();
            for ( var addressDatabasePair: batchingMap.keySet() )
            {
                batchingMap.computeIfPresent( addressDatabasePair, ( key, value ) -> flushSingleAddressDatabasePairToMap( key, value, linePerAddressMap ) );
            }
            return linePerAddressMap.values().stream().peek( line -> line.setLength( line.length() - 2 ) ).collect( Collectors.toList() );
        }

        private BatchedTransactions flushSingleAddressDatabasePairToMap( AddressDatabase addressDatabasePair,
                                                                         BatchedTransactions batchedTxs,
                                                                         Map<SocketAddress,StringBuilder> map )
        {
            if ( batchedTxs.nrbOfTx == 0 )
            {
                return null;
            }
            else
            {
                var addressLine = map.computeIfAbsent( addressDatabasePair.address,
                                                       address -> new StringBuilder().append( format( "%n    [%s] sent ", address ) ) );
                addressLine.append( format( "%d TxPullRequests for %s for %s, ",
                                            batchedTxs.nrbOfTx,
                                            addressDatabasePair.databaseId,
                                            formatTxs( batchedTxs ) ) );
                return new BatchedTransactions();
            }
        }

        private static String formatTxs( BatchedTransactions txs )
        {
            var onlyOneTxId = txs.maxTxId == txs.minTxId;
            return onlyOneTxId ?
                   format( "transaction above id %d", txs.maxTxId ) :
                   format( "transactions above id %d to %d", txs.minTxId, txs.maxTxId );
        }

        private static class BatchedTransactions
        {
            int nrbOfTx;
            long minTxId;
            long maxTxId;

            BatchedTransactions()
            {
                nrbOfTx = 0;
                minTxId = Long.MAX_VALUE;
                maxTxId = Long.MIN_VALUE;
            }

            BatchedTransactions addTransaction( long newTxId )
            {
                nrbOfTx++;
                minTxId = Math.min( newTxId, minTxId );
                maxTxId = Math.max( newTxId, maxTxId );
                return this;
            }
        }

        private static class AddressDatabase
        {
            SocketAddress address;
            DatabaseId databaseId;

            AddressDatabase( SocketAddress address, DatabaseId databaseId )
            {
                this.address = address;
                this.databaseId = databaseId;
            }

            @Override
            public boolean equals( Object o )
            {
                if ( this == o )
                {
                    return true;
                }
                if ( o == null || getClass() != o.getClass() )
                {
                    return false;
                }
                AddressDatabase that = (AddressDatabase) o;
                return Objects.equals( address, that.address ) &&
                       Objects.equals( databaseId, that.databaseId );
            }

            @Override
            public int hashCode()
            {
                return Objects.hash( address, databaseId );
            }
        }
    }
}
