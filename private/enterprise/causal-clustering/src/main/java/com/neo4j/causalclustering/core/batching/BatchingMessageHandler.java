/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.batching;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.RaftMessages.InboundRaftMessageContainer;
import com.neo4j.causalclustering.helper.scheduling.LimitingScheduler;
import com.neo4j.causalclustering.helper.scheduling.ReoccurringJobQueue;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.messaging.ComposableMessageHandler;
import com.neo4j.causalclustering.messaging.LifecycleMessageHandler;

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

import static com.neo4j.causalclustering.core.batching.BoundedPriorityQueue.Result.OK;

/**
 * This class gets Raft messages as input and queues them up for processing. Some messages are batched together before they are forwarded to the Raft machine,
 * for reasons of efficiency.
 */
public class BatchingMessageHandler implements Runnable, LifecycleMessageHandler<RaftMessages.InboundRaftMessageContainer<?>>
{

    private final LifecycleMessageHandler<InboundRaftMessageContainer<?>> handler;
    private final Log log;
    private final BoundedPriorityQueue<InboundRaftMessageContainer<?>> inQueue;
    private final LimitingScheduler scheduler;
    private final BatchingHandlerFactory batchingHandlerFactory;

    private volatile boolean stopped;
    private volatile BoundedPriorityQueue.Result lastResult = OK;
    private AtomicLong droppedCount = new AtomicLong();

    BatchingMessageHandler( LifecycleMessageHandler<InboundRaftMessageContainer<?>> handler,
            BoundedPriorityQueue.Config inQueueConfig, BatchingConfig batchConfig, LimitingScheduler scheduler,
            LogProvider logProvider )
    {
        this.handler = handler;
        this.log = logProvider.getLog( getClass() );
        this.inQueue = new BoundedPriorityQueue<>( inQueueConfig, ContentSizeHandler::of, new MessagePriority() );
        this.scheduler = scheduler;
        this.batchingHandlerFactory = new BatchingHandlerFactory( batchConfig, inQueue );
    }

    public static ComposableMessageHandler composable( Config config,
            JobScheduler jobScheduler, LogProvider logProvider )
    {
        var inQueueConfig = new BoundedPriorityQueue.Config( config.get( CausalClusteringSettings.raft_in_queue_size ),
                                                             config.get( CausalClusteringSettings.raft_in_queue_max_bytes ) );

        var batchConfig = new BatchingConfig( config.get( CausalClusteringSettings.raft_in_queue_max_batch ),
                                              config.get( CausalClusteringSettings.raft_in_queue_max_batch_bytes ) );

        return delegate -> new BatchingMessageHandler( delegate, inQueueConfig, batchConfig,
                                                       new LimitingScheduler( jobScheduler, Group.RAFT_BATCH_HANDLER,
                                                                              logProvider.getLog( BatchingMessageHandler.class ),
                                                                              new ReoccurringJobQueue<>() ), logProvider );
    }

    @Override
    public void start( RaftId raftId ) throws Exception
    {
        handler.start( raftId );
    }

    @Override
    public void stop() throws Exception
    {
        stopped = true;
        handler.stop();
        scheduler.disable();
    }

    @Override
    public void handle( InboundRaftMessageContainer<?> messageContainer )
    {
        if ( stopped )
        {
            log.debug( "This handler has been stopped, dropping the message: %s", messageContainer );
            return;
        }

        var result = inQueue.offer( messageContainer );
        logQueueState( result );
        if ( result == OK )
        {
            scheduler.offerJob( this );
        }
    }

    private void logQueueState( BoundedPriorityQueue.Result result )
    {
        if ( result != OK )
        {
            droppedCount.incrementAndGet();
        }

        if ( result != lastResult )
        {
            if ( result == OK )
            {
                log.info( "Raft in-queue not dropping messages anymore. Dropped %d messages.",
                        droppedCount.getAndSet( 0 ) );
            }
            else
            {
                log.warn( "Raft in-queue dropping messages after: " + result );
            }
            lastResult = result;
        }
    }

    @Override
    public void run()
    {
        inQueue.poll().ifPresent( message ->
        {
            var batchedMessage = message.message().dispatch( batchingHandlerFactory.batchingHandler( message.receivedAt(), message.raftId() ) );
            handler.handle( batchedMessage == null ? message : batchedMessage );
        } );
    }
}
