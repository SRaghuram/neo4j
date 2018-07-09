/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.bloom;

import org.apache.lucene.document.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.neo4j.concurrent.BinaryLatch;
import org.neo4j.function.ThrowingAction;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.api.impl.schema.writer.PartitionedIndexWriter;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.JobScheduler;

class FulltextUpdateApplier extends LifecycleAdapter
{
    private static final FulltextIndexUpdate STOP_SIGNAL = new FulltextIndexUpdate( null, null );
    private static final int POPULATING_BATCH_SIZE = 10_000;
    private static final JobScheduler.Group UPDATE_APPLIER = new JobScheduler.Group( "FulltextIndexUpdateApplier" );
    private static final String APPLIER_THREAD_NAME = "Fulltext Index Add-On Applier Thread";

    private final LinkedBlockingQueue<FulltextIndexUpdate> workQueue;
    private final Log log;
    private final AvailabilityGuard availabilityGuard;
    private final JobScheduler scheduler;
    // This 'stopped' boolean is important for being able to halt a startup process; to stop before we're fully started.
    private final AtomicBoolean stopped;
    private volatile JobScheduler.JobHandle workerThread;

    FulltextUpdateApplier( Log log, AvailabilityGuard availabilityGuard, JobScheduler scheduler )
    {
        this.log = log;
        this.availabilityGuard = availabilityGuard;
        this.scheduler = scheduler;
        stopped = new AtomicBoolean();
        workQueue = new LinkedBlockingQueue<>();
    }

    AsyncFulltextIndexOperation writeBarrier() throws IOException
    {
        FulltextIndexUpdate barrier = new FulltextIndexUpdate( null, ThrowingAction.noop() );
        enqueueUpdate( barrier );
        return barrier;
    }

    AsyncFulltextIndexOperation populate( FulltextIndexType indexType, GraphDatabaseService db, WritableFulltext index )
            throws IOException
    {
        return enqueuePopulateIndex( index, db, indexType.entityIterator( db ) );
    }

    private AsyncFulltextIndexOperation enqueuePopulateIndex(
            WritableFulltext index, GraphDatabaseService db,
            Supplier<ResourceIterable<? extends Entity>> entitySupplier ) throws IOException
    {
        FulltextIndexUpdate population = new FulltextIndexUpdate( index, () ->
        {
            try
            {
                PartitionedIndexWriter indexWriter = index.getIndexWriter();
                String[] indexedPropertyKeys = index.getProperties().toArray( new String[0] );
                ArrayList<Supplier<Document>> documents = new ArrayList<>();
                try ( Transaction ignore = db.beginTx( 1, TimeUnit.DAYS ) )
                {
                    ResourceIterable<? extends Entity> entities = entitySupplier.get();
                    for ( Entity entity : entities )
                    {
                        long entityId = entity.getId();
                        Map<String,Object> properties = entity.getProperties( indexedPropertyKeys );
                        if ( !properties.isEmpty() )
                        {
                            documents.add( documentBuilder( entityId, properties ) );
                        }

                        if ( documents.size() > POPULATING_BATCH_SIZE )
                        {
                            indexWriter.addDocuments( documents.size(), reifyDocuments( documents ) );
                            documents.clear();
                        }
                    }
                }
                indexWriter.addDocuments( documents.size(), reifyDocuments( documents ) );
                index.maybeRefreshBlocking();
                index.setPopulated();
            }
            catch ( Throwable th )
            {
                if ( index != null )
                {
                    index.setFailed();
                }
                throw th;
            }
        } );

        enqueueUpdate( population );
        return population;
    }

    private Supplier<Document> documentBuilder( long entityId, Map<String,Object> properties )
    {
        return () -> LuceneFulltextDocumentStructure.documentRepresentingProperties( entityId, properties );
    }

    private Iterable<Document> reifyDocuments( ArrayList<Supplier<Document>> documents )
    {
        return () -> documents.stream().map( Supplier::get ).iterator();
    }

    private void enqueueUpdate( FulltextIndexUpdate update ) throws IOException
    {
        try
        {
            workQueue.put( update );
        }
        catch ( InterruptedException e )
        {
            throw new IOException( "Fulltext index update failed.", e );
        }
    }

    @Override
    public void start()
    {
        if ( workerThread != null )
        {
            throw new IllegalStateException( APPLIER_THREAD_NAME + " already started." );
        }
        ApplierWorker applierWorker = new ApplierWorker( workQueue, log, availabilityGuard, stopped );
        workerThread = scheduler.schedule( UPDATE_APPLIER, applierWorker );
        try
        {
            writeBarrier().awaitCompletion(); // Wait for the applier thread to be fully started, before proceeding,
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Failed to start fulltext applier thread.", e );
        }
    }

    @Override
    public void stop()
    {
        stopped.set( true );
        boolean enqueued;
        do
        {
            enqueued = workQueue.offer( STOP_SIGNAL );
        }
        while ( !enqueued );

        try
        {
            workerThread.waitTermination();
            workerThread = null;
        }
        catch ( InterruptedException e )
        {
            log.error( "Interrupted before " + APPLIER_THREAD_NAME + " could shut down.", e );
        }
        catch ( ExecutionException e )
        {
            log.error( "Exception while waiting for " + APPLIER_THREAD_NAME + " to shut down.", e );
        }
    }

    private static class FulltextIndexUpdate extends BinaryLatch implements AsyncFulltextIndexOperation
    {
        private final WritableFulltext index;
        private final ThrowingAction<IOException> action;
        private volatile Throwable throwable;

        private FulltextIndexUpdate( WritableFulltext index, ThrowingAction<IOException> action )
        {
            this.index = index;
            this.action = action;
        }

        @Override
        public void awaitCompletion() throws ExecutionException
        {
            super.await();
            Throwable th = this.throwable;
            if ( th != null )
            {
                throw new ExecutionException( th );
            }
        }

        void applyUpdate()
        {
            try
            {
                action.apply();
            }
            catch ( Throwable e )
            {
                throwable = e;
            }
        }
    }

    private static class ApplierWorker implements Runnable
    {
        private LinkedBlockingQueue<FulltextIndexUpdate> workQueue;
        private final Log log;
        private final AvailabilityGuard availabilityGuard;
        private final AtomicBoolean stopped;

        ApplierWorker( LinkedBlockingQueue<FulltextIndexUpdate> workQueue, Log log,
                       AvailabilityGuard availabilityGuard, AtomicBoolean stopped )
        {
            this.workQueue = workQueue;
            this.log = log;
            this.availabilityGuard = availabilityGuard;
            this.stopped = stopped;
        }

        @Override
        public void run()
        {
            Thread.currentThread().setName( APPLIER_THREAD_NAME );
            waitForDatabaseToBeAvailable();
            Set<WritableFulltext> refreshableSet = new HashSet<>();
            List<BinaryLatch> latches = new ArrayList<>();

            FulltextIndexUpdate update;
            while ( (update = getNextUpdate()) != STOP_SIGNAL )
            {
                update = drainQueueAndApplyUpdates( update, refreshableSet, latches );
                refreshAndClearIndexes( refreshableSet );
                releaseAndClearLatches( latches );

                if ( update == STOP_SIGNAL )
                {
                    return;
                }
            }
        }

        private void waitForDatabaseToBeAvailable()
        {
            boolean isAvailable;
            do
            {
                isAvailable = availabilityGuard.isAvailable( 100 );
            }
            while ( !isAvailable && !availabilityGuard.isShutdown() && !stopped.get() );
        }

        private FulltextIndexUpdate drainQueueAndApplyUpdates(
                FulltextIndexUpdate update,
                Set<WritableFulltext> refreshableSet,
                List<BinaryLatch> latches )
        {
            do
            {
                applyUpdate( update, refreshableSet, latches );
                update = workQueue.poll();
            }
            while ( update != null && update != STOP_SIGNAL );
            return update;
        }

        private void refreshAndClearIndexes( Set<WritableFulltext> refreshableSet )
        {
            for ( WritableFulltext index : refreshableSet )
            {
                refreshIndex( index );
            }
            refreshableSet.clear();
        }

        private void releaseAndClearLatches( List<BinaryLatch> latches )
        {
            for ( BinaryLatch latch : latches )
            {
                latch.release();
            }
            latches.clear();
        }

        private FulltextIndexUpdate getNextUpdate()
        {
            FulltextIndexUpdate update = null;
            do
            {
                try
                {
                    update = workQueue.take();
                }
                catch ( InterruptedException e )
                {
                    log.debug( APPLIER_THREAD_NAME + " decided to ignore an interrupt.", e );
                }
            }
            while ( update == null );
            return update;
        }

        private void applyUpdate( FulltextIndexUpdate update,
                                  Set<WritableFulltext> refreshableSet,
                                  List<BinaryLatch> latches )
        {
            latches.add( update );
            update.applyUpdate();
            refreshableSet.add( update.index );
        }

        private void refreshIndex( WritableFulltext index )
        {
            try
            {
                if ( index != null )
                {
                    index.maybeRefreshBlocking();
                }
            }
            catch ( Throwable e )
            {
                log.error( "Failed to refresh fulltext after updates.", e );
            }
        }
    }
}
