/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency.checking;

import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.neo4j.consistency.checking.CheckDecorator;
import org.neo4j.consistency.checking.DynamicRecordCheck;
import org.neo4j.consistency.checking.DynamicStore;
import org.neo4j.consistency.checking.LabelTokenRecordCheck;
import org.neo4j.consistency.checking.NodeDynamicLabelOrphanChainStartCheck;
import org.neo4j.consistency.checking.PropertyKeyTokenRecordCheck;
import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.checking.RelationshipGroupRecordCheck;
import org.neo4j.consistency.checking.RelationshipTypeTokenRecordCheck;
import org.neo4j.consistency.checking.full.Distributor;
import org.neo4j.consistency.checking.full.FullCheckNewUtils;
import org.neo4j.consistency.checking.full.StoreProcessorTask.Scanner;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.NodeConsistencyReport;
import org.neo4j.consistency.store.StoreAccess;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;

import static java.lang.String.format;
import static org.neo4j.consistency.checking.DynamicStore.ARRAY;
import static org.neo4j.consistency.checking.DynamicStore.NODE_LABEL;
import static org.neo4j.consistency.checking.DynamicStore.SCHEMA;

public abstract class AbstractStoreProcessor extends RecordStore.Processor<RuntimeException>
{
    private RecordCheck<NodeRecord,ConsistencyReport.NodeConsistencyReport> sparseNodeChecker;
    private RecordCheck<NodeRecord,NodeConsistencyReport> denseNodeChecker;
    private RecordCheck<RelationshipRecord,ConsistencyReport.RelationshipConsistencyReport> relationshipChecker;
    private RecordCheck<PropertyRecord,ConsistencyReport.PropertyConsistencyReport> propertyChecker;
    private RecordCheck<PropertyKeyTokenRecord,ConsistencyReport.PropertyKeyTokenConsistencyReport> propertyKeyTokenChecker;
    private RecordCheck<RelationshipTypeTokenRecord,ConsistencyReport.RelationshipTypeConsistencyReport> relationshipTypeTokenChecker;
    private RecordCheck<LabelTokenRecord,ConsistencyReport.LabelTokenConsistencyReport> labelTokenChecker;
    private RecordCheck<RelationshipGroupRecord,ConsistencyReport.RelationshipGroupConsistencyReport> relationshipGroupChecker;
    private int qSize = 10000;

    public AbstractStoreProcessor()
    {
        this( CheckDecorator.NONE );
    }

    public AbstractStoreProcessor( CheckDecorator decorator )
    {
        if ( decorator == null )
        {
            this.sparseNodeChecker = null;
            this.denseNodeChecker = null;
            this.relationshipChecker = null;
            this.propertyChecker = null;
            this.propertyKeyTokenChecker = null;
            this.relationshipTypeTokenChecker = null;
            this.labelTokenChecker = null;
            this.relationshipGroupChecker = null;
            return;
        }
        this.sparseNodeChecker = decorator.decorateNodeChecker( NodeRecordCheck.forSparseNodes() );
        this.denseNodeChecker = decorator.decorateNodeChecker( NodeRecordCheck.forDenseNodes() );
        this.relationshipChecker = decorator.decorateRelationshipChecker( new RelationshipRecordCheck() );
        this.propertyChecker = decorator.decoratePropertyChecker( new PropertyRecordCheck() );
        this.propertyKeyTokenChecker = decorator.decoratePropertyKeyTokenChecker( new PropertyKeyTokenRecordCheck() );
        this.relationshipTypeTokenChecker =
                decorator.decorateRelationshipTypeTokenChecker( new RelationshipTypeTokenRecordCheck() );
        this.labelTokenChecker = decorator.decorateLabelTokenChecker( new LabelTokenRecordCheck() );
        this.relationshipGroupChecker = decorator.decorateRelationshipGroupChecker( new RelationshipGroupRecordCheck() );
    }

    private boolean forward = true;

    public boolean getDirection()
    {
        return forward;
    }

    public boolean reverseDirection()
    {
        forward = !forward;
        return forward;
    }

    public void setQSize( int qSize )
    {
        this.qSize = qSize;
    }

    public void reDecorateRelationship( CheckDecorator decorator, RelationshipRecordCheck newDecorator )
    {
        this.relationshipChecker = decorator.decorateRelationshipChecker( (RelationshipRecordCheck) newDecorator );
    }

    public void reDecorateNode( CheckDecorator decorator, NodeRecordCheck newDecorator, boolean sparseNode )
    {
        if ( newDecorator == null )
        {
            this.sparseNodeChecker = decorator.decorateNodeChecker( NodeRecordCheck.forSparseNodes( false ) );
            this.denseNodeChecker = decorator.decorateNodeChecker( NodeRecordCheck.forDenseNodes( false ) );
        }
        else
        {
            if ( sparseNode )
                this.sparseNodeChecker = decorator.decorateNodeChecker( newDecorator );
            else
                this.denseNodeChecker = decorator.decorateNodeChecker( newDecorator );
        }
    }

    protected abstract void checkNode( RecordStore<NodeRecord> store, NodeRecord node,
            RecordCheck<NodeRecord,ConsistencyReport.NodeConsistencyReport> checker );

    protected abstract void checkRelationship( RecordStore<RelationshipRecord> store, RelationshipRecord rel,
            RecordCheck<RelationshipRecord,ConsistencyReport.RelationshipConsistencyReport> checker );

    protected abstract void checkProperty( RecordStore<PropertyRecord> store, PropertyRecord property,
            RecordCheck<PropertyRecord,ConsistencyReport.PropertyConsistencyReport> checker );

    protected abstract void checkRelationshipTypeToken( RecordStore<RelationshipTypeTokenRecord> store,
            RelationshipTypeTokenRecord record,
            RecordCheck<RelationshipTypeTokenRecord,ConsistencyReport.RelationshipTypeConsistencyReport> checker );

    protected abstract void checkLabelToken( RecordStore<LabelTokenRecord> store, LabelTokenRecord record,
            RecordCheck<LabelTokenRecord,ConsistencyReport.LabelTokenConsistencyReport> checker );

    protected abstract void checkPropertyKeyToken( RecordStore<PropertyKeyTokenRecord> store,
            PropertyKeyTokenRecord record,
            RecordCheck<PropertyKeyTokenRecord,ConsistencyReport.PropertyKeyTokenConsistencyReport> checker );

    protected abstract void checkDynamic( RecordType type, RecordStore<DynamicRecord> store, DynamicRecord string,
            RecordCheck<DynamicRecord,ConsistencyReport.DynamicConsistencyReport> checker );

    protected abstract void checkDynamicLabel( RecordType type, RecordStore<DynamicRecord> store, DynamicRecord string,
            RecordCheck<DynamicRecord,ConsistencyReport.DynamicLabelConsistencyReport> checker );

    protected abstract void checkRelationshipGroup( RecordStore<RelationshipGroupRecord> store,
            RelationshipGroupRecord record,
            RecordCheck<RelationshipGroupRecord,ConsistencyReport.RelationshipGroupConsistencyReport> checker );

    @Override
    public void processSchema( RecordStore<DynamicRecord> store, DynamicRecord schema )
    {
        // cf. StoreProcessor
        checkDynamic( RecordType.SCHEMA, store, schema, new DynamicRecordCheck( store, SCHEMA ) );
    }

    private int sparse = 0, dense = 0;

    @Override
    public final void processNode( RecordStore<NodeRecord> store, NodeRecord node )
    {
        if ( node.isDense() )
        {
            dense++;
            checkNode( store, node, denseNodeChecker );
        }
        else
        {
            sparse++;
            checkNode( store, node, sparseNodeChecker );
        }
        /*if (node.getLongId() % 100000 == 0)
        {
        	System.out.println("Disk I/Os -"+ConsistencyCheckService.getNeoStore().getAccessStatsStr());
        	System.out.println("Dense:["+dense+"] Sparse ["+sparse+"]");
        	dense = sparse = 0;
        	StringBuilder str = new StringBuilder();
        	for (int i = 0; i < FullCheckNewUtils.Fields.length; i++)
        	{
        		if (FullCheckNewUtils.Fields[i] > 0)
        			str.append("["+i+":"+ FullCheckNewUtils.Fields[i]+"] ");
        		FullCheckNewUtils.Fields[i] = 0;
        	}
        	System.out.println("Check times -"+ str.toString());
        }*/
    }

    @Override
    public final void processRelationship( RecordStore<RelationshipRecord> store, RelationshipRecord rel )
    {
        checkRelationship( store, rel, relationshipChecker );
    }

    @Override
    public final void processProperty( RecordStore<PropertyRecord> store, PropertyRecord property )
    {
        checkProperty( store, property, propertyChecker );
    }

    @Override
    public final void processString( RecordStore<DynamicRecord> store, DynamicRecord string, IdType idType )
    {
        RecordType type;
        DynamicStore dereference;
        switch ( idType )
        {
        case STRING_BLOCK:
            type = RecordType.STRING_PROPERTY;
            dereference = DynamicStore.STRING;
            break;
        case RELATIONSHIP_TYPE_TOKEN_NAME:
            type = RecordType.RELATIONSHIP_TYPE_NAME;
            dereference = DynamicStore.RELATIONSHIP_TYPE;
            break;
        case PROPERTY_KEY_TOKEN_NAME:
            type = RecordType.PROPERTY_KEY_NAME;
            dereference = DynamicStore.PROPERTY_KEY;
            break;
        case LABEL_TOKEN_NAME:
            type = RecordType.LABEL_NAME;
            dereference = DynamicStore.LABEL;
            break;
        default:
            throw new IllegalArgumentException( format( "The id type [%s] is not valid for String records.", idType ) );
        }
        checkDynamic( type, store, string, new DynamicRecordCheck( store, dereference ) );
    }

    @Override
    public final void processArray( RecordStore<DynamicRecord> store, DynamicRecord array )
    {
        checkDynamic( RecordType.ARRAY_PROPERTY, store, array, new DynamicRecordCheck( store, ARRAY ) );
    }

    @Override
    public final void processLabelArrayWithOwner( RecordStore<DynamicRecord> store, DynamicRecord array )
    {
        checkDynamic( RecordType.NODE_DYNAMIC_LABEL, store, array, new DynamicRecordCheck( store, NODE_LABEL ) );
        checkDynamicLabel( RecordType.NODE_DYNAMIC_LABEL, store, array, new NodeDynamicLabelOrphanChainStartCheck() );
    }

    @Override
    public final void processRelationshipTypeToken( RecordStore<RelationshipTypeTokenRecord> store,
            RelationshipTypeTokenRecord record )
    {
        checkRelationshipTypeToken( store, record, relationshipTypeTokenChecker );
    }

    @Override
    public final void
            processPropertyKeyToken( RecordStore<PropertyKeyTokenRecord> store, PropertyKeyTokenRecord record )
    {
        checkPropertyKeyToken( store, record, propertyKeyTokenChecker );
    }

    @Override
    public void processLabelToken( RecordStore<LabelTokenRecord> store, LabelTokenRecord record )
    {
        checkLabelToken( store, record, labelTokenChecker );
    }

    @Override
    public void processRelationshipGroup( RecordStore<RelationshipGroupRecord> store, RelationshipGroupRecord record )
            throws RuntimeException
    {
        checkRelationshipGroup( store, record, relationshipGroupChecker );
    }

    public RecordCheck<PropertyRecord,ConsistencyReport.PropertyConsistencyReport> getPropertyChecker()
    {
        return this.propertyChecker;
    }

    //-------------------------start - Parallel logic -------------------------------------
    static long recordsPerCPU = 0;

    public static boolean withinBounds( long id )
    {
        if ( recordsPerCPU > 0
                && (id > (threadIndex.get() + 1) * recordsPerCPU || id < threadIndex.get() * recordsPerCPU) )
            return false;
        return true;
    }

    public <R extends AbstractBaseRecord> String applyFilteredParallel( RecordStore<R> store, StoreAccess storeAccess, 
            Distributor<R> distributor, Predicate<? super R>... filters ) throws RuntimeException
    {
        return applyFilteredParallel( store, storeAccess, distributor, ProgressListener.NONE, filters );
    }

    public <R extends AbstractBaseRecord> String applyFilteredParallel( RecordStore<R> store, StoreAccess storeAccess, 
            Distributor<R> distributor, ProgressListener progressListener, Predicate<? super R>... filters )
            throws RuntimeException
    {
        return applyParallel( store, storeAccess, distributor, progressListener, filters );
    }

    private <R extends AbstractBaseRecord> String applyParallel( RecordStore<R> store, StoreAccess storeAccess, Distributor<R> distributor,
            ProgressListener progressListener, Predicate<? super R>... filters ) throws RuntimeException
    {
        int MAX_THREADS = distributor.getNumThreads();
        recordsPerCPU = distributor.getRecordsPerCPU();
        RecordCheckWorker<R>[] worker = new RecordCheckWorker[MAX_THREADS];
        ArrayBlockingQueue<R>[] recordQ = new ArrayBlockingQueue[MAX_THREADS];
        CountDownLatch startLatch = new CountDownLatch( 1 );
        CountDownLatch endLatch = new CountDownLatch( MAX_THREADS );
        StringBuilder msgs = new StringBuilder();
        for ( int threadId = 0; threadId < MAX_THREADS; threadId++ )
        {
            recordQ[threadId] = new ArrayBlockingQueue<R>( qSize );
            worker[threadId] =
                    new RecordCheckWorker<R>( threadId, startLatch, endLatch, recordQ[threadId], store, this );
            worker[threadId].start();
        }
        try
        {
            startLatch.countDown();
            //--            
            int[] recsProcessed = new int[MAX_THREADS];
            int total = 0;
            int[] qIndex = null;
            int entryCount = 0;
            Iterable<R> iterator = Scanner.scan( store, storeAccess, this.getDirection(), filters );
            for ( R record : iterator )
            {
                total++;
                try
                {
                    qIndex = distributor.whichQ( record, qIndex );
                    for ( int i = 0; i < qIndex.length; i++ )
                    {
                        while ( !recordQ[qIndex[i]].offer( record, 100, TimeUnit.MILLISECONDS ) )
                            qIndex[i] = (qIndex[i] + 1) % qIndex.length;
                        recsProcessed[qIndex[i]]++;
                    }
                }
                catch ( Exception e )
                {
                    System.out.println( "ERROR in applyParallel scan:" + e.getMessage() );
                    break;
                }
                progressListener.set( entryCount++ );
            }
            msgs.append( "Max Threads[" + MAX_THREADS + "] Recs/CPU[" + distributor.getRecordsPerCPU() + "]" );
            msgs.append( "Total[" + total + "] " );
            for ( int i = 0; i < MAX_THREADS; i++ )
                msgs.append( "Q" + i + "[" + recsProcessed[i] + "] " );
            progressListener.done();
            //--
            for ( int threadId = 0; threadId < MAX_THREADS; threadId++ )
                worker[threadId].isDone();
            endLatch.await();
        }
        catch ( Exception e )
        {
            //ignore
            System.out.println( "ERROR in applyParallel:" + e.getMessage() );
        }
        recordsPerCPU = 0;
        return msgs.toString();
    }

    public static final ThreadLocal<Integer> threadIndex = new ThreadLocal<>();

    private class RecordCheckWorker<R extends AbstractBaseRecord> extends java.lang.Thread
    {
        private final int threadId;
        private final CountDownLatch waitSignal;
        private final CountDownLatch endSignal;
        public boolean done = false;
        private int threadLocalProgress;
        public ArrayBlockingQueue<R> recordsQ = null;
        RecordStore<R> store;
        AbstractStoreProcessor processor = null;

        public RecordCheckWorker( int threadId, CountDownLatch wait, CountDownLatch end,
                ArrayBlockingQueue<R> recordsQ, RecordStore<R> store, AbstractStoreProcessor p )
        {
            this.threadId = threadId;
            this.recordsQ = recordsQ;
            this.waitSignal = wait;
            this.store = store;
            this.processor = p;
            this.endSignal = end;
        }

        private void reportProgress()
        {
            //progress.add( threadLocalProgress );
            threadLocalProgress = 0;
        }

        public void isDone()
        {
            done = true;
        }

        @Override
        public void run()
        {
            try
            {
                FullCheckNewUtils.threadIndex.set( threadId );
                waitSignal.await();
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
            }

            while ( !done || !recordsQ.isEmpty() )
            {
                try
                {
                    R record = recordsQ.poll( 2000, TimeUnit.MILLISECONDS );
                    if ( record != null )
                        store.accept( processor, record );
                }
                catch ( Exception ie )
                {
                    System.out.println( "error in RecordStoreProcessor:" + ie.getMessage() );
                }
            }
            reportProgress();
            endSignal.countDown();
        }
    }
    //------------------------- end - Parallel logic -------------------------------------
}
