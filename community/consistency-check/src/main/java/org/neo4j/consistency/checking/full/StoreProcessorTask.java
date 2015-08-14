/*
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
package org.neo4j.consistency.checking.full;

import java.util.Iterator;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.consistency.checking.full.StoppableRunnable;
import org.neo4j.consistency.store.DirectRecordAccess;
import org.neo4j.consistency.store.StoreAccess;
import org.neo4j.consistency.store.StoreIdIterator;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static java.lang.String.format;

public class StoreProcessorTask<R extends AbstractBaseRecord> implements StoppableRunnable
{
    private final RecordStore<R> store;
    private final StoreProcessor[] processors;
    private final ProgressListener[] progressListeners;
    private NeoStore neoStore = null;
    private StoreAccess storeAccess;
    private String name = null;

    StoreProcessorTask( String name, RecordStore<R> store, ProgressMonitorFactory.MultiPartBuilder builder,
            TaskExecutionOrder order, StoreProcessor singlePassProcessor, StoreProcessor... multiPassProcessors )
    {
        this( name, store, "", builder, order, singlePassProcessor, multiPassProcessors );
    }

    StoreProcessorTask( String name, RecordStore<R> store, String builderPrefix,
            ProgressMonitorFactory.MultiPartBuilder builder, TaskExecutionOrder order,
            StoreProcessor singlePassProcessor, StoreProcessor... multiPassProcessors )
    {
        this.name = name;
        this.store = store;
        String storeFileName = store.getStorageFileName().getName();
        String sanitizedBuilderPrefix = builderPrefix == null ? "" : builderPrefix;
        
        this.processors = multiPassProcessors;
        this.progressListeners = new ProgressListener[multiPassProcessors.length];
        for ( int i = 0; i < multiPassProcessors.length; i++ )
        {
            String partName = name + indexedPartName( storeFileName, sanitizedBuilderPrefix, i );
            progressListeners[i] = builder.progressForPart( partName, store.getHighId() );
        }
    }

    private String partName( String storeFileName, String builderPrefix )
    {
        return builderPrefix.length() == 0 ? storeFileName : format( "%s_run_%s", storeFileName, builderPrefix );
    }

    private String indexedPartName( String storeFileName, String prefix, int i )
    {
        if ( prefix.length() != 0 )
        {
            prefix += "_";
        }
        return format( "%s_pass_%s%d", storeFileName, prefix, i );
    }

    @SuppressWarnings( "unchecked" )
    public void run()
    {
        if ( processors.length > 1 )
        {
            System.out.println( "ERROR : Not allowed" );
    		return;
    	}
    	StoreProcessor processor = processors[0];
        long start = System.currentTimeMillis();
        if ( storeAccess != null )
            storeAccess.resetStats( FullCheckNewUtils.LOCALITY );
        beforeProcessing( processor );
            try
            {   
            if ( processor instanceof CacheProcessor )
            	{

                    ((CacheProcessor) processor).action.setCache( ((CacheProcessor) processor).getCacheFields() );
                    if ( !processor.getDirection() )
                        FullCheckNewUtils.setForward( false );
                    ((CacheProcessor) processor).action.processCache();
                    FullCheckNewUtils.setForward( true );
	            }
            	else
            	{
              
	            	processor.setCache();
                    FullCheckNewUtils.Counts.initCount( FullCheckNewUtils.MAX_THREADS );
                    if ( !processor.getDirection() )
                        FullCheckNewUtils.setForward( false );
                    
                    //FullCheckNewUtils.threadIndex.set( 0 );
                    if ( processor.isParallel() )
                    {
                        Distributor distributor = null;
                        if ( processor.getStage() == FullCheckNewUtils.Stages.Stage1_NS_PropsLabels )
                            distributor =
                                    new Distributor<NodeRecord>( neoStore.getNodeStore().getHighId(), Runtime
                                            .getRuntime().availableProcessors() - 1 );
                        else if ( processor.getStage() == FullCheckNewUtils.Stages.Stage8_PS_Props )
                            distributor =
                                    new Distributor<PropertyRecord>( neoStore.getPropertyStore().getHighId(), Runtime
                                            .getRuntime().availableProcessors() - 1 );
                            else
                            distributor =
                                    new Distributor<RelationshipRecord>( neoStore.getNodeStore().getHighId(), Runtime
                                            .getRuntime().availableProcessors() - 1 );
                        processor.setQSize( FullCheckNewUtils.QSize );
                        String msg = processor.applyFilteredParallel( store, storeAccess, distributor, progressListeners[0] );
                        FullCheckNewUtils.saveMessage( msg );
                    }
                    else
                        processor.applyFiltered( store, progressListeners[0] ); 
                    FullCheckNewUtils.setForward( true );
                    FullCheckNewUtils.printPostMessage( processor.getStage() );
	            }
            }
            catch ( Throwable e )
            {
                progressListeners[0].failed( e );
            }
            finally
            {
                afterProcessing( processor );
            }
        FullCheckNewUtils.saveAccessData( (System.currentTimeMillis() - start), getStoreName(), store.getHighId() );
    }
    
    public String getStoreName()
    {
    	String storeName = store.getClass().getName();
        return storeName.substring( storeName.lastIndexOf( "." ) + 1 );
    }

    protected void beforeProcessing( StoreProcessor processor )
    {
        // intentionally empty
    }

    protected void afterProcessing( StoreProcessor processor )
    {
        // intentionally empty
    }

    @Override
    public void stopScanning()
    {
        processors[0].stop();
    }

    public void setStoreAccess( StoreAccess storeAccess )
    {
        this.storeAccess = storeAccess;
    	this.neoStore = storeAccess.getRawNeoStore();
    }

	@Override
    public FullCheckNewUtils.Stages getStage()
    {
		return processors[0].getStage();
	}

    @Override
    public String getName()
    {
        // TODO Auto-generated method stub
        return name;
    }
    
    static long recordsPerCPU = 0;
    public static final ThreadLocal<Integer> threadIndex = new ThreadLocal<>();
    public static boolean withinBounds (long id)
    {
        if (recordsPerCPU > 0 && (id > (threadIndex.get()+ 1)* recordsPerCPU ||
                id < threadIndex.get() * recordsPerCPU))
            return false;
        return true;
    }
    Predicate<AbstractBaseRecord> IN_USE = new Predicate<AbstractBaseRecord>()
            {
                @Override
                public boolean accept( AbstractBaseRecord item )
                {
                    return item.inUse();
                }
            };
    public static class Scanner
    {
        @SafeVarargs
        public static <R extends AbstractBaseRecord> Iterable<R> scan( final RecordStore<R> store,
                final Predicate<? super R>... filters )
        {
            return new Iterable<R>()
            {
                @Override
                public Iterator<R> iterator()
                {
                    return new PrefetchingIterator<R>()
                    {
                        final PrimitiveLongIterator ids = new StoreIdIterator( store );//forward ? new StoreIdIterator( store ) : new StoreIdIterator( store , false) ;

                        @Override
                        protected R fetchNextOrNull()
                        {
                            scan:
                            while ( ids.hasNext() )
                            {
                                R record = store.forceGetRecord( ids.next() );
                                for ( Predicate<? super R> filter : filters )
                                {
                                    if ( !filter.accept( record ) )
                                    {
                                        continue scan;
                                    }
                                }
                                return record;
                            }
                            return null;
                        }
                    };
                }
            };
        }
        
        @SafeVarargs
        public static <R extends AbstractBaseRecord> Iterable<R> scan( final RecordStore<R> store,
                StoreAccess access, final boolean forward, final Predicate<? super R>... filters )
        {
            final StoreAccess storeAccess = access;
            return new Iterable<R>()
            {
                @Override
                public Iterator<R> iterator()
                {
                    return new PrefetchingIterator<R>()
                    {
                        final PrimitiveLongIterator ids = new StoreIdIterator( store , forward);//forward ? new StoreIdIterator( store ) : new StoreIdIterator( store , false) ;

                        @Override
                        protected R fetchNextOrNull()
                        {
                            scan:
                            while ( ids.hasNext() )
                            {
                                R record = StoreAccess.getRecord( store, ids.next(), storeAccess );//store.forceGetRecord( ids.next() );
                                for ( Predicate<? super R> filter : filters )
                                {
                                    if ( !filter.accept( record ) )
                                    {
                                        continue scan;
                                    }
                                }
                                return record;
                            }
                            return null;
                        }
                    };
                }
            };
        }

        public static <R extends AbstractBaseRecord> Iterable<R> scanById( final RecordStore<R> store,
                Iterable<Long> ids )
        {
            return new IterableWrapper<R,Long>( ids )
            {
                @Override
                protected R underlyingObjectToObject( Long id )
                {
                    return store.forceGetRecord( id );
                }
            };
        }
    }
}
