/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.internal.freki;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;

import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.freki.Record.recordXFactor;
import static org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier.TRACER_SUPPLIER;
import static org.neo4j.storageengine.api.RelationshipDirection.INCOMING;
import static org.neo4j.storageengine.api.RelationshipDirection.LOOP;
import static org.neo4j.storageengine.api.RelationshipDirection.OUTGOING;
import static org.neo4j.storageengine.api.RelationshipSelection.ALL_RELATIONSHIPS;

public class FrekiAnalysis
{
    private final MainStores stores;

    public FrekiAnalysis( FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache ) throws IOException
    {
        this( new MainStores( fs, databaseLayout, pageCache, new DefaultIdGeneratorFactory( fs, immediate() ), PageCacheTracer.NULL, TRACER_SUPPLIER,
                immediate(), false ) );
    }

    public FrekiAnalysis( MainStores stores )
    {
        this.stores = stores;
    }

    public void dumpNodes( String nodeIdSpec )
    {
        if ( nodeIdSpec.equals( "*" ) )
        {
            dumpAllNodes();
        }
        else
        {
            var nodeId = Long.parseLong( nodeIdSpec );
            dumpNode( nodeId );
        }
    }

    private void dumpAllNodes()
    {
        try ( var nodeCursor = new FrekiNodeCursor( stores, PageCursorTracer.NULL );
              var propertyCursor = new FrekiPropertyCursor( stores, PageCursorTracer.NULL );
              var relationshipCursor = new FrekiRelationshipTraversalCursor( stores, PageCursorTracer.NULL ) )
        {
            nodeCursor.scan();
            while ( nodeCursor.next() )
            {
                dumpNode( nodeCursor, propertyCursor, relationshipCursor );
            }
        }
    }

    public void dumpNode( long nodeId )
    {
        try ( var nodeCursor = new FrekiNodeCursor( stores, PageCursorTracer.NULL );
              var propertyCursor = new FrekiPropertyCursor( stores, PageCursorTracer.NULL );
              var relationshipCursor = new FrekiRelationshipTraversalCursor( stores, PageCursorTracer.NULL ) )
        {
            nodeCursor.single( nodeId );
            if ( !nodeCursor.next() )
            {
                System.out.println( "Node " + nodeId + " not in use" );
                return;
            }
            dumpNode( nodeCursor, propertyCursor, relationshipCursor );
        }
    }

    private void dumpNode( FrekiNodeCursor nodeCursor, FrekiPropertyCursor propertyCursor, FrekiRelationshipTraversalCursor relationshipCursor )
    {
        dumpLogicalRepresentation( nodeCursor, propertyCursor, relationshipCursor );

        // More physical
        System.out.println( "x1: " + nodeCursor.smallRecord.dataForReading() );
        if ( nodeCursor.headerState.isDense )
        {
            System.out.println( "DENSE" );
        }
        else if ( nodeCursor.headerState.containsForwardPointer )
        {
            System.out.printf( "x%d: %s%n", recordXFactor( nodeCursor.record.sizeExp() ), nodeCursor.smallRecord.dataForReading() );
        }
    }

    public void dumpLogicalRepresentation( FrekiNodeCursor nodeCursor, FrekiPropertyCursor propertyCursor,
            FrekiRelationshipTraversalCursor relationshipCursor )
    {
        long nodeId = nodeCursor.loadedNodeId;
        System.out.printf( "Node[%d] %s%n", nodeId, nodeCursor );
        System.out.printf( "  labels:%s%n", Arrays.toString( nodeCursor.labels() ) );

        System.out.println( "  properties..." );
        nodeCursor.properties( propertyCursor );
        while ( propertyCursor.next() )
        {
            System.out.printf( "  %d=%s%n", propertyCursor.propertyKey(), propertyCursor.propertyValue() );
        }

        System.out.println( "  relationships..." );
        nodeCursor.relationships( relationshipCursor, ALL_RELATIONSHIPS );
        while ( relationshipCursor.next() )
        {
            var direction = relationshipCursor.sourceNodeReference() == relationshipCursor.targetNodeReference() ? LOOP :
                            relationshipCursor.sourceNodeReference() == nodeId ? OUTGOING : INCOMING;
            System.out.printf( "  %s[:%d]%s(%d)%n",
                    direction == LOOP ? "--" : direction == OUTGOING ? "--" : "<-", relationshipCursor.type(),
                    direction == LOOP ? "--" : direction == OUTGOING ? "->" : "--", relationshipCursor.neighbourNodeReference() );
        }
    }

    public void dumpRecord( Record record )
    {
        try ( var nodeCursor = new FrekiNodeCursor( stores, PageCursorTracer.NULL );
              var propertyCursor = new FrekiPropertyCursor( stores, PageCursorTracer.NULL );
              var relationshipCursor = new FrekiRelationshipTraversalCursor( stores, PageCursorTracer.NULL ) )
        {
            if ( !nodeCursor.initializeFromRecord( record ) )
            {
                System.out.println( "Not in use" );
                return;
            }
            dumpLogicalRepresentation( nodeCursor, propertyCursor, relationshipCursor );
            System.out.println( nodeCursor.record.dataForReading() );
            System.out.println( nodeCursor.headerState );
        }
    }

    public void dumpStats()
    {
        // Get the stats
        System.out.println( "Calculating stats ..." );
        var storeStats = gatherStoreStats();

        printFactors( "Distribution of record sizes", storeStats, ( stats, stat ) ->
                (double) stat.usedRecords / stats[0].usedRecords );
        printFactors( "Record occupancy", storeStats, ( stats, stat ) ->
                (double) stat.bytesOccupiedInUsedRecords / (stat.bytesOccupiedInUsedRecords + stat.bytesVacantInUsedRecords) );

        // - TODO Number of big values
        // - TODO Avg size of big value
    }

    private void printFactors( String title, StoreStats[] stats, BiFunction<StoreStats[],StoreStats,Double> calculator )
    {
        System.out.println( title + ":" );
        for ( StoreStats stat : stats )
        {
            System.out.printf( "  x%d: %.2f%n", recordXFactor( stat.sizeExp ), calculator.apply( stats, stat ) * 100d );
        }
    }

    public StoreStats[] gatherStoreStats()
    {
        var storeStats = new StoreStats[stores.getNumMainStores()];
        Collection<Callable<StoreStats>> storeDistributionTasks = new ArrayList<>();
        for ( var i = 0; i < storeStats.length; i++ )
        {
            var sizeExp = i;
            var store = stores.mainStore( sizeExp );
            if ( store != null )
            {
                storeDistributionTasks.add( () ->
                {
                    var stats = new StoreStats( sizeExp );
                    var highId = store.getHighId();
                    var record = store.newRecord();
                    var recordSize = store.recordSize();
                    try ( var cursor = store.openReadCursor() )
                    {
                        for ( var nodeId = 0; nodeId < highId; nodeId++ )
                        {
                            if ( store.read( cursor, record, nodeId ) )
                            {
                                stats.usedRecords++;
                                var data = new MutableNodeRecordData( nodeId );
                                var buffer = record.dataForReading();
                                data.deserialize( buffer, stores.bigPropertyValueStore );
                                stats.bytesOccupiedInUsedRecords += Record.HEADER_SIZE + buffer.position();
                                stats.bytesVacantInUsedRecords += recordSize - Record.HEADER_SIZE - buffer.position();
                            }
                            else
                            {
                                stats.unusedRecords++;
                            }
                        }
                    }
                    return stats;
                } );
            }
        }
        runTasksInParallel( storeDistributionTasks );
        return storeStats;
    }

    public <T> List<T> runTasksInParallel( Collection<Callable<T>> storeDistributionTasks )
    {
        var executorService = Executors.newFixedThreadPool( storeDistributionTasks.size() );
        try
        {
            var futures = executorService.invokeAll( storeDistributionTasks );
            List<T> result = new ArrayList<>();
            for ( var future : futures )
            {
                result.add( future.get() );
            }
            return result;
        }
        catch ( InterruptedException | ExecutionException e )
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException( e );
        }
        finally
        {
            executorService.shutdown();
        }
    }

    public void printAverageFactorFilledRecords() throws IOException
    {
        for ( var i = 0; i < 4; i++ )
        {
            var store = stores.mainStore( i );
            if ( store != null )
            {
                System.out.println( averageFactorFilledRecords( store ) );
            }
        }
    }

    private double averageFactorFilledRecords( SimpleStore store )
    {
        var bytesUsed = 0;
        var bytesMax = 9;
        var record = store.newRecord();
        try ( var cursor = store.openReadCursor() )
        {
            var highId = store.getHighId();
            for ( var id = 0; id < highId; id++ )
            {
                if ( store.read( cursor, record, id ) )
                {
                    var data = new MutableNodeRecordData( id );
                    var buffer = record.dataForReading();
                    data.deserialize( buffer, stores.bigPropertyValueStore );
                    bytesUsed += buffer.position();
                    bytesMax += buffer.capacity();
                }
            }
        }
        return ((double) bytesUsed) / bytesMax;
    }

    private static class StoreStats
    {
        private final int sizeExp;
        private long usedRecords;
        private long unusedRecords;
        private long bytesOccupiedInUsedRecords;
        private long bytesVacantInUsedRecords;

        StoreStats( int sizeExp )
        {
            this.sizeExp = sizeExp;
        }
    }
}
