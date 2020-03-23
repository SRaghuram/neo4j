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
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.tokenstore.GBPTreeTokenStore;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.lifecycle.Life;
import org.neo4j.storageengine.api.StandardConstraintRuleAccessor;
import org.neo4j.token.api.NamedToken;

import static java.util.Arrays.stream;
import static java.util.stream.Stream.of;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.freki.CursorAccessPatternTracer.NO_TRACING;
import static org.neo4j.internal.freki.MutableNodeRecordData.forwardPointerPointsToDense;
import static org.neo4j.internal.freki.MutableNodeRecordData.forwardPointerPointsToXRecord;
import static org.neo4j.internal.freki.Record.FLAG_IN_USE;
import static org.neo4j.internal.freki.Record.recordSize;
import static org.neo4j.internal.freki.Record.recordXFactor;
import static org.neo4j.io.ByteUnit.bytesToString;
import static org.neo4j.storageengine.api.RelationshipDirection.INCOMING;
import static org.neo4j.storageengine.api.RelationshipDirection.LOOP;
import static org.neo4j.storageengine.api.RelationshipDirection.OUTGOING;
import static org.neo4j.storageengine.api.RelationshipSelection.ALL_RELATIONSHIPS;
import static org.neo4j.token.TokenHolders.readOnlyTokenHolders;

public class FrekiAnalysis extends Life implements AutoCloseable
{
    private final MainStores stores;
    private final FrekiCursorFactory cursorFactory;

    public FrekiAnalysis( FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache ) throws IOException
    {
        this( new Stores( fs, databaseLayout, pageCache, new DefaultIdGeneratorFactory( fs, immediate() ), PageCacheTracer.NULL,
                immediate(), false, new StandardConstraintRuleAccessor(), i -> i, readOnlyTokenHolders() ), true,
                stores -> new FrekiCursorFactory( stores, NO_TRACING ) );
    }

    public FrekiAnalysis( MainStores stores )
    {
        this( stores, new FrekiCursorFactory( stores, NO_TRACING ) );
    }

    public FrekiAnalysis( MainStores stores, FrekiCursorFactory cursorFactory )
    {
        this( stores, false, s -> cursorFactory );
    }

    FrekiAnalysis( MainStores stores, boolean manageStoreLifeToo, Function<MainStores,FrekiCursorFactory> cursorFactory )
    {
        this.stores = stores;
        this.cursorFactory = cursorFactory.apply( stores );
        if ( manageStoreLifeToo )
        {
            life.add( stores );
        }
        life.start();
    }

    public void dumpRelationship( long relId )
    {
        try ( var cursor = cursorFactory.allocateRelationshipScanCursor( PageCursorTracer.NULL );
              var propertyCursor = cursorFactory.allocatePropertyCursor( PageCursorTracer.NULL ) )
        {
            cursor.single( relId );
            if ( cursor.next() )
            {
                System.out.printf( "%d -[%d]-> %d %n", cursor.sourceNodeReference(), cursor.type(), cursor.targetNodeReference() );
                cursor.properties( propertyCursor );
                dumpProperties( propertyCursor );
            }
            else
            {
                System.out.println( "Not found" );
            }
        }
    }

    public void dumpNodes( String nodeIdSpec )
    {
        if ( nodeIdSpec.equals( "*" ) )
        {
            dumpAllNodes();
        }
        else if ( nodeIdSpec.contains( "-" ) )
        {
            var separatorIndex = nodeIdSpec.indexOf( '-' );
            var fromId = Long.parseLong( nodeIdSpec.substring( 0, separatorIndex ) );
            var toId = Long.parseLong( nodeIdSpec.substring( separatorIndex + 1 ) );
            dumpNodeRange( fromId, toId );
        }
        else if ( nodeIdSpec.contains( "," ) )
        {
            dumpNodes( stream( nodeIdSpec.split( "," ) ).mapToLong( Long::parseLong ).toArray() );
        }
        else
        {
            dumpNodes( Long.parseLong( nodeIdSpec ) );
        }
    }

    private void dumpAllNodes()
    {
        try ( var nodeCursor = cursorFactory.allocateNodeCursor( PageCursorTracer.NULL );
              var propertyCursor = cursorFactory.allocatePropertyCursor( PageCursorTracer.NULL );
              var relationshipCursor = cursorFactory.allocateRelationshipTraversalCursor( PageCursorTracer.NULL ) )
        {
            nodeCursor.scan();
            while ( nodeCursor.next() )
            {
                dumpNode( nodeCursor, propertyCursor, relationshipCursor );
            }
        }
    }

    public void dumpNodes( long... nodeIds )
    {
        try ( var nodeCursor = cursorFactory.allocateNodeCursor( PageCursorTracer.NULL );
                var propertyCursor = cursorFactory.allocatePropertyCursor( PageCursorTracer.NULL );
                var relationshipCursor = cursorFactory.allocateRelationshipTraversalCursor( PageCursorTracer.NULL ) )
        {
            for ( long nodeId : nodeIds )
            {
                dumpNode( nodeId, nodeCursor, propertyCursor, relationshipCursor );
            }
        }
    }

    public void dumpNodeRange( long fromNodeId, long toNodeId )
    {
        try ( var nodeCursor = cursorFactory.allocateNodeCursor( PageCursorTracer.NULL );
              var propertyCursor = cursorFactory.allocatePropertyCursor( PageCursorTracer.NULL );
              var relationshipCursor = cursorFactory.allocateRelationshipTraversalCursor( PageCursorTracer.NULL ) )
        {
            for ( long nodeId = fromNodeId; nodeId < toNodeId; nodeId++ )
            {
                dumpNode( nodeId, nodeCursor, propertyCursor, relationshipCursor );
            }
        }
    }

    public void dumpNode( long nodeId, FrekiNodeCursor nodeCursor, FrekiPropertyCursor propertyCursor, FrekiRelationshipTraversalCursor relationshipCursor )
    {
        nodeCursor.single( nodeId );
        if ( !nodeCursor.next() )
        {
            System.out.println( "Node " + nodeId + " not in use" );
            return;
        }
        dumpNode( nodeCursor, propertyCursor, relationshipCursor );
    }

    private void dumpNode( FrekiNodeCursor nodeCursor, FrekiPropertyCursor propertyCursor, FrekiRelationshipTraversalCursor relationshipCursor )
    {
        dumpLogicalRepresentation( nodeCursor, propertyCursor, relationshipCursor );

        // More physical
        System.out.println( nodeCursor.toString() );
        System.out.println( "TODO print x1 raw bytes" );
        if ( forwardPointerPointsToXRecord( nodeCursor.data.forwardPointer ) )
        {
            System.out.println( "TODO print xL raw bytes" );
        }
    }

    public void dumpLogicalRepresentation( FrekiNodeCursor nodeCursor, FrekiPropertyCursor propertyCursor,
            FrekiRelationshipTraversalCursor relationshipCursor )
    {
        var nodeId = nodeCursor.entityReference();
        System.out.printf( "Node[%d] %s%n", nodeId, nodeCursor );
        System.out.printf( "  labels:%s%n", Arrays.toString( nodeCursor.labels() ) );

        nodeCursor.properties( propertyCursor );
        dumpProperties( propertyCursor );

        System.out.println( "  relationships..." );
        nodeCursor.relationships( relationshipCursor, ALL_RELATIONSHIPS );
        while ( relationshipCursor.next() )
        {
            var direction = relationshipCursor.sourceNodeReference() == relationshipCursor.targetNodeReference() ? LOOP :
                            relationshipCursor.sourceNodeReference() == nodeId ? OUTGOING : INCOMING;
            System.out.printf( "  (%d)%s[:%d,%d]%s(%d)%n", relationshipCursor.originNodeReference(),
                    direction == LOOP ? "--" : direction == OUTGOING ? "--" : "<-", relationshipCursor.type(),
                    relationshipCursor.entityReference(),
                    direction == LOOP ? "--" : direction == OUTGOING ? "->" : "--", relationshipCursor.neighbourNodeReference() );
        }
    }

    private void dumpProperties( FrekiPropertyCursor propertyCursor )
    {
        System.out.println( "  properties..." );
        while ( propertyCursor.next() )
        {
            System.out.printf( "  %d=%s%n", propertyCursor.propertyKey(), propertyCursor.propertyValue() );
        }
    }

    /**
     * E.g. 123x4 or 456
     */
    public void dumpRecord( String record )
    {
        var xIndex = record.indexOf( 'x' );
        long id;
        int sizeExp;
        if ( xIndex == -1 )
        {
            id = Long.parseLong( record );
            sizeExp = 0;
        }
        else
        {
            id = Long.parseLong( record.substring( 0, xIndex ) );
            sizeExp = Record.sizeExpFromXFactor( Integer.parseInt( record.substring( xIndex + 1 ) ) );
        }
        dumpRecord( id, sizeExp );
    }

    public void dumpRecord( long id, int sizeExp )
    {
        dumpRecord( loadRecord( id, sizeExp ) );
    }

    private Record loadRecord( long id, int sizeExp )
    {
        var store = stores.mainStore( sizeExp );
        var record = store.newRecord();
        try ( var cursor = store.openReadCursor( PageCursorTracer.NULL ) )
        {
            store.read( cursor, record, id );
            return record;
        }
    }

    public void dumpRecord( Record record )
    {
        try ( var nodeCursor = cursorFactory.allocateNodeCursor( PageCursorTracer.NULL );
              var propertyCursor = cursorFactory.allocatePropertyCursor( PageCursorTracer.NULL );
              var relationshipCursor = cursorFactory.allocateRelationshipTraversalCursor( PageCursorTracer.NULL ) )
        {
            System.out.println( record );
            if ( !nodeCursor.initializeFromRecord( record ) )
            {
                System.out.println( "Not in use" );
                return;
            }
            dumpLogicalRepresentation( nodeCursor, propertyCursor, relationshipCursor );
            System.out.println( nodeCursor );
        }
    }

    public void dumpStats()
    {
        // Get the stats
        System.out.println( "Calculating main store stats ..." );
        var storeStats = gatherStoreStats();

        var totalNumDenseNodes = of( storeStats ).mapToLong( s -> s.numDenseNodes ).sum();
        printPercents( "Distribution of nodes", storeStats, ( stats, stat ) ->
        {
            long numRecords = stat == stats[0]
                    // For x1 we're interested in nodes that are ONLY in x1
                    ? stat.usedRecords - stream( stats ).filter( s -> s != stats[0] ).mapToLong( s -> s.usedRecords ).sum()
                    : stat.usedRecords;
            return (double) numRecords / stats[0].usedRecords;
        }, true );
        System.out.printf( " (DE: %.2f%%)%n", percent( totalNumDenseNodes, storeStats[0].usedRecords ) );
        printPercents( "Record occupancy i.e. on average how much of the record is actual useful data", storeStats, ( stats, stat ) ->
                (double) stat.bytesOccupiedInUsedRecords / (stat.bytesOccupiedInUsedRecords + stat.bytesVacantInUsedRecords), false );

        var totalOccupied = of( storeStats ).mapToLong( s -> s.bytesOccupiedInUsedRecords ).sum();
        var totalVacant = of( storeStats ).mapToLong( s -> s.bytesVacantInUsedRecords + (s.unusedRecords * recordSize( s.sizeExp )) ).sum();
        var totalVacantInUsedRecords = of( storeStats ).mapToLong( s -> s.bytesVacantInUsedRecords ).sum();
        var totalPossibleOccupied = of( storeStats ).mapToLong( s -> s.usedRecords * recordSize( s.sizeExp ) ).sum();
        var total = of( storeStats ).mapToLong( s -> (s.usedRecords + s.unusedRecords) * recordSize( s.sizeExp ) ).sum();

        System.out.printf( "Total occupied bytes in used records %s (%.2f%%)%n", bytesToString( totalOccupied ),
                percent( totalOccupied, totalPossibleOccupied ) );
        System.out.printf( "Total vacant bytes in used records %s (%.2f%%)%n", bytesToString( totalVacantInUsedRecords ),
                percent( totalVacantInUsedRecords, totalPossibleOccupied ) );
        System.out.printf( "Total occupied bytes %s (%.2f%%)%n", bytesToString( totalOccupied ), percent( totalOccupied, total ) );
        System.out.printf( "Total vacant bytes %s (%.2f%%)%n", bytesToString( totalVacant ), percent( totalVacant, total ) );

        System.out.println();
        System.out.println( "Calculating dense store stats ..." );
        DenseRelationshipStore.Stats denseStats = stores.denseStore.gatherStats( PageCursorTracer.NULL );
        System.out.printf( "  Total dense store file size: %s%n", bytesToString( denseStats.totalTreeByteSize() ) );
        System.out.printf( "  Effective dense store data size: %s%n", bytesToString( denseStats.effectiveByteSize() ) );
        System.out.printf( "  Number of dense nodes: %d%n", denseStats.numberOfNodes() );
        System.out.printf( "  Avg number of relationships per dense node: %.2f%n", (double) denseStats.numberOfRelationships() / denseStats.numberOfNodes() );
        System.out.printf( "  Avg effective data size per dense node: %.2f%n", (double) denseStats.effectiveByteSize() / denseStats.numberOfNodes() );
        System.out.printf( "  Avg effective relationship size per dense node: %.2f%n",
                (double) denseStats.effectiveRelationshipsByteSize() / denseStats.numberOfNodes() );
        System.out.printf( "  Avg effective size per relationship: %.2f%n",
                (double) denseStats.effectiveRelationshipsByteSize() / denseStats.numberOfRelationships() );
        System.out.printf( "  Avg total size per dense node: %.2f%n", (double) denseStats.totalTreeByteSize() / denseStats.numberOfNodes() );

        // - TODO Number of big values
        // - TODO Avg size of big value
    }

    private static double percent( long part, long whole )
    {
        return 100D * part / whole;
    }

    private void printPercents( String title, StoreStats[] stats, BiFunction<StoreStats[],StoreStats,Double> calculator, boolean includeCumulative )
    {
        System.out.println( title + ":" );
        double cumulativePercent = 0;
        String format = "  x%d: %.2f%%" + (includeCumulative ? " = %.2f" : "") + "%n";
        for ( StoreStats stat : stats )
        {
            double percent = calculator.apply( stats, stat ) * 100d;
            cumulativePercent += percent;
            Object[] args = includeCumulative
                            ? new Object[]{recordXFactor( stat.sizeExp ), percent, cumulativePercent}
                            : new Object[]{recordXFactor( stat.sizeExp ), percent};
            System.out.printf( format, args );
        }
    }

    public StoreStats[] gatherStoreStats()
    {
        Collection<Callable<StoreStats>> storeDistributionTasks = new ArrayList<>();
        for ( var i = 0; i < stores.getNumMainStores(); i++ )
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
                    var recordDataSize = store.recordDataSize();
                    try ( var cursor = store.openReadCursor( PageCursorTracer.NULL ) )
                    {
                        for ( var id = 0; id < highId; id++ )
                        {
                            if ( store.read( cursor, record, id ) && record.hasFlag( FLAG_IN_USE ) )
                            {
                                stats.usedRecords++;
                                var data = new MutableNodeRecordData( id );
                                var buffer = record.data();
                                try
                                {
                                    data.deserialize( buffer, stores.bigPropertyValueStore );
                                }
                                catch ( Exception e )
                                {
                                    System.err.println( "Caught exception when processing id " + id + " in store x" +
                                            Record.recordXFactor( store.recordSizeExponential() ) );
                                    throw e;
                                }
                                stats.bytesOccupiedInUsedRecords += Record.HEADER_SIZE + buffer.position();
                                stats.bytesVacantInUsedRecords += recordDataSize - buffer.position();
                                if ( forwardPointerPointsToDense( data.getForwardPointer() ) )
                                {
                                    stats.numDenseNodes++;
                                }
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
        return runTasksInParallel( storeDistributionTasks ).stream().toArray( StoreStats[]::new );
    }

    public boolean nodeIsDense( long nodeId )
    {
        var record = loadRecord( nodeId, 0 );
        if ( record.hasFlag( FLAG_IN_USE ) )
        {
            var data = new MutableNodeRecordData( nodeId );
            data.deserialize( record.data(), stores.bigPropertyValueStore );
            return forwardPointerPointsToDense( data.getForwardPointer() );
        }
        return false;
    }

    private <T> List<T> runTasksInParallel( Collection<Callable<T>> storeDistributionTasks )
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

    public void dumpTokens()
    {
        if ( stores instanceof Stores )
        {
            Stores allStores = (Stores) stores;
            try
            {
                dumpTokens( allStores.propertyKeyTokenStore, "Property key tokens" );
                dumpTokens( allStores.labelTokenStore, "Label tokens" );
                dumpTokens( allStores.relationshipTypeTokenStore, "Relationship type tokens" );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        }
        else
        {
            System.out.println( "Please instantiate FrekiAnalysis with a full Stores to get this feature" );
        }
    }

    public void dumpTokens( GBPTreeTokenStore tokenStore, String name ) throws IOException
    {
        System.out.println( name );
        for ( NamedToken token : tokenStore.loadTokens( PageCursorTracer.NULL ) )
        {
            System.out.println( "  " + token.id() + ": " + token.name() );
        }
    }

    @Override
    public void close()
    {
        shutdown();
    }

    private static class StoreStats
    {
        private final int sizeExp;
        private long usedRecords;
        private long unusedRecords;
        private long bytesOccupiedInUsedRecords;
        private long bytesVacantInUsedRecords;
        private long numDenseNodes;

        StoreStats( int sizeExp )
        {
            this.sizeExp = sizeExp;
        }
    }
}
