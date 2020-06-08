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

<<<<<<< HEAD
import java.io.IOException;
=======
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;
<<<<<<< HEAD
=======
import java.util.function.Consumer;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
import java.util.function.Function;

import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.tokenstore.GBPTreeTokenStore;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.lifecycle.Life;
<<<<<<< HEAD
import org.neo4j.storageengine.api.StandardConstraintRuleAccessor;
import org.neo4j.token.api.NamedToken;

=======
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.StandardConstraintRuleAccessor;
import org.neo4j.token.api.NamedToken;

import static java.lang.String.format;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
import static java.util.Arrays.stream;
import static java.util.stream.Stream.of;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.freki.CursorAccessPatternTracer.NO_TRACING;
import static org.neo4j.internal.freki.FrekiMainStoreCursor.NULL;
<<<<<<< HEAD
import static org.neo4j.internal.freki.MutableNodeRecordData.sizeExponentialFromRecordPointer;
=======
import static org.neo4j.internal.freki.Header.FLAG_HAS_DENSE_RELATIONSHIPS;
import static org.neo4j.internal.freki.Header.FLAG_LABELS;
import static org.neo4j.internal.freki.Header.OFFSET_DEGREES;
import static org.neo4j.internal.freki.Header.OFFSET_PROPERTIES;
import static org.neo4j.internal.freki.Header.OFFSET_RECORD_POINTER;
import static org.neo4j.internal.freki.Header.OFFSET_RELATIONSHIPS;
import static org.neo4j.internal.freki.MutableNodeData.buildRecordPointer;
import static org.neo4j.internal.freki.MutableNodeData.forwardPointer;
import static org.neo4j.internal.freki.MutableNodeData.idFromRecordPointer;
import static org.neo4j.internal.freki.MutableNodeData.readRecordPointers;
import static org.neo4j.internal.freki.MutableNodeData.sizeExponentialFromRecordPointer;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
import static org.neo4j.internal.freki.Record.FLAG_IN_USE;
import static org.neo4j.internal.freki.Record.recordSize;
import static org.neo4j.internal.freki.Record.recordXFactor;
import static org.neo4j.io.ByteUnit.bytesToString;
import static org.neo4j.storageengine.api.RelationshipDirection.INCOMING;
import static org.neo4j.storageengine.api.RelationshipDirection.LOOP;
import static org.neo4j.storageengine.api.RelationshipDirection.OUTGOING;
import static org.neo4j.storageengine.api.RelationshipSelection.ALL_RELATIONSHIPS;
<<<<<<< HEAD
import static org.neo4j.token.TokenHolders.readOnlyTokenHolders;
=======
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa

public class FrekiAnalysis extends Life implements AutoCloseable
{
    private final MainStores stores;
<<<<<<< HEAD
=======
    private final PrintStream out;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    private final FrekiCursorFactory cursorFactory;

    public FrekiAnalysis( FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache ) throws IOException
    {
<<<<<<< HEAD
        this( new Stores( fs, databaseLayout, pageCache, new DefaultIdGeneratorFactory( fs, immediate() ), PageCacheTracer.NULL,
                immediate(), false, new StandardConstraintRuleAccessor(), i -> i, readOnlyTokenHolders() ), true,
                stores -> new FrekiCursorFactory( stores, NO_TRACING ) );
=======
        this( new Stores( fs, databaseLayout, pageCache, new DefaultIdGeneratorFactory( fs, immediate() ), PageCacheTracer.NULL, immediate(), false,
                        new StandardConstraintRuleAccessor(), i -> i, EmptyMemoryTracker.INSTANCE ), true,
                stores -> new FrekiCursorFactory( stores, NO_TRACING ), System.out );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    }

    public FrekiAnalysis( MainStores stores )
    {
        this( stores, new FrekiCursorFactory( stores, NO_TRACING ) );
    }

    public FrekiAnalysis( MainStores stores, FrekiCursorFactory cursorFactory )
    {
<<<<<<< HEAD
        this( stores, false, s -> cursorFactory );
    }

    FrekiAnalysis( MainStores stores, boolean manageStoreLifeToo, Function<MainStores,FrekiCursorFactory> cursorFactory )
    {
        this.stores = stores;
=======
        this( stores, false, s -> cursorFactory, System.out );
    }

    FrekiAnalysis( MainStores stores, boolean manageStoreLifeToo, Function<MainStores,FrekiCursorFactory> cursorFactory, PrintStream out )
    {
        this.stores = stores;
        this.out = out;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
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
<<<<<<< HEAD
              var propertyCursor = cursorFactory.allocatePropertyCursor( PageCursorTracer.NULL ) )
=======
              var propertyCursor = cursorFactory.allocatePropertyCursor( PageCursorTracer.NULL, EmptyMemoryTracker.INSTANCE ) )
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
        {
            cursor.single( relId );
            if ( cursor.next() )
            {
<<<<<<< HEAD
                System.out.printf( "%d -[%d]-> %d %n", cursor.sourceNodeReference(), cursor.type(), cursor.targetNodeReference() );
=======
                out.printf( "%d -[%d]-> %d %n", cursor.sourceNodeReference(), cursor.type(), cursor.targetNodeReference() );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
                cursor.properties( propertyCursor );
                dumpProperties( propertyCursor );
            }
            else
            {
<<<<<<< HEAD
                System.out.println( "Not found" );
=======
                out.println( "Not found" );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
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

<<<<<<< HEAD
    private void dumpAllNodes()
    {
        try ( var nodeCursor = cursorFactory.allocateNodeCursor( PageCursorTracer.NULL );
              var propertyCursor = cursorFactory.allocatePropertyCursor( PageCursorTracer.NULL );
              var relationshipCursor = cursorFactory.allocateRelationshipTraversalCursor( PageCursorTracer.NULL ) )
=======
    void dumpAllNodes()
    {
        try ( var nodeCursor = cursorFactory.allocateNodeCursor( PageCursorTracer.NULL  );
              var propertyCursor = cursorFactory.allocatePropertyCursor( PageCursorTracer.NULL, EmptyMemoryTracker.INSTANCE );
              var relationshipCursor = cursorFactory.allocateRelationshipTraversalCursor( PageCursorTracer.NULL  ) )
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
        {
            nodeCursor.scan();
            while ( nodeCursor.next() )
            {
                dumpNode( nodeCursor, propertyCursor, relationshipCursor );
            }
        }
    }

<<<<<<< HEAD
    public void dumpNodes( long... nodeIds )
    {
        try ( var nodeCursor = cursorFactory.allocateNodeCursor( PageCursorTracer.NULL );
                var propertyCursor = cursorFactory.allocatePropertyCursor( PageCursorTracer.NULL );
                var relationshipCursor = cursorFactory.allocateRelationshipTraversalCursor( PageCursorTracer.NULL ) )
=======
    void dumpNodes( long... nodeIds )
    {
        try ( var nodeCursor = cursorFactory.allocateNodeCursor( PageCursorTracer.NULL  );
                var propertyCursor = cursorFactory.allocatePropertyCursor( PageCursorTracer.NULL, EmptyMemoryTracker.INSTANCE );
                var relationshipCursor = cursorFactory.allocateRelationshipTraversalCursor( PageCursorTracer.NULL  ) )
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
        {
            for ( long nodeId : nodeIds )
            {
                dumpNode( nodeId, nodeCursor, propertyCursor, relationshipCursor );
            }
        }
    }

<<<<<<< HEAD
    public void dumpNodeRange( long fromNodeId, long toNodeId )
    {
        try ( var nodeCursor = cursorFactory.allocateNodeCursor( PageCursorTracer.NULL );
              var propertyCursor = cursorFactory.allocatePropertyCursor( PageCursorTracer.NULL );
              var relationshipCursor = cursorFactory.allocateRelationshipTraversalCursor( PageCursorTracer.NULL ) )
=======
    void dumpNodeRange( long fromNodeId, long toNodeId )
    {
        try ( var nodeCursor = cursorFactory.allocateNodeCursor( PageCursorTracer.NULL  );
              var propertyCursor = cursorFactory.allocatePropertyCursor( PageCursorTracer.NULL, EmptyMemoryTracker.INSTANCE );
              var relationshipCursor = cursorFactory.allocateRelationshipTraversalCursor( PageCursorTracer.NULL  ) )
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
        {
            for ( long nodeId = fromNodeId; nodeId < toNodeId; nodeId++ )
            {
                dumpNode( nodeId, nodeCursor, propertyCursor, relationshipCursor );
            }
        }
    }

<<<<<<< HEAD
    public void dumpNode( long nodeId, FrekiNodeCursor nodeCursor, FrekiPropertyCursor propertyCursor, FrekiRelationshipTraversalCursor relationshipCursor )
=======
    void dumpNode( long nodeId, FrekiNodeCursor nodeCursor, FrekiPropertyCursor propertyCursor, FrekiRelationshipTraversalCursor relationshipCursor )
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    {
        nodeCursor.single( nodeId );
        if ( !nodeCursor.next() )
        {
<<<<<<< HEAD
            System.out.println( "Node " + nodeId + " not in use" );
=======
            out.println( "Node " + nodeId + " not in use" );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
            return;
        }
        dumpNode( nodeCursor, propertyCursor, relationshipCursor );
    }

<<<<<<< HEAD
    private void dumpNode( FrekiNodeCursor nodeCursor, FrekiPropertyCursor propertyCursor, FrekiRelationshipTraversalCursor relationshipCursor )
=======
    void dumpNode( FrekiNodeCursor nodeCursor, FrekiPropertyCursor propertyCursor, FrekiRelationshipTraversalCursor relationshipCursor )
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    {
        dumpLogicalRepresentation( nodeCursor, propertyCursor, relationshipCursor );

        // More physical
<<<<<<< HEAD
        System.out.println( nodeCursor.toString() );
        printRawRecordContents( nodeCursor.data.records[0], 0 );
        if ( nodeCursor.data.forwardPointer != NULL )
        {
            int sizeExp = sizeExponentialFromRecordPointer( nodeCursor.data.forwardPointer );
            printRawRecordContents( nodeCursor.data.records[sizeExp], nodeCursor.entityReference() );
        }
    }

    private void printRawRecordContents( Record record, long nodeId )
    {
        System.out.println( record );
        MutableNodeRecordData data = new MutableNodeRecordData( nodeId );
        data.deserialize( record.data( 0 ), stores.bigPropertyValueStore, PageCursorTracer.NULL );
        System.out.println( "  " + data );
    }

    public void dumpLogicalRepresentation( FrekiNodeCursor nodeCursor, FrekiPropertyCursor propertyCursor,
            FrekiRelationshipTraversalCursor relationshipCursor )
    {
        var nodeId = nodeCursor.entityReference();
        System.out.printf( "Node[%d] %s%n", nodeId, nodeCursor );
        System.out.printf( "  labels:%s%n", Arrays.toString( nodeCursor.labels() ) );
=======
        out.println( nodeCursor.toString() );
        printRawRecordContents( nodeCursor.data.records[0][0], 0 );
        if ( nodeCursor.data.xLChainStartPointer != NULL )
        {
            long id = nodeCursor.data.nodeId;
            //Force full reload, to step XLChain manually
            nodeCursor.reset();
            nodeCursor.single( id );
            nodeCursor.next();
            while ( nodeCursor.data.xLChainNextLinkPointer != NULL )
            {
                int sizeExp = sizeExponentialFromRecordPointer( nodeCursor.data.xLChainNextLinkPointer );
                printRawRecordContents( nodeCursor.data.records[sizeExp][nodeCursor.data.recordIndex[sizeExp]], nodeCursor.entityReference() );
                nodeCursor.loadNextChainLink();
            }
        }
    }

    public void dumpPhysicalPartsLayout( long nodeId )
    {
        visitPhysicalPartsLayout( nodeId, new PhysicalPartsLayoutPrinter( out ) );
    }

    public <V extends PhysicalPartsLayoutVisitor> V visitPhysicalPartsLayout( long nodeId, V visitor )
    {
        Header header = new Header();
        long pointer = buildRecordPointer( 0, nodeId );
        boolean first = true;
        while ( pointer != NULL )
        {
            int sizeExp = sizeExponentialFromRecordPointer( pointer );
            long id = idFromRecordPointer( pointer );
            Record record = loadRecord( id, sizeExp );
            header.deserialize( record.data( 0 ) );
            pointer = header.hasMark( OFFSET_RECORD_POINTER ) ?
                      forwardPointer( readRecordPointers( record.data( header.getOffset( OFFSET_RECORD_POINTER ) ) ), record.sizeExp() > 0 ) : NULL;
            visitor.xRecord( sizeExp, id, header, first, pointer == NULL );
            first = false;
        }
        return visitor;
    }

    void printRawRecordContents( Record record, long nodeId )
    {
        out.println( record );
        MutableNodeData data = new MutableNodeData( nodeId, stores.bigPropertyValueStore, PageCursorTracer.NULL );
        data.deserialize( record );
        out.println( "  " + data );
    }

    void dumpLogicalRepresentation( FrekiNodeCursor nodeCursor, FrekiPropertyCursor propertyCursor,
            FrekiRelationshipTraversalCursor relationshipCursor )
    {
        var nodeId = nodeCursor.entityReference();
        out.printf( "Node[%d] %s%n", nodeId, nodeCursor );
        out.printf( "  labels:%s%n", Arrays.toString( nodeCursor.labels() ) );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa

        nodeCursor.properties( propertyCursor );
        dumpProperties( propertyCursor );

<<<<<<< HEAD
        System.out.println( "  relationships..." );
=======
        out.println( "  relationships..." );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
        nodeCursor.relationships( relationshipCursor, ALL_RELATIONSHIPS );
        while ( relationshipCursor.next() )
        {
            var direction = relationshipCursor.sourceNodeReference() == relationshipCursor.targetNodeReference() ? LOOP :
                            relationshipCursor.sourceNodeReference() == nodeId ? OUTGOING : INCOMING;
<<<<<<< HEAD
            System.out.printf( "  (%d)%s[:%d,%d]%s(%d)%n", relationshipCursor.originNodeReference(),
=======
            out.printf( "  (%d)%s[:%d,%d]%s(%d)%n", relationshipCursor.originNodeReference(),
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
                    direction == LOOP ? "--" : direction == OUTGOING ? "--" : "<-", relationshipCursor.type(),
                    relationshipCursor.entityReference(),
                    direction == LOOP ? "--" : direction == OUTGOING ? "->" : "--", relationshipCursor.neighbourNodeReference() );
        }
    }

<<<<<<< HEAD
    private void dumpProperties( FrekiPropertyCursor propertyCursor )
    {
        System.out.println( "  properties..." );
        while ( propertyCursor.next() )
        {
            System.out.printf( "  %d=%s%n", propertyCursor.propertyKey(), propertyCursor.propertyValue() );
        }
    }

=======
    void dumpProperties( FrekiPropertyCursor propertyCursor )
    {
        out.println( "  properties..." );
        while ( propertyCursor.next() )
        {
            out.printf( "  %d=%s%n", propertyCursor.propertyKey(), propertyCursor.propertyValue() );
        }
    }

    public boolean nodeIsDense( long nodeId )
    {
        Header header = new Header();
        header.deserialize( loadRecord( nodeId, 0 ).data( 0 ) );
        return header.hasMark( FLAG_HAS_DENSE_RELATIONSHIPS );
    }

>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
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

<<<<<<< HEAD
    public void dumpRecord( long id, int sizeExp )
=======
    void dumpRecord( long id, int sizeExp )
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
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

<<<<<<< HEAD
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
=======
    void dumpRecord( Record record )
    {
        out.println( record );
        MutableNodeData data = new MutableNodeData( -1, stores.bigPropertyValueStore, PageCursorTracer.NULL );
        data.deserialize( record );
        out.println( data );
    }

    public void dumpStoreStats()
    {
        // Sparse store stats
        out.println( "Calculating main store stats ..." );
        var storeStats = gatherStoreStats();
        var totalNumDenseNodes = of( storeStats ).mapToLong( s -> s.numDenseNodes ).sum();
        printPercents( "  Record sizes distribution", storeStats, ( stats, stat ) ->
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
        {
            long numRecords = stat == stats[0]
                    // For x1 we're interested in nodes that are ONLY in x1
                    ? stat.usedRecords - stream( stats ).filter( s -> s != stats[0] ).mapToLong( s -> s.usedRecords ).sum()
                    : stat.usedRecords;
            return (double) numRecords / stats[0].usedRecords;
        }, true );
<<<<<<< HEAD
        System.out.printf( " (DE: %.2f%%)%n", percent( totalNumDenseNodes, storeStats[0].usedRecords ) );
        printPercents( "Record occupancy i.e. on average how much of the record is actual useful data", storeStats, ( stats, stat ) ->
=======
        out.printf( "   (DE: %.2f%%)%n", percent( totalNumDenseNodes, storeStats[0].usedRecords ) );
        printPercents( "  Record chains distribution", storeStats, ( stats, stat ) ->
        {
            long numChainRecords = stream( stats ).mapToLong( s -> s.chainRecords ).sum();
            return (double) numChainRecords / stat.usedRecords;
        }, false );
        printPercents( "  Record occupancy i.e. on average how much of the record is actual useful data", storeStats, ( stats, stat ) ->
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
                (double) stat.bytesOccupiedInUsedRecords / (stat.bytesOccupiedInUsedRecords + stat.bytesVacantInUsedRecords), false );

        var totalOccupied = of( storeStats ).mapToLong( s -> s.bytesOccupiedInUsedRecords ).sum();
        var totalVacant = of( storeStats ).mapToLong( s -> s.bytesVacantInUsedRecords + (s.unusedRecords * recordSize( s.sizeExp )) ).sum();
        var totalVacantInUsedRecords = of( storeStats ).mapToLong( s -> s.bytesVacantInUsedRecords ).sum();
        var totalPossibleOccupied = of( storeStats ).mapToLong( s -> s.usedRecords * recordSize( s.sizeExp ) ).sum();
        var total = of( storeStats ).mapToLong( s -> (s.usedRecords + s.unusedRecords) * recordSize( s.sizeExp ) ).sum();

<<<<<<< HEAD
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
=======
        out.printf( "  Total occupied/vacant bytes in used records %s/%s (%.2f%%/%.2f%%)%n",
                bytesToString( totalOccupied ), bytesToString( totalVacantInUsedRecords ),
                percent( totalOccupied, totalPossibleOccupied ), percent( totalVacantInUsedRecords, totalPossibleOccupied ) );
        out.printf( "  Total occupied/vacant bytes %s/%s (%.2f%%/%.2f%%)%n",
                bytesToString( totalOccupied ), bytesToString( totalVacant ),
                percent( totalOccupied, total ), percent( totalVacant, total ) );

        // Dense store stats
        out.println();
        out.println( "Calculating dense store stats ..." );
        DenseRelationshipStore.Stats denseStats = stores.denseStore.gatherStats( PageCursorTracer.NULL );
        out.printf( "  Total dense store file size: %s%n", bytesToString( denseStats.totalTreeByteSize() ) );
        out.printf( "  Effective dense store data size: %s%n", bytesToString( denseStats.effectiveByteSize() ) );
        out.printf( "  Number of dense nodes: %d%n", denseStats.numberOfNodes() );
        out.printf( "  Avg number of relationships per dense node: %.2f%n", (double) denseStats.numberOfRelationships() / denseStats.numberOfNodes() );
        out.printf( "  Avg effective size per dense node: %.2f%n", (double) denseStats.effectiveByteSize() / denseStats.numberOfNodes() );
        out.printf( "  Avg effective size per relationship: %.2f%n",
                (double) denseStats.effectiveRelationshipsByteSize() / denseStats.numberOfRelationships() );
        out.printf( "  Avg total size per dense node: %.2f%n", (double) denseStats.totalTreeByteSize() / denseStats.numberOfNodes() );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa

        // - TODO Number of big values
        // - TODO Avg size of big value
    }

    static double percent( long part, long whole )
    {
        return 100D * part / whole;
    }

    private void printPercents( String title, StoreStats[] stats, BiFunction<StoreStats[],StoreStats,Double> calculator, boolean includeCumulative )
    {
<<<<<<< HEAD
        System.out.println( title + ":" );
        double cumulativePercent = 0;
        String format = "  x%d: %.2f%%" + (includeCumulative ? " = %.2f" : "") + "%n";
=======
        out.println( title + ":" );
        double cumulativePercent = 0;
        String format = "    x%d: %.2f%%" + (includeCumulative ? " = %.2f" : "") + "%n";
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
        for ( StoreStats stat : stats )
        {
            double percent = calculator.apply( stats, stat ) * 100d;
            cumulativePercent += percent;
            Object[] args = includeCumulative
                            ? new Object[]{recordXFactor( stat.sizeExp ), percent, cumulativePercent}
                            : new Object[]{recordXFactor( stat.sizeExp ), percent};
<<<<<<< HEAD
            System.out.printf( format, args );
        }
    }

    public StoreStats[] gatherStoreStats()
=======
            out.printf( format, args );
        }
    }

    private StoreStats[] gatherStoreStats()
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
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
<<<<<<< HEAD
=======
                    var header = new Header();
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
                    try ( var cursor = store.openReadCursor( PageCursorTracer.NULL ) )
                    {
                        for ( var id = 0; id < highId; id++ )
                        {
                            if ( store.read( cursor, record, id ) && record.hasFlag( FLAG_IN_USE ) )
                            {
                                stats.usedRecords++;
<<<<<<< HEAD
                                var data = new MutableNodeRecordData( id );
                                var buffer = record.data();
                                try
                                {
                                    data.deserialize( buffer, stores.bigPropertyValueStore, PageCursorTracer.NULL );
=======
                                var data = new MutableNodeData( id, stores.bigPropertyValueStore, PageCursorTracer.NULL );
                                try
                                {
                                    header = data.deserialize( record );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
                                }
                                catch ( Exception e )
                                {
                                    System.err.println( "Caught exception when processing id " + id + " in store x" +
                                            recordXFactor( store.recordSizeExponential() ) );
                                    throw e;
                                }
<<<<<<< HEAD
                                stats.bytesOccupiedInUsedRecords += Record.HEADER_SIZE + buffer.position();
                                stats.bytesVacantInUsedRecords += recordDataSize - buffer.position();
                                if ( data.isDense() )
                                {
                                    stats.numDenseNodes++;
                                }
=======
                                int endOffset = header.getOffset( Header.OFFSET_END );
                                stats.bytesOccupiedInUsedRecords += Record.HEADER_SIZE + endOffset;
                                stats.bytesVacantInUsedRecords += recordDataSize - endOffset;
                                if ( header.hasMark( Header.FLAG_HAS_DENSE_RELATIONSHIPS ) )
                                {
                                    stats.numDenseNodes++;
                                }
                                if ( isSplit( header, FLAG_LABELS ) || isSplit( header, OFFSET_PROPERTIES ) || isSplit( header, OFFSET_DEGREES ) )
                                {
                                    stats.chainRecords++;
                                }
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
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

<<<<<<< HEAD
    public boolean nodeIsDense( long nodeId )
    {
        var record = loadRecord( nodeId, 0 );
        if ( record.hasFlag( FLAG_IN_USE ) )
        {
            var data = new MutableNodeRecordData( nodeId );
            data.deserialize( record.data(), stores.bigPropertyValueStore, PageCursorTracer.NULL );
            return data.isDense();
        }
        return false;
=======
    private static boolean isSplit( Header header, int slot )
    {
        return header.hasMark( slot ) && header.hasReferenceMark( slot );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
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
<<<<<<< HEAD
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

=======
            out.println( "Please instantiate FrekiAnalysis with a full Stores to get this feature" );
        }
    }

    void dumpTokens( GBPTreeTokenStore tokenStore, String name ) throws IOException
    {
        out.println( name );
        for ( NamedToken token : tokenStore.loadTokens( PageCursorTracer.NULL ) )
        {
            out.println( "  " + token.id() + ": " + token.name() );
        }
    }

    FrekiAnalysis forOutput( PrintStream out )
    {
        return new FrekiAnalysis( stores, false, stores -> cursorFactory, out );
    }

    public String captureOutput( Consumer<FrekiAnalysis> action )
    {
        ByteArrayOutputStream byteArrayOut = new ByteArrayOutputStream();
        PrintStream capturedOut = new PrintStream( byteArrayOut );
        action.accept( forOutput( capturedOut ) );
        capturedOut.close();
        return byteArrayOut.toString();
    }

>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
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
<<<<<<< HEAD
=======
        private long chainRecords;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa

        StoreStats( int sizeExp )
        {
            this.sizeExp = sizeExp;
        }
    }
<<<<<<< HEAD
=======

    public interface PhysicalPartsLayoutVisitor
    {
        void xRecord( int sizeExp, long id, Header header, boolean first, boolean last );
    }

    // e.g. X1(12):L -> X8(19):R -> X4(243):P -> DENSE
    private static class PhysicalPartsLayoutPrinter implements PhysicalPartsLayoutVisitor
    {
        private final StringBuilder builder = new StringBuilder();
        private final PrintStream out;
        private boolean isDense;

        PhysicalPartsLayoutPrinter( PrintStream out )
        {
            this.out = out;
        }

        @Override
        public void xRecord( int sizeExp, long id, Header header, boolean first, boolean last )
        {
            builder.append( !first ? " -> " : "" );
            builder.append( format( "X%d(%d):", recordXFactor( sizeExp ), id ) );
            appendPhysicalPart( builder, header, FLAG_LABELS, 'L' );
            appendPhysicalPart( builder, header, OFFSET_PROPERTIES, 'P' );
            appendPhysicalPart( builder, header, OFFSET_RELATIONSHIPS, 'R' );
            appendPhysicalPart( builder, header, OFFSET_DEGREES, 'D' );
            isDense = header.hasMark( FLAG_HAS_DENSE_RELATIONSHIPS );
            if ( last )
            {
                builder.append( isDense ? " -> DENSE" : "" );
                out.println( builder );
            }
        }

        private void appendPhysicalPart( StringBuilder builder, Header header, int slot, char part )
        {
            if ( header.hasMark( slot ) )
            {
                builder.append( part );
            }
        }
    }
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
}
