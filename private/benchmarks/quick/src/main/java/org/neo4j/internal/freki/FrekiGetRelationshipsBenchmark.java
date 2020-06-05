/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package org.neo4j.internal.freki;

import com.neo4j.bench.quick.QuickBenchmark;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.RunnerException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.neo4j.graphdb.Direction;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.util.Preconditions;

import static java.lang.Integer.parseInt;
import static org.neo4j.internal.freki.FrekiStorageEngineFactory.instantiateStandalone;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier.EMPTY;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;
import static org.neo4j.kernel.lifecycle.LifecycleAdapter.onShutdown;
import static org.neo4j.lock.ResourceLocker.IGNORE;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.RelationshipSelection.selection;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

@State( Scope.Benchmark )
@OutputTimeUnit( TimeUnit.NANOSECONDS )
public class FrekiGetRelationshipsBenchmark
{
    private static final int SELECTED_TYPE = 0;
    private static final boolean SELECTED_DIRECTION = true;

    @Param( "freki-benchmark-db" )
    public String dbDirectory;

    @Param( {"1", "10", "100"} )
    public String numSelectedRelationships;

    @Param( {"0", "1", "10", "100"} )
    public String numRelationshipsOfSameTypeButOtherDirection;

    @Param( {"0", "1", "10", "100"} )
    public String numOtherTypes;

    @Param( {"0", "1", "10", "100"} )
    public String numRelationshipsPerOtherTypeGroup;

    @Param( {"0", "10000", "1000000"} )
    public String numOtherRelationshipsInDenseStore;

    @Benchmark
    @BenchmarkMode( {Mode.AverageTime} )
    public void getRelationshipsOfTypeAndDirection( StateClass state, Blackhole hole )
    {
        FrekiRelationshipTraversalCursor cursor = state.relationshipCursor;
        state.nodeCursor.relationships( cursor, selection( SELECTED_TYPE, SELECTED_DIRECTION ? Direction.OUTGOING : Direction.INCOMING ) );
        while ( cursor.next() )
        {
            hole.consume( cursor.entityReference() );
        }
    }

    @State( Scope.Benchmark )
    public static class StateClass
    {
        LifeSupport life;
        FrekiStorageEngine storageEngine;
        FrekiNodeCursor nodeCursor;
        FrekiRelationshipTraversalCursor relationshipCursor;
        long nodeId;

        @Setup( Level.Trial )
        public void createData( FrekiGetRelationshipsBenchmark benchmark ) throws Exception
        {
            // Start the database, create the data
            File baseDir = new File( benchmark.dbDirectory );
            baseDir.mkdirs();
            File dbDirectory = new File( baseDir, "dbdir" );
            FileUtils.deleteRecursively( dbDirectory );
            life = new LifeSupport();
            FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
            JobScheduler scheduler = life.add( createInitialisedScheduler() );
            PageCache pageCache = new MuninnPageCache( new SingleFilePageSwapperFactory( fs ), 10_000, NULL, EMPTY, scheduler );
            life.add( onShutdown( pageCache::close ) );
            storageEngine = life.add( instantiateStandalone( fs, DatabaseLayout.ofFlat( dbDirectory ), pageCache, nullLogProvider() ) );
            life.start();

            // Create the data to benchmark
            nodeId = insertData(
                    parseInt( benchmark.numSelectedRelationships ),
                    parseInt( benchmark.numRelationshipsOfSameTypeButOtherDirection ),
                    parseInt( benchmark.numOtherTypes ),
                    parseInt( benchmark.numRelationshipsPerOtherTypeGroup ),
                    parseInt( benchmark.numOtherRelationshipsInDenseStore ) );

            System.out.printf( "LAYOUT: %s,%s,%s,%s,%s: %s%n", benchmark.numOtherRelationshipsInDenseStore, benchmark.numOtherTypes,
                    benchmark.numRelationshipsOfSameTypeButOtherDirection, benchmark.numRelationshipsPerOtherTypeGroup, benchmark.numSelectedRelationships,
                    storageEngine.analysis().captureOutput( a -> a.dumpPhysicalPartsLayout( nodeId ) ) );

            // Allocate cursors
            try ( FrekiStorageReader reader = storageEngine.newReader() )
            {
                nodeCursor = reader.allocateNodeCursor( PageCursorTracer.NULL );
                relationshipCursor = reader.allocateRelationshipTraversalCursor( PageCursorTracer.NULL );
            }
        }

        private long insertData( int numSelectedRelationships, int numRelationshipsOfSameTypeButOtherDirection, int numOtherTypes,
                int numRelationshipsPerOtherTypeGroup, int numOtherRelationshipsInDenseStore ) throws Exception
        {
            if ( (numRelationshipsPerOtherTypeGroup > 0 || numOtherTypes > 0) && numRelationshipsPerOtherTypeGroup * numOtherTypes == 0 )
            {
                throw new IllegalArgumentException( "Unnecessary permutation of numOtherTypes=" + numOtherTypes +
                        ", numRelationshipsPerOtherTypeGroup=" + numRelationshipsPerOtherTypeGroup );
            }

            TxState txState = new TxState();
            CommandCreationContext creationContext = storageEngine.newCommandCreationContext( PageCursorTracer.NULL, INSTANCE );
            if ( numOtherRelationshipsInDenseStore > 0 )
            {
                // If we should have other dense nodes in the store then create half of that amount before the node, and half after it
                // so that we can't find the relationships by best-case binary-search in the tree
                long preDenseNodeId = createNode( txState, creationContext );
                createRelationships( txState, creationContext, preDenseNodeId, SELECTED_TYPE, true, numOtherRelationshipsInDenseStore / 2 );
            }

            long nodeId = createNode( txState, creationContext );
            createRelationships( txState, creationContext, nodeId, 0, SELECTED_DIRECTION, numSelectedRelationships );
            createRelationships( txState, creationContext, nodeId, 0, !SELECTED_DIRECTION, numRelationshipsOfSameTypeButOtherDirection );
            for ( int i = 0; i < numOtherTypes; i++ )
            {
                createRelationships( txState, creationContext, nodeId, 1 + i, SELECTED_DIRECTION, numRelationshipsPerOtherTypeGroup );
            }

            if ( numOtherRelationshipsInDenseStore > 0 )
            {
                long postDenseNodeId = createNode( txState, creationContext );
                createRelationships( txState, creationContext, postDenseNodeId, SELECTED_TYPE, true, numOtherRelationshipsInDenseStore / 2 );
            }

            try ( FrekiStorageReader reader = storageEngine.newReader() )
            {
                List<StorageCommand> commands = new ArrayList<>();
                storageEngine.createCommands( commands, txState, reader, creationContext, IGNORE, BASE_TX_ID, t -> t, PageCursorTracer.NULL );
                PhysicalTransactionRepresentation tx = new PhysicalTransactionRepresentation( commands );
                tx.setHeader( new byte[0], 0, 0, 0, 0 );
                storageEngine.apply( new TransactionToApply( tx, PageCursorTracer.NULL ), TransactionApplicationMode.EXTERNAL );
            }

            if ( numOtherRelationshipsInDenseStore > 0 && !storageEngine.analysis().nodeIsDense( nodeId ) )
            {
                throw new IllegalArgumentException( "Unnecessary permutation for non-dense node" );
            }

            return nodeId;
        }

        @Setup( Level.Invocation )
        public void setupCursor()
        {
            nodeCursor.single( nodeId );
            nodeCursor.next();
            // ... and let the benchmark initialize the relationship cursor itself and iterate over the relationships
        }

        private long createNode( TxState txState, CommandCreationContext creationContext )
        {
            long nodeId = creationContext.reserveNode();
            txState.nodeDoCreate( nodeId );
            return nodeId;
        }

        private void createRelationships( TxState txState, CommandCreationContext creationContext, long nodeId, int typeId, boolean outgoing, int count )
        {
            for ( int i = 0; i < count; i++ )
            {
                createRelationship( txState, creationContext, nodeId, typeId, outgoing );
            }
        }

        private void createRelationship( TxState txState, CommandCreationContext creationContext, long nodeId, int typeId, boolean outgoing )
        {
            long otherNode = createNode( txState, creationContext );
            long startNode = outgoing ? nodeId : otherNode;
            long endNode = outgoing ? otherNode : nodeId;
            txState.relationshipDoCreate( creationContext.reserveRelationship( startNode ), typeId, startNode, endNode );
        }

        @TearDown
        public void tearDown()
        {
            life.stop();
        }
    }

    public static void main( String[] args ) throws RunnerException
    {
        QuickBenchmark.benchmark().forks( 1 ).include( FrekiGetRelationshipsBenchmark.class ).iterations( 1, 1 ).run();
    }

    // Aggregates results with physical node layouts, all from the output of the run (each benchmark outputs physical node layout)
    public static void aggregateWithLayouts( Path path ) throws IOException
    {
        Map<String,String> layouts = new HashMap<>();
        try ( Stream<String> lines = Files.lines( path ) )
        {
            lines.forEach( line ->
            {
                if ( line.startsWith( "LAYOUT: " ) )
                {
                    String permutation = line.substring( 8, line.indexOf( 'X' ) - 2 );
                    String layout = line.substring( line.indexOf( 'X' ) );
                    String prev = layouts.put( permutation, layout );
                    Preconditions.checkState( prev == null, "Duplicate permutation %s", permutation );
                }
                else if ( line.startsWith( "FrekiGetRelationshipsBenchmark.getRelationshipsOfTypeAndDirection " ) )
                {
                    int startOfPermutationThing = line.indexOf( "freki-benchmark-db" ) + "freki-benchmark-db".length();
                    StringBuilder benchmarkPermutationBuilder = new StringBuilder();
                    for ( int i = 0; i < 5; i++ )
                    {
                        startOfPermutationThing = addNextPermutationParameter( startOfPermutationThing, line, benchmarkPermutationBuilder );
                    }
                    String benchmarkPermutation = benchmarkPermutationBuilder.toString();
                    String layout = layouts.get( benchmarkPermutation );
                    Preconditions.checkState( layout != null, "Expected permutation to exist %s", benchmarkPermutation );
                    System.out.println( line + " " + layout );
                }
                else if ( line.startsWith( "Benchmark       " ) )
                {
                    System.out.println( line );
                }
            } );
        }
    }

    private static int addNextPermutationParameter( int index, String line, StringBuilder benchmarkPermutationBuilder )
    {
        while ( Character.isWhitespace( line.charAt( index ) ) )
        {
            index++;
        }
        int startIndex = index;
        while ( !Character.isWhitespace( line.charAt( index ) ) )
        {
            index++;
        }
        benchmarkPermutationBuilder.append( benchmarkPermutationBuilder.length() > 0 ? "," : "" ).append( line.substring( startIndex, index ) );
        return index;
    }
}
