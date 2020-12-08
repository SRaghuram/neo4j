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
package org.neo4j.graphdb;

import org.assertj.core.description.Description;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.platform.commons.util.ExceptionUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.core.NodeEntity;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.Barrier;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.Race;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static java.lang.String.format;
import static java.util.concurrent.ConcurrentHashMap.newKeySet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.FeatureState.ENABLED;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.internal.helpers.progress.ProgressMonitorFactory.NONE;
import static org.neo4j.kernel.impl.MyRelTypes.TEST;
import static org.neo4j.kernel.impl.MyRelTypes.TEST2;
import static org.neo4j.kernel.impl.store.record.Record.isNull;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;
import static org.neo4j.test.OtherThreadExecutor.command;
import static org.neo4j.test.Race.throwing;

@ImpermanentDbmsExtension( configurationCallback = "configure" )
@ExtendWith( RandomExtension.class )
@TestInstance( PER_CLASS )
class DenseNodeConcurrencyIT
{
    private static final int NUM_INITIAL_RELATIONSHIPS_PER_DENSE_NODE = 500;
    private static final int NUM_INITIAL_RELATIONSHIPS_PER_SPARSE_NODE = 10;
    private static final int NUM_DENSE_NODES_IN_MULTI_SETUP = 10;
    private static final int NUM_TASKS = 1_000;
    private static final RelationshipType INITIAL_DENSE_NODE_TYPE = TEST2;
    private final Label INITIAL_LABEL = Label.label( "INITIAL" );

    @Inject
    DatabaseManagementService dbms;

    @Inject
    GraphDatabaseAPI database;

    @Inject
    FileSystemAbstraction fs; // this is the actual ephemeral file system, we close it ourselves

    @Inject
    RandomRule random;

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( GraphDatabaseInternalSettings.relaxed_dense_node_locking, ENABLED );
        builder.setFileSystem( new UncloseableDelegatingFileSystemAbstraction( builder.getFileSystem() ) );
    }

    @AfterAll
    void consistencyCheck() throws ConsistencyCheckIncompleteException, IOException
    {
        try
        {
            DependencyResolver deps = database.getDependencyResolver();
            Config config = deps.resolveDependency( Config.class );
            DatabaseLayout databaseLayout = database.databaseLayout();
            dbms.shutdown();
            ConsistencyCheckService.Result result = new ConsistencyCheckService().runFullConsistencyCheck( databaseLayout, config, NONE, nullLogProvider(),
                    deps.resolveDependency( FileSystemAbstraction.class ), false, ConsistencyFlags.DEFAULT );
            assertThat( result.isSuccessful() ).as( new Description()
            {
                @Override
                public String value()
                {
                    return consistencyCheckReportAsString( result );
                }
            } ).isTrue();
        }
        finally
        {
            fs.close();
        }
    }

    @Test
    void shouldHandleDetachDeleteWithConcurrentAdds()
    {
        long initialNodes;
        long initialRelationships;
        try ( Transaction tx = database.beginTx() )
        {
            initialNodes = Iterables.count( tx.getAllNodes() );
            initialRelationships = Iterables.count( tx.getAllRelationships() );
        }

        long denseNodeToDelete = createNode( new HashSet<>(), 100 );
        AtomicInteger numNodes = new AtomicInteger( 2 );
        int numModifiers = Runtime.getRuntime().availableProcessors() - 1;
        int numCreators = Math.max( numModifiers * 2 / 3, 2 );
        int numDeleters = Math.max( numModifiers / 3, 1 );
        CountDownLatch latch = new CountDownLatch( numCreators * 2 );
        AtomicBoolean done = new AtomicBoolean();
        Race race = new Race().withFailureAction( t -> done.set( true ) );
        race.addContestants( 1, throwing( () ->
        {
            latch.await();
            try ( Transaction tx = database.beginTx() )
            {
                InternalTransaction internalTx = (InternalTransaction) tx;
                internalTx.kernelTransaction().dataWrite().nodeDetachDelete( denseNodeToDelete );
                tx.commit();
                numNodes.decrementAndGet();
            }
            done.set( true );
        } ) );
        race.addContestants( numCreators, throwing( () ->
        {
            try
            {
                int creations = 0;
                while ( !done.get() && creations < 20 )
                {
                    latch.countDown();
                    try ( Transaction tx = database.beginTx() )
                    {
                        Node denseNode = tx.getNodeById( denseNodeToDelete );
                        var type = random.nextBoolean() ? TEST : TEST2;
                        switch ( random.nextInt( 3 ) )
                        {
                        case 0:
                            denseNode.createRelationshipTo( tx.createNode(), type );
                            numNodes.incrementAndGet();
                            break;
                        case 1:
                            tx.createNode().createRelationshipTo( denseNode, type );
                            numNodes.incrementAndGet();
                        default:
                            denseNode.createRelationshipTo( denseNode, type );
                            break;
                        }
                        tx.commit();
                        creations++;
                        Thread.sleep( 1 ); // Provoke race, since we do not have fair locking, this will give a small windows to grab exclusive
                    }
                }
            }
            catch ( NotFoundException e )
            {
                //expected, exit!
            }
        } ) );
        race.addContestants( numDeleters, throwing( () ->
        {
            try
            {
                while ( !done.get() )
                {
                    try ( Transaction tx = database.beginTx() )
                    {
                        Node denseNode = tx.getNodeById( denseNodeToDelete );
                        Relationship[] relationships = Iterables.asArray( Relationship.class, denseNode.getRelationships() );
                        if ( relationships.length > 0 )
                        {
                            random.among( relationships ).delete();
                        }
                        tx.commit();
                        Thread.sleep( 1 ); // Provoke race, since we do not have fair locking, this will give a small windows to grab exclusive
                    }
                }
            }
            catch ( NotFoundException e )
            {
                //expected, exit!
            }
        } ) );

        race.goUnchecked();

        try ( Transaction tx = database.beginTx() )
        {
            assertThat( Iterables.count( tx.getAllNodes() ) ).isEqualTo( numNodes.get() + initialNodes );
            assertThat( Iterables.count( tx.getAllRelationships() ) ).isEqualTo( initialRelationships );
            assertThatThrownBy( () -> tx.getNodeById( denseNodeToDelete ) ).isInstanceOf( NotFoundException.class );
        }
    }

    /**
     * @param multipleDenseNodes if {@code true} then multiple subject nodes are created and will get relationships created between each other during the test,
     * otherwise only one dense node is the test subject and other nodes are just small created nodes.
     * @param startAsDense if {@code true} then the subject node(s) will start as dense and has lots of relationships of the initial type,
     * otherwise the node(s) will start as sparse with only a few relationships.
     * @param multipleOperationsInOneTx if {@code true} then each transaction have a chance to do multiple operations,
     * otherwise each transaction only performs one operation.
     * @param multipleTypes if {@code true} then relationships will be either of several types, otherwise only the same type.
     * @param operationWeights chances of each type of operation happening in the test.
     */
    @MethodSource( "permutations" )
    @ParameterizedTest( name = "multipleDenseNodes:{0}, startAsDense:{1}, multipleOpsPerTx:{2}, multipleTypes:{3}, opWeights:{4}" )
    void shouldCreateAndDeleteRelationshipsConcurrently( boolean multipleDenseNodes, boolean startAsDense, boolean multipleOperationsInOneTx,
            boolean multipleTypes, int[] operationWeights )
    {
        // given
        Map<Long,Set<Relationship>> relationships = new ConcurrentHashMap<>();
        Set<Relationship> allRelationships = newKeySet();
        Set<Long> initialDenseNodes = new HashSet<>( createInitialNodes( multipleDenseNodes, startAsDense, relationships ) );
        Set<Long> denseNodeIds = ConcurrentHashMap.newKeySet();
        denseNodeIds.addAll( initialDenseNodes );
        relationships.forEach( ( nodeId, rels ) -> allRelationships.addAll( rels ) );

        // when
        Queue<WorkTask> workQueue = new ConcurrentLinkedDeque<>( createWork( operationWeights ) );
        Race race = new Race().withFailureAction( t -> workQueue.clear() );
        int numWorkers = Runtime.getRuntime().availableProcessors();
        List<RelationshipType> typesList = new ArrayList<>();
        typesList.add( INITIAL_DENSE_NODE_TYPE );
        if ( multipleTypes )
        {
            typesList.add( withName( "a" ) );
            typesList.add( withName( "b" ) );
        }
        RelationshipType[] types = typesList.toArray( new RelationshipType[0] );
        AtomicInteger numDeadlocks = new AtomicInteger();
        race.addContestants( numWorkers, throwing( () ->
        {
            WorkTask work;
            while ( (work = workQueue.poll()) != null )
            {
                // Construct all the tasks that this transaction will perform
                List<WorkTask> txTasks = new ArrayList<>();
                txTasks.add( work );
                while ( multipleOperationsInOneTx && random.nextBoolean() && (work = workQueue.poll()) != null )
                {
                    txTasks.add( work );
                }

                // Try to perform those operations, if we fail on dead-lock then simply retry all those operations until we don't
                boolean retry;
                int numRetries = 0;
                do
                {
                    // Intermediary state of created/deleted relationships to update or relationships mirror with upon success of the transaction
                    Map<Long,TxNodeChanges> txCreated = new HashMap<>();
                    Map<Long,TxNodeChanges> txDeleted = new HashMap<>();
                    try ( Transaction tx = database.beginTx() )
                    {
                        for ( WorkTask task : txTasks )
                        {
                            task.perform( tx, denseNodeIds, random.among( types ), relationships, allRelationships, random, txCreated, txDeleted );
                        }
                        tx.commit();
                        retry = false;
                        // Now on success update each node's relationship mirror
                        txCreated.forEach( ( nodeId, changes ) -> relationships.get( nodeId ).addAll( changes.relationships ) );
                        txDeleted.forEach( ( nodeId, changes ) -> relationships.get( nodeId ).removeAll( changes.relationships ) );
                        // Finally update the global relationships map afterwards so that no deleter is able to spot them before we've updated the mirrors
                        txCreated.forEach( ( nodeId, changes ) -> allRelationships.addAll( changes.relationships ) );
                    }
                    catch ( TransientTransactionFailureException e )
                    {
                        retry = true;
                        numRetries++;
                        allRelationships.addAll(
                                txDeleted.values().stream().flatMap( change -> change.relationships.stream() ).collect( Collectors.toSet() ) );
                        denseNodeIds.addAll(
                                txDeleted.values().stream().filter( change -> change.node ).map( change -> change.id ).collect( Collectors.toList() ) );
                        numDeadlocks.incrementAndGet();
                        // Random back-off after deadlock
                        Thread.sleep( random.nextInt( 1, 10 * numRetries ) );
                    }
                    catch ( NotFoundException e )
                    {
                        retry = false; // can happen when detach-deleting
                    }
                }
                while ( retry );
            }
        } ), 1 );

        race.goUnchecked();

        // then
        Set<Long> deletedDenseNodes = new HashSet<>( initialDenseNodes );
        deletedDenseNodes.removeAll( denseNodeIds );
        assertDeletedNodes( deletedDenseNodes );

        for ( long denseNodeId : denseNodeIds )
        {
            assertRelationshipsAndDegrees( denseNodeId, relationships.get( denseNodeId ) );
        }
        assertThat( numDeadlocks.get() ).isLessThan( NUM_TASKS / 10 );
    }

    private static class TxNodeChanges
    {
        final long id;
        Set<Relationship> relationships = new HashSet<>();
        boolean node;

        private TxNodeChanges( long id )
        {
            this.id = id;
        }
    }

    private void assertDeletedNodes( Set<Long> deletedInitialNodes )
    {
        try ( Transaction tx = database.beginTx() )
        {
            deletedInitialNodes.forEach( id ->
            {
                try
                {
                    Node node = tx.getNodeById( id );
                    assertThat( node.getLabels() ).doesNotContain( INITIAL_LABEL ); //in case id was reused
                }
                catch ( NotFoundException e )
                {
                    //Expected, since we deleted it
                }
            });
        }
    }

    static Stream<Arguments> permutations()
    {
        List<Arguments> permutations = new ArrayList<>();
        for ( boolean multipleDenseNodes : new boolean[]{true, false} )
        {
            for ( boolean startAsDense : new boolean[]{true, false} )
            {
                for ( boolean multipleOpsPerTx : new boolean[]{true, false} )
                {
                    for ( boolean multipleTypes : new boolean[]{true, false} )
                    {
                        // Only create
                        permutations.add( arguments( multipleDenseNodes, startAsDense, multipleOpsPerTx, multipleTypes, new int[]{1} ) );
                        if ( startAsDense )
                        {
                            // Only delete
                            permutations.add( arguments( multipleDenseNodes, startAsDense, multipleOpsPerTx, multipleTypes, new int[]{0, 1} ) );
                            // Create and delete, mostly deletes
                            permutations.add( arguments( multipleDenseNodes, startAsDense, multipleOpsPerTx, multipleTypes, new int[]{3, 4} ) );
                        }
                        // Create and delete, mostly creates
                        permutations.add( arguments( multipleDenseNodes, startAsDense, multipleOpsPerTx, multipleTypes, new int[]{3, 1} ) );
                        // Create, delete, delete all
                        permutations.add( arguments( multipleDenseNodes, startAsDense, multipleOpsPerTx, multipleTypes, new int[]{10, 6, 2, 1, 1} ) );
                        permutations.add( arguments( multipleDenseNodes, startAsDense, multipleOpsPerTx, multipleTypes, new int[]{40, 20, 8, 6, 4, 1} ) );
                    }
                }
            }
        }
        return permutations.stream();
    }

    private void assertRelationshipsAndDegrees( long denseNodeId, Set<Relationship> relationships )
    {
        try ( Transaction tx = database.beginTx() )
        {
            Node node = tx.getNodeById( denseNodeId );
            Set<Relationship> currentRelationships = new HashSet<>();
            node.getRelationships().forEach( currentRelationships::add );
            assertThat( currentRelationships )
                    .as( new Description()
                    {
                        @Override
                        public String value()
                        {
                            var shouldContain = diff( relationships, currentRelationships );
                            var shouldNotContain = diff( currentRelationships, relationships );
                            return (shouldContain.isEmpty() ? "" : " Should contain: " + shouldContain) +
                                    (!shouldContain.isEmpty() && !shouldNotContain.isEmpty() ? "\n" : "") +
                                    (shouldNotContain.isEmpty() ? "" : " Should not contain: " + shouldNotContain);
                        }
                    } )
                    .isEqualTo( relationships );
            assertThat( node.getDegree() ).isEqualTo( relationships.size() );
            for ( RelationshipType type : currentRelationships.stream().map( Relationship::getType ).collect( Collectors.toSet() ) )
            {
                assertThat( node.getDegree( type ) ).isEqualTo( currentRelationships.stream().filter( r -> r.isType( type ) ).count() );
                assertThat( node.getDegree( type, Direction.OUTGOING ) ).isEqualTo(
                        currentRelationships.stream().filter( r -> r.isType( type ) && r.getStartNode().equals( node ) ).count() );
                assertThat( node.getDegree( type, Direction.INCOMING ) ).isEqualTo(
                        currentRelationships.stream().filter( r -> r.isType( type ) && r.getEndNode().equals( node ) ).count() );
            }
        }
    }

    private <T> Set<T> diff( Set<T> s1, Set<T> s2 )
    {
        Set<T> s = new HashSet<>( s1 );
        s.removeAll( s2 );
        return s;
    }

    private interface WorkTask
    {
        void perform( Transaction tx, Set<Long> denseNodeIds, RelationshipType type, Map<Long,Set<Relationship>> relationshipsMirror,
                Set<Relationship> allRelationships, RandomRule random, Map<Long,TxNodeChanges> txCreated, Map<Long,TxNodeChanges> txDeleted );
    }

    enum WorkType implements WorkTask
    {
        CREATE
        {
            @Override
            public void perform( Transaction tx, Set<Long> denseNodeIds, RelationshipType type, Map<Long,Set<Relationship>> relationshipsMirror,
                    Set<Relationship> allRelationships, RandomRule random, Map<Long,TxNodeChanges> txCreated, Map<Long,TxNodeChanges> txDeleted )
            {
                Node from;
                Node to;
                switch ( random.nextInt( 6 ) )
                {
                case 0:
                case 1:
                case 2:
                    from = randomDenseNode( tx, denseNodeIds, random );
                    to = denseNodeIds.size() > 1 && random.nextBoolean() ? randomDenseNode( tx, denseNodeIds, random ) : tx.createNode();
                    break;
                case 3:
                case 4:
                    from = denseNodeIds.size() > 1 && random.nextBoolean() ? randomDenseNode( tx, denseNodeIds, random ) : tx.createNode();
                    to = randomDenseNode( tx, denseNodeIds, random );
                    break;
                case 5:
                default:
                    from = randomDenseNode( tx, denseNodeIds, random );
                    to = from;
                    break;
                }

                int numRelationships = random.nextInt( 3 ) + 1;
                for ( int i = 0; i < numRelationships; i++ )
                {
                    Relationship relationship = from.createRelationshipTo( to, type );
                    trackTxRelationship( relationship, txCreated, txDeleted, denseNodeIds, true );
                }
            }
        },
        DELETE
        {
            @Override
            public void perform( Transaction tx, Set<Long> denseNodeIds, RelationshipType type, Map<Long,Set<Relationship>> relationshipsMirror,
                    Set<Relationship> allRelationships, RandomRule random, Map<Long,TxNodeChanges> txCreated, Map<Long,TxNodeChanges> txDeleted )
            {
                Node onNode = randomDenseNode( tx, denseNodeIds, random );
                List<Relationship> rels = StreamSupport.stream( onNode.getRelationships( type ).spliterator(), false ).collect( Collectors.toList() );
                int batch = random.nextInt( 3 ) + 1;
                for ( int i = 0; i < batch; i++ )
                {
                    Relationship rel = rels.isEmpty() ? null : rels.get( random.nextInt( rels.size() ) );
                    if ( rel != null && allRelationships.remove( rel ) )
                    {
                        rels.remove( rel );
                        safeDeleteRelationship( rel, txCreated, txDeleted, denseNodeIds );
                    }
                }
            }
        },
        DELETE_ALL_TYPE_DIRECTION
        {
            @Override
            public void perform( Transaction tx, Set<Long> denseNodeIds, RelationshipType type, Map<Long,Set<Relationship>> relationshipsMirror,
                    Set<Relationship> allRelationships, RandomRule random, Map<Long,TxNodeChanges> txCreated, Map<Long,TxNodeChanges> txDeleted )
            {
                Node onNode = randomDenseNode( tx, denseNodeIds, random );
                Iterable<Relationship> relationships = onNode.getRelationships( Direction.values()[random.nextInt( Direction.values().length )], type );
                deleteRelationships( allRelationships, txCreated, txDeleted, relationships, denseNodeIds );
            }
        },
        DELETE_ALL_TYPE
        {
            @Override
            public void perform( Transaction tx, Set<Long> denseNodeIds, RelationshipType type, Map<Long,Set<Relationship>> relationshipsMirror,
                    Set<Relationship> allRelationships, RandomRule random, Map<Long,TxNodeChanges> txCreated, Map<Long,TxNodeChanges> txDeleted )
            {
                Node onNode = randomDenseNode( tx, denseNodeIds, random );
                deleteRelationships( allRelationships, txCreated, txDeleted, onNode.getRelationships( type ), denseNodeIds );
            }
        },
        DELETE_ALL
        {
            @Override
            public void perform( Transaction tx, Set<Long> denseNodeIds, RelationshipType type, Map<Long,Set<Relationship>> relationshipsMirror,
                    Set<Relationship> allRelationships, RandomRule random, Map<Long,TxNodeChanges> txCreated, Map<Long,TxNodeChanges> txDeleted )
            {
                Node onNode = randomDenseNode( tx, denseNodeIds, random );
                deleteRelationships( allRelationships, txCreated, txDeleted, onNode.getRelationships(), denseNodeIds );
            }
        },
        CHANGE_OTHER_NODE_DATA
        {
            @Override
            public void perform( Transaction tx, Set<Long> denseNodeIds, RelationshipType type, Map<Long,Set<Relationship>> relationshipsMirror,
                    Set<Relationship> allRelationships, RandomRule random, Map<Long,TxNodeChanges> txCreated, Map<Long,TxNodeChanges> txDeleted )
            {
                Node onNode = randomDenseNode( tx, denseNodeIds, random );
                switch ( random.nextInt( 4 ) )
                {
                case 0:
                    onNode.setProperty( "KEY_" + random.nextInt( 3 ), random.nextInt() );
                    break;
                case 1:
                    onNode.removeProperty( "KEY_" + random.nextInt( 3 ) );
                    break;
                case 2:
                    onNode.addLabel( Label.label( "LABEL_" + random.nextInt( 3 ) ) );
                    break;
                default:
                    onNode.removeLabel( Label.label( "LABEL_" + random.nextInt( 3 ) ) );
                    break;
                }
            }
        },
        DETACH_DELETE
        {
            @Override
            public void perform( Transaction tx, Set<Long> denseNodeIds, RelationshipType type, Map<Long,Set<Relationship>> relationshipsMirror,
                    Set<Relationship> allRelationships, RandomRule random, Map<Long,TxNodeChanges> txCreated, Map<Long,TxNodeChanges> txDeleted )
            {
                //This work task is currently unused (detach delete is tested in another test) due
                // to the problem of keeping track of the actually deleted relationships in order to create correct assertions as the end of the test.
                try
                {
                    long id;
                    do
                    {
                        if ( denseNodeIds.size() <= 3 )
                        {
                            return;
                        }
                        id = randomAmong( denseNodeIds, random );
                    }
                    while ( id == Record.NULL_REFERENCE.longValue() || !denseNodeIds.remove( id ) );
                    txDeleted.computeIfAbsent( id, TxNodeChanges::new ).node = true;

                    InternalTransaction internalTx = (InternalTransaction) tx;
                    internalTx.kernelTransaction().dataWrite().nodeDetachDelete( id );

                    Set<Long> denseNodesInclId = new HashSet<>( denseNodeIds );
                    denseNodesInclId.add( id );
                    long finalId = id;
                    allRelationships.removeIf( relationship -> {
                        boolean toDelete = relationship.getStartNodeId() == finalId || relationship.getEndNodeId() == finalId;
                        if ( toDelete )
                        {
                            trackTxRelationship( relationship, txCreated, txDeleted, denseNodesInclId, false );
                        }
                        return toDelete;
                    } );
                }
                catch ( KernelException e )
                {
                    ExceptionUtils.throwAsUncheckedException( e );
                }
            }
        };

        private static Node randomDenseNode( Transaction tx, Set<Long> denseNodeIds, RandomRule random )
        {
            long id = randomAmong( denseNodeIds, random );
            assertThat( isNull( id ) ).isFalse();
            return tx.getNodeById( id );
        }

        private static long randomAmong( Set<Long> ids, RandomRule randomRule )
        {
            long value = Record.NULL_REFERENCE.longValue();
            do
            {
                Object[] array = ids.toArray();
                if ( array.length >= 1 )
                {
                    value = (long) randomRule.among( array );
                }
            }
            while ( !ids.contains( value ) && !ids.isEmpty() );
            return ids.isEmpty() ? Record.NULL_REFERENCE.longValue() : value;
        }

        private static void safeDeleteRelationship( Relationship relationship, Map<Long,TxNodeChanges> txCreated,
                Map<Long,TxNodeChanges> txDeleted, Set<Long> denseNodeIds )
        {
            try
            {
                trackTxRelationship( relationship, txCreated, txDeleted, denseNodeIds, false );
                relationship.delete();
            }
            catch ( NotFoundException e )
            {
                //this is not yet fully understood, but is caught and rethrown as transient to cause retry of this transaction
                throw new TransientTransactionFailureException( Status.Database.Unknown, "Relationship vanished in front of us, hmm" );
            }
        }

        private static void trackTxRelationship( Relationship relationship, Map<Long,TxNodeChanges> txCreated, Map<Long,TxNodeChanges> txDeleted,
                Set<Long> denseNodeIds, boolean create )
        {
            var map1 = create ? txCreated : txDeleted;
            var map2 = create ? txDeleted : txCreated;
            if ( denseNodeIds.contains( relationship.getStartNodeId() ) )
            {
                map1.computeIfAbsent( relationship.getStartNodeId(), TxNodeChanges::new ).relationships.add( relationship );
                map2.computeIfAbsent( relationship.getStartNodeId(), TxNodeChanges::new ).relationships.remove( relationship );
            }
            if ( denseNodeIds.contains( relationship.getEndNodeId() ) )
            {
                map1.computeIfAbsent( relationship.getEndNodeId(), TxNodeChanges::new ).relationships.add( relationship );
                map2.computeIfAbsent( relationship.getEndNodeId(), TxNodeChanges::new ).relationships.remove( relationship );
            }
        }

        private static void deleteRelationships( Set<Relationship> allRelationships, Map<Long,TxNodeChanges> txCreated,
                Map<Long,TxNodeChanges> txDeleted, Iterable<Relationship> relationships, Set<Long> denseNodeIds )
        {
            List<Relationship> readRelationships = StreamSupport.stream( relationships.spliterator(), false ).collect( Collectors.toList() );
            readRelationships.stream().filter( allRelationships::remove ).forEach(
                    relationship -> safeDeleteRelationship( relationship, txCreated, txDeleted, denseNodeIds ) );
        }
    }

    private Collection<WorkTask> createWork( int... weights )
    {
        if ( weights.length < WorkType.values().length )
        {
            // Just fill the rest with zeros
            weights = Arrays.copyOf( weights, WorkType.values().length );
        }

        ArrayList<WorkTask> weightedWork = new ArrayList<>();
        for ( WorkType work : WorkType.values() )
        {
            weightedWork.addAll( Collections.nCopies( weights[work.ordinal()], work ) );
        }

        List<WorkTask> work = new ArrayList<>();
        for ( int i = 0; i < NUM_TASKS; i++ )
        {
            work.add( weightedWork.get( random.nextInt( weightedWork.size() ) ) );
        }
        return work;
    }

    @Test
    void shouldNotBlockOnCreateOnLongChain() throws ExecutionException, InterruptedException
    {
        // given
        Set<Relationship> relationships = newKeySet();
        long denseNodeId = createDenseNode( relationships );

        // when
        assertNotBlocking(
                tx -> relationships.add( tx.getNodeById( denseNodeId ).createRelationshipTo( tx.createNode(), INITIAL_DENSE_NODE_TYPE ) ),
                tx -> relationships.add( tx.getNodeById( denseNodeId ).createRelationshipTo( tx.createNode(), INITIAL_DENSE_NODE_TYPE ) ) );

        // then
        assertRelationshipsAndDegrees( denseNodeId, relationships );
    }

    @Test
    void shouldBlockOnCreateWithNewType() throws Throwable
    {
        // given
        Set<Relationship> relationships = new HashSet<>();
        long denseNodeId = createDenseNode( relationships );

        // when
        assertBlocking(
                tx -> relationships.add( tx.getNodeById( denseNodeId ).createRelationshipTo( tx.createNode(), TEST ) ),
                tx -> relationships.add( tx.getNodeById( denseNodeId ).createRelationshipTo( tx.createNode(), TEST ) ) );

        // then
        assertRelationshipsAndDegrees( denseNodeId, relationships );
    }

    @Test
    void shouldBlockOnCreateWithExistingTypeNewDirection() throws Throwable
    {
        // given
        Set<Relationship> relationships = new HashSet<>();
        long denseNodeId = createDenseNode( relationships );

        // when
        assertBlocking(
                tx -> relationships.add( tx.createNode().createRelationshipTo( tx.getNodeById( denseNodeId ), INITIAL_DENSE_NODE_TYPE ) ),
                tx -> relationships.add( tx.getNodeById( denseNodeId ).createRelationshipTo( tx.createNode(), INITIAL_DENSE_NODE_TYPE ) ) );

        // also then
        assertRelationshipsAndDegrees( denseNodeId, relationships );
    }

    @Test
    void shouldNotBlockOnDeleteOnSameLongChain() throws Throwable
    {
        // given
        Set<Relationship> relationships = new HashSet<>();
        long denseNodeId = createDenseNode( relationships );

        // Create a bunch of relationships in a somewhat more controlled way, where we know that the order of these relationships
        // will be the order of the chain so that we can use that below to pick two relationships that are slightly apart from each other
        long[] relationshipIds = new long[10];
        for ( int i = 0; i < relationshipIds.length; i++ )
        {
            try ( Transaction tx = database.beginTx() )
            {
                Node denseNode = tx.getNodeById( denseNodeId );
                Relationship relationship = denseNode.createRelationshipTo( tx.createNode(), INITIAL_DENSE_NODE_TYPE );
                tx.commit();
                relationships.add( relationship );
                relationshipIds[i] = relationship.getId();
            }
        }

        // when
        assertNotBlocking(
                tx -> deleteRelationship( tx.getRelationshipById( relationshipIds[3] ), relationships ),
                tx -> deleteRelationship( tx.getRelationshipById( relationshipIds[7] ), relationships ) );

        // then
        assertRelationshipsAndDegrees( denseNodeId, relationships );
    }

    @Test
    void shouldBlockOnCreateForNodeThatIsBeingDeleted() throws Throwable
    {
        // given
        long denseNodeId = createDenseNode( new HashSet<>() );

        // when/then
        assertBlocking( tx ->
                {
                    Node denseNode = tx.getNodeById( denseNodeId );
                    denseNode.getRelationships().forEach( Relationship::delete );
                    denseNode.delete();
                }, tx -> assertThatThrownBy( () -> tx.getNodeById( denseNodeId ).createRelationshipTo( tx.createNode(), INITIAL_DENSE_NODE_TYPE )  )
                        .isInstanceOfAny( NotFoundException.class ),
                details -> details.isAt( NodeEntity.class, "createRelationshipTo" ) );
    }

    private void deleteRelationship( Relationship relationship, Set<Relationship> relationships )
    {
        relationship.delete();
        relationships.remove( relationship );
    }

    private void assertNotBlocking( Consumer<Transaction> tx1, Consumer<Transaction> tx2 ) throws InterruptedException, ExecutionException
    {
        try ( OtherThreadExecutor t2 = new OtherThreadExecutor( "T2" ) )
        {
            Barrier.Control barrier = new Barrier.Control();
            Future<Object> t2Future = t2.executeDontWait( command( () ->
            {
                try ( Transaction tx = database.beginTx() )
                {
                    tx1.accept( tx );
                    ((TransactionImpl) tx).commit( barrier::reached );
                }
            } ) );
            barrier.await();
            try ( Transaction tx = database.beginTx() )
            {
                tx2.accept( tx );
                tx.commit();
            }
            barrier.release();
            t2Future.get();
        }
    }

    private void assertBlocking( Consumer<Transaction> tx1, Consumer<Transaction> tx2 ) throws InterruptedException, ExecutionException, TimeoutException
    {
        assertBlocking( tx1, tx2, details -> details.isAt( KernelTransactionImplementation.class, "commit" ) );
    }

    private void assertBlocking( Consumer<Transaction> tx1, Consumer<Transaction> tx2, Predicate<OtherThreadExecutor.WaitDetails> waitDetailsPredicate )
            throws InterruptedException, ExecutionException, TimeoutException
    {
        try ( OtherThreadExecutor t2 = new OtherThreadExecutor( "T2" );
                OtherThreadExecutor t3 = new OtherThreadExecutor( "T3" ) )
        {
            Barrier.Control barrier = new Barrier.Control();
            Future<Object> t2Future = t2.executeDontWait( command( () ->
            {
                try ( Transaction tx = database.beginTx() )
                {
                    tx1.accept( tx );
                    ((TransactionImpl) tx).commit( barrier::reached );
                }
            } ) );
            barrier.await();
            Future<Object> t3Future = t3.executeDontWait( command( () ->
            {
                try ( Transaction tx = database.beginTx() )
                {
                    tx2.accept( tx );
                    tx.commit();
                }
            } ) );
            t3.waitUntilWaiting( waitDetailsPredicate );
            barrier.release();
            t2Future.get();
            t3Future.get();
        }
    }

    private long createDenseNode( Set<Relationship> createdRelationships )
    {
        return createNode( createdRelationships, NUM_INITIAL_RELATIONSHIPS_PER_DENSE_NODE );
    }

    private long createNode( Set<Relationship> createdRelationships, int numInitialRelationships )
    {
        try ( Transaction tx = database.beginTx() )
        {
            Node denseNode = tx.createNode();
            Node otherNode = tx.createNode();
            for ( int i = 0; i < numInitialRelationships; i++ )
            {
                createdRelationships.add( denseNode.createRelationshipTo( otherNode, INITIAL_DENSE_NODE_TYPE ) );
            }
            tx.commit();
            return denseNode.getId();
        }
    }

    private Collection<Long> createConnectedNodes( Map<Long,Set<Relationship>> createdRelationships, int numInitialRelationshipsPerNode )
    {
        try ( Transaction tx = database.beginTx() )
        {
            Node[] denseNodes = new Node[NUM_DENSE_NODES_IN_MULTI_SETUP];
            for ( int i = 0; i < denseNodes.length; i++ )
            {
                denseNodes[i] = tx.createNode();
                createdRelationships.put( denseNodes[i].getId(), newKeySet() );
            }
            int numRelationships = numInitialRelationshipsPerNode * denseNodes.length;
            for ( int i = 0; i < numRelationships; i++ )
            {
                Node startNode = random.among( denseNodes );
                Node endNode = random.among( denseNodes );
                Relationship relationship = startNode.createRelationshipTo( endNode, INITIAL_DENSE_NODE_TYPE );
                createdRelationships.get( startNode.getId() ).add( relationship );
                createdRelationships.get( endNode.getId() ).add( relationship );
            }
            tx.commit();
            return Arrays.stream( denseNodes ).map( Node::getId ).collect( Collectors.toList() );
        }
    }

    private Collection<Long> createInitialNodes( boolean multipleDenseNodes, boolean startAsDense, Map<Long,Set<Relationship>> relationships )
    {
        Collection<Long> denseNodeIds;
        int numInitialRelationshipsPerNode = startAsDense ? NUM_INITIAL_RELATIONSHIPS_PER_DENSE_NODE : NUM_INITIAL_RELATIONSHIPS_PER_SPARSE_NODE;
        if ( multipleDenseNodes )
        {
            denseNodeIds = createConnectedNodes( relationships, numInitialRelationshipsPerNode );
        }
        else
        {
            Set<Relationship> nodeRelationships =  newKeySet();
            long node = createNode( nodeRelationships, numInitialRelationshipsPerNode );
            denseNodeIds = List.of( node );
            relationships.put( node, nodeRelationships );
        }
        try ( Transaction tx = database.beginTx() )
        {
            denseNodeIds.forEach( l -> tx.getNodeById( l ).addLabel( INITIAL_LABEL ) );
        }
        return denseNodeIds;
    }

    private String consistencyCheckReportAsString( ConsistencyCheckService.Result result )
    {
        try ( BufferedReader reader = new BufferedReader( fs.openAsReader( result.reportFile(), Charset.defaultCharset() ) ) )
        {
            StringBuilder builder = new StringBuilder();
            String line;
            while ( (line = reader.readLine()) != null )
            {
                builder.append( format( "%s%n", line ) );
            }
            return builder.toString();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
