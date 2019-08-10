/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.CausalClusteringTestHelpers;
import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalTime;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.LongStream;

import org.neo4j.function.ThrowingAction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.WriteOperationsNotAllowedException;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.TokenAccess;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.leader_election_timeout;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@ClusterExtension
class TokenReplicationStressIT
{
    private static final int EXECUTION_TIME_SECONDS = Integer.getInteger( "TokenReplicationStressTestExecutionTimeSeconds", 30 );

    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

    @BeforeEach
    void setUp() throws Exception
    {
        cluster = clusterFactory.createCluster( ClusterConfig.clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 0 )
                .withSharedCoreParam( leader_election_timeout, "2s" ) );

        cluster.start();
    }

    @Test
    void shouldReplicateTokensWithConcurrentElections() throws Throwable
    {
        AtomicBoolean stop = new AtomicBoolean();

        CompletableFuture<Void> tokenCreator1 = runAsync( () -> createTokens( cluster, evenTokenIdsSupplier(), stop ) );
        CompletableFuture<Void> tokenCreator2 = runAsync( () -> createTokens( cluster, oddTokenIdsSupplier(), stop ) );
        CompletableFuture<Void> electionTrigger = runAsync( () -> triggerElections( cluster, stop ) );
        CompletableFuture<Void> allOperations = allOf( tokenCreator1, tokenCreator2, electionTrigger );

        awaitUntilDeadlineOrFailure( stop, allOperations );

        stop.set( true );
        allOperations.join();

        // we need to allow time for the tokens to replicate
        assertEventually( () -> verifyTokens( cluster ), 15, SECONDS );

        // assert number of tokens on every cluster member is the same after a restart
        // restart is needed to make sure tokens are persisted and not only in token caches
        cluster.shutdown();
        cluster.start();

        verifyTokens( cluster );
    }

    void assertEventually( ThrowingAction<?> actual, long timeout, TimeUnit timeUnit ) throws Exception
    {
        long endTimeMillis = System.currentTimeMillis() + timeUnit.toMillis( timeout );

        do
        {
            try
            {
                actual.apply();
                return;
            }
            catch ( AssertionError e )
            {
                if ( System.currentTimeMillis() > endTimeMillis )
                {
                    throw e;
                }
                // swallow and try again
            }

            Thread.sleep( 10 );
        } while ( true );
    }

    private static void createTokens( Cluster cluster, LongSupplier tokenIdSupplier, AtomicBoolean stop )
    {
        while ( !stop.get() )
        {
            CoreClusterMember leader = awaitLeader( cluster );
            GraphDatabaseService db = leader.defaultDatabase();

            // transaction that creates a lot of new tokens
            try ( Transaction tx = db.beginTx() )
            {
                for ( int i = 0; i < 10; i++ )
                {
                    long tokenId = tokenIdSupplier.getAsLong();

                    Label label = Label.label( "Label_" + tokenId );
                    String propertyKey = "Property_" + tokenId;
                    RelationshipType type = RelationshipType.withName( "RELATIONSHIP_" + tokenId );

                    Node node1 = db.createNode( label );
                    Node node2 = db.createNode( label );

                    node1.setProperty( propertyKey, tokenId );
                    node2.setProperty( propertyKey, tokenId );

                    node1.createRelationshipTo( node2, type );
                }
                tx.commit();
            }
            catch ( WriteOperationsNotAllowedException ignore )
            {
                // this can happen because other thread is forcing elections
            }
            catch ( Throwable t )
            {
                throw new RuntimeException( "Failed to create tokens", t );
            }
        }
    }

    private static void triggerElections( Cluster cluster, AtomicBoolean stop )
    {
        while ( !stop.get() )
        {
            try
            {
                SECONDS.sleep( 5 );
                CausalClusteringTestHelpers.forceReelection( cluster, DEFAULT_DATABASE_NAME );
            }
            catch ( Throwable t )
            {
                throw new RuntimeException( "Failed to trigger an election", t );
            }
        }
    }

    private static void awaitUntilDeadlineOrFailure( AtomicBoolean stop, CompletableFuture<Void> allOperations ) throws InterruptedException
    {
        Duration executionTime = Duration.ofSeconds( EXECUTION_TIME_SECONDS );
        LocalTime deadline = LocalTime.now().plus( executionTime );

        while ( deadline.compareTo( LocalTime.now() ) > 0 )
        {
            if ( allOperations.isCompletedExceptionally() )
            {
                stop.set( true );
                break;
            }
            SECONDS.sleep( 1 );
        }
    }

    private static CoreClusterMember awaitLeader( Cluster cluster )
    {
        try
        {
            return cluster.awaitLeader();
        }
        catch ( TimeoutException e )
        {
            throw new IllegalStateException( "No leader found", e );
        }
    }

    private void verifyTokens( Cluster cluster )
    {
        verifyLabelTokens( cluster );
        verifyPropertyKeyTokens( cluster );
        verifyRelationshipTypeTokens( cluster );
    }

    private void verifyLabelTokens( Cluster cluster )
    {
        verifyTokens( "Labels", cluster, this::allLabels );
    }

    private void verifyPropertyKeyTokens( Cluster cluster )
    {
        verifyTokens( "Property keys", cluster, this::allPropertyKeys );
    }

    private void verifyRelationshipTypeTokens( Cluster cluster )
    {
        verifyTokens( "Relationship types", cluster, this::allRelationshipTypes );
    }

    private static void verifyTokens( String tokenType, Cluster cluster, Function<CoreClusterMember,List<String>> tokensExtractor )
    {
        List<List<String>> tokensFromAllMembers = cluster.coreMembers()
                .stream()
                .map( tokensExtractor )
                .collect( toList() );

        for ( List<String> tokens : tokensFromAllMembers )
        {
            assertTokensAreUnique( tokens );
        }

        if ( !allTokensEqual( tokensFromAllMembers ) )
        {
            String tokensString = tokensFromAllMembers.stream()
                    .map( List::toString )
                    .collect( joining( "\n" ) );

            fail( tokenType + " are not the same on different cluster members:\n" + tokensString );
        }
    }

    private static void assertTokensAreUnique( List<String> tokens )
    {
        Set<String> uniqueTokens = new HashSet<>( tokens );
        if ( uniqueTokens.size() != tokens.size() )
        {
            fail( "Tokens contain duplicates: " + tokens );
        }
    }

    private static boolean allTokensEqual( List<List<String>> tokensFromAllMembers )
    {
        long distinctSets = tokensFromAllMembers.stream()
                .map( HashSet::new )
                .distinct()
                .count();

        return distinctSets == 1;
    }

    private List<String> allLabels( CoreClusterMember member )
    {
        return allTokens( member, TokenAccess.LABELS )
                .stream()
                .map( Label::name )
                .collect( toList() );
    }

    private List<String> allPropertyKeys( CoreClusterMember member )
    {
        return allTokens( member, TokenAccess.PROPERTY_KEYS );
    }

    private List<String> allRelationshipTypes( CoreClusterMember member )
    {
        return allTokens( member, TokenAccess.RELATIONSHIP_TYPES )
                .stream()
                .map( RelationshipType::name )
                .collect( toList() );
    }

    private static <T> List<T> allTokens( CoreClusterMember member, TokenAccess<T> tokenAccess )
    {
        GraphDatabaseFacade db = member.defaultDatabase();
        try ( Transaction ignore = db.beginTx() )
        {
            KernelTransaction kernelTx = currentKernelTx( member );
            return Iterators.asList( tokenAccess.all( kernelTx ) );
        }
    }

    private static LongSupplier evenTokenIdsSupplier()
    {
        return tokenIdsSupplier( 0 );
    }

    private static LongSupplier oddTokenIdsSupplier()
    {
        return tokenIdsSupplier( 1 );
    }

    private static LongSupplier tokenIdsSupplier( long initialValue )
    {
        return LongStream.iterate( initialValue, i -> i + 2 ).iterator()::nextLong;
    }

    private static KernelTransaction currentKernelTx( CoreClusterMember member )
    {
        GraphDatabaseFacade graphDatabaseFacade = member.defaultDatabase();
        ThreadToStatementContextBridge bridge = graphDatabaseFacade.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );
        return bridge.getKernelTransactionBoundToThisThread( true, graphDatabaseFacade.databaseId() );
    }
}
