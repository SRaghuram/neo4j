/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext;

import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.ClusterMember;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.discovery.ReadReplica;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.test.causalclustering.ClusterRule;

import static com.neo4j.kernel.api.impl.fulltext.FulltextProceduresTest.AWAIT_POPULATION;
import static com.neo4j.kernel.api.impl.fulltext.FulltextProceduresTest.ENTITYID;
import static com.neo4j.kernel.api.impl.fulltext.FulltextProceduresTest.NODE_CREATE;
import static com.neo4j.kernel.api.impl.fulltext.FulltextProceduresTest.QUERY;
import static com.neo4j.kernel.api.impl.fulltext.FulltextProceduresTest.RELATIONSHIP_CREATE;
import static com.neo4j.kernel.api.impl.fulltext.FulltextProceduresTest.array;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FulltextIndexCausalClusterIT
{
    private static final Label LABEL = Label.label( "LABEL" );
    private static final String PROP = "prop";
    private static final String PROP2 = "otherprop";
    private static final RelationshipType REL = RelationshipType.withName( "REL" );
    private static final String NODE_INDEX = "nodeIndex";
    private static final String REL_INDEX = "relIndex";

    @Rule
    public ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 1 );

    private Cluster cluster;
    private long nodeId1;
    private long nodeId2;
    private long relId1;

    @Before
    public void setUp() throws Exception
    {
        cluster = clusterRule.startCluster();
    }

    @Test
    public void fulltextIndexContentsMustBeReplicatedWhenPopulaing() throws Exception
    {
        cluster.coreTx( ( db, tx ) ->
        {
            Node node1 = db.createNode( LABEL );
            node1.setProperty( PROP, "This is an integration test." );
            Node node2 = db.createNode( LABEL );
            node2.setProperty( PROP2, "This is a related integration test." );
            Relationship rel = node1.createRelationshipTo( node2, REL );
            rel.setProperty( PROP, "They relate" );
            nodeId1 = node1.getId();
            nodeId2 = node2.getId();
            relId1 = rel.getId();
            tx.success();
        } );
        cluster.coreTx( ( db, tx ) ->
        {
            db.execute( format( NODE_CREATE, NODE_INDEX, array( LABEL.name() ), array( PROP, PROP2 ) ) ).close();
            db.execute( format( RELATIONSHIP_CREATE, REL_INDEX, array( REL.name() ), array( PROP ) ) ).close();
            tx.success();
        } );

        awaitCatchup();

        verifyIndexContents( NODE_INDEX, "integration", nodeId1, nodeId2 );
        verifyIndexContents( NODE_INDEX, "test", nodeId1, nodeId2 );
        verifyIndexContents( NODE_INDEX, "related", nodeId2 );
        verifyIndexContents( REL_INDEX, "relate", relId1 );
    }

    @Test
    public void fulltextIndexContentsMustBeReplicatedWhenUpdating() throws Exception
    {
        cluster.coreTx( ( db, tx ) ->
        {
            db.execute( format( NODE_CREATE, NODE_INDEX, array( LABEL.name() ), array( PROP, PROP2 ) ) ).close();
            db.execute( format( RELATIONSHIP_CREATE, REL_INDEX, array( REL.name() ), array( PROP ) ) ).close();
            tx.success();
        } );

        awaitCatchup();

        cluster.coreTx( ( db, tx ) ->
        {
            Node node1 = db.createNode( LABEL );
            node1.setProperty( PROP, "This is an integration test." );
            Node node2 = db.createNode( LABEL );
            node2.setProperty( PROP2, "This is a related integration test." );
            Relationship rel = node1.createRelationshipTo( node2, REL );
            rel.setProperty( PROP, "They relate" );
            nodeId1 = node1.getId();
            nodeId2 = node2.getId();
            relId1 = rel.getId();
            tx.success();
        } );

        awaitCatchup();

        verifyIndexContents( NODE_INDEX, "integration", nodeId1, nodeId2 );
        verifyIndexContents( NODE_INDEX, "test", nodeId1, nodeId2 );
        verifyIndexContents( NODE_INDEX, "related", nodeId2 );
        verifyIndexContents( REL_INDEX, "relate", relId1 );
    }

    private void awaitCatchup() throws InterruptedException
    {
        MutableLongSet appliedTransactions = new LongHashSet();
        Consumer<ClusterMember> awaitPopulationAndCollectionAppliedTransactionId = member ->
        {
            try
            {
                member.database().execute( format( AWAIT_POPULATION, NODE_INDEX ) );
                member.database().execute( format( AWAIT_POPULATION, REL_INDEX ) );
                DependencyResolver dependencyResolver = member.database().getDependencyResolver();
                TransactionIdStore transactionIdStore = dependencyResolver.resolveDependency( TransactionIdStore.class );
                appliedTransactions.add( transactionIdStore.getLastClosedTransactionId() );
            }
            catch ( QueryExecutionException | IllegalArgumentException e )
            {
                if ( e.getMessage().equals( "No index was found" ) )
                {
                    // Looks like the index creation hasn't been replicated yet, so we force a retry by making sure that
                    // the 'appliedTransactions' set will definitely contain more than one element.
                    appliedTransactions.add( -1L );
                    appliedTransactions.add( -2L );
                }
            }
        };
        do
        {
            appliedTransactions.clear();
            Thread.sleep( 25 );
            Collection<CoreClusterMember> cores = cluster.coreMembers();
            Collection<ReadReplica> readReplicas = cluster.readReplicas();
            cores.forEach( awaitPopulationAndCollectionAppliedTransactionId );
            readReplicas.forEach( awaitPopulationAndCollectionAppliedTransactionId );
        }
        while ( appliedTransactions.size() != 1 );
    }

    private void verifyIndexContents( String index, String queryString, long... entityIds ) throws Exception
    {
        for ( CoreClusterMember member : cluster.coreMembers() )
        {
            verifyIndexContents( member.database(), index, queryString, entityIds );
        }
        for ( ReadReplica member : cluster.readReplicas() )
        {
            verifyIndexContents( member.database(), index, queryString, entityIds );
        }
    }

    private void verifyIndexContents( GraphDatabaseService db, String index, String queryString, long[] entityIds ) throws Exception
    {
        List<Long> expected = Arrays.stream( entityIds ).boxed().collect( Collectors.toList() );
        try ( Result result = db.execute( format( QUERY, index, queryString ) ) )
        {
            Set<Long> results = new HashSet<>();
            while ( result.hasNext() )
            {
                results.add( (Long) result.next().get( ENTITYID ) );
            }
            String errorMessage = errorMessage( results, expected ) + " (" + db + ", leader is " +  cluster.awaitLeader() + ") query = " + queryString;
            assertEquals( errorMessage, expected.size(), results.size() );
            int i = 0;
            while ( !results.isEmpty() )
            {
                assertTrue( errorMessage, results.remove( expected.get( i++ ) ) );
            }
        }
    }

    private static String errorMessage( Set<Long> actual, List<Long> expected )
    {
        return format( "Query results differ from expected, expected %s but got %s", expected, actual );
    }
}
