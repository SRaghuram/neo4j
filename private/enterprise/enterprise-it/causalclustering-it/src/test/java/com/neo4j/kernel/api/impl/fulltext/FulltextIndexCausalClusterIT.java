/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.read_replica.ReadReplica;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.neo4j.common.DependencyResolver;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.impl.fulltext.analyzer.providers.Arabic;
import org.neo4j.kernel.api.impl.fulltext.analyzer.providers.UrlOrEmail;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.extension.Inject;
import org.neo4j.values.storable.Value;

import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.AWAIT_REFRESH;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.NODE_CREATE;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.NODE_CREATE_WITH_CONFIG;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.QUERY_NODES;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.QUERY_RELS;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.RELATIONSHIP_CREATE;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.RELATIONSHIP_CREATE_WITH_CONFIG;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.asConfigMap;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.asConfigString;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.asProcedureConfigMap;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.asStrList;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexSettingsKeys.PROCEDURE_EVENTUALLY_CONSISTENT;

@ClusterExtension
@TestInstance( PER_METHOD )
class FulltextIndexCausalClusterIT
{
    private static final String NODE = "node";
    private static final String RELATIONSHIP = "relationship";
    private static final Label LABEL = Label.label( "LABEL" );
    private static final String PROP = "prop";
    private static final String PROP2 = "otherprop";
    // The "ec_prop" property is added because the EC indexes cannot have exactly the same entity-token/property-token sets as the non-EC indexes:
    private static final String EC_PROP = "ec_prop";
    private static final RelationshipType REL = RelationshipType.withName( "REL" );
    private static final String NODE_INDEX = "nodeIndex";
    private static final String REL_INDEX = "relIndex";
    private static final String NODE_INDEX_EC = "nodeIndexEventuallyConsistent";
    private static final String REL_INDEX_EC = "relIndexEventuallyConsistent";
    private static final String EVENTUALLY_CONSISTENT_SETTING = ", {" + PROCEDURE_EVENTUALLY_CONSISTENT + ": 'true'}";

    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;
    private long nodeId1;
    private long nodeId2;
    private long relId1;

    @BeforeEach
    void beforeEach() throws Exception
    {
        cluster = clusterFactory.createCluster( clusterConfig().withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 1 ) );
        cluster.start();
    }

    @Test
    void fulltextIndexContentsMustBeReplicatedWhenPopulating() throws Exception
    {
        cluster.coreTx( ( db, tx ) ->
        {
            Node node1 = tx.createNode( LABEL );
            node1.setProperty( PROP, "This is an integration test." );
            node1.setProperty( EC_PROP, true );
            Node node2 = tx.createNode( LABEL );
            node2.setProperty( PROP2, "This is a related integration test." );
            node2.setProperty( EC_PROP, true );
            Relationship rel = node1.createRelationshipTo( node2, REL );
            rel.setProperty( PROP, "They relate" );
            rel.setProperty( EC_PROP, true );
            nodeId1 = node1.getId();
            nodeId2 = node2.getId();
            relId1 = rel.getId();
            tx.commit();
        } );
        cluster.coreTx( ( db, tx ) ->
        {
            tx.execute( format( NODE_CREATE, NODE_INDEX, asStrList( LABEL.name() ), asStrList( PROP, PROP2 ) ) ).close();
            tx.execute( format( RELATIONSHIP_CREATE, REL_INDEX, asStrList( REL.name() ), asStrList( PROP ) ) ).close();
            tx
                    .execute( format( NODE_CREATE, NODE_INDEX_EC, asStrList( LABEL.name() ),
                            asStrList( PROP, PROP2, EC_PROP ) + EVENTUALLY_CONSISTENT_SETTING ) )
                    .close();
            tx
                    .execute( format( RELATIONSHIP_CREATE, REL_INDEX_EC, asStrList( REL.name() ),
                            asStrList( PROP, EC_PROP ) + EVENTUALLY_CONSISTENT_SETTING ) )
                    .close();
            tx.commit();
        } );

        awaitCatchup();

        verifyIndexContents( NODE_INDEX, "integration", true, nodeId1, nodeId2 );
        verifyIndexContents( NODE_INDEX_EC, "integration", true, nodeId1, nodeId2 );
        verifyIndexContents( NODE_INDEX, "test", true, nodeId1, nodeId2 );
        verifyIndexContents( NODE_INDEX_EC, "test", true, nodeId1, nodeId2 );
        verifyIndexContents( NODE_INDEX, "related", true, nodeId2 );
        verifyIndexContents( NODE_INDEX_EC, "related", true, nodeId2 );
        verifyIndexContents( REL_INDEX, "relate", false, relId1 );
        verifyIndexContents( REL_INDEX_EC, "relate", false, relId1 );
    }

    @Test
    void fulltextIndexContentsMustBeReplicatedWhenUpdating() throws Exception
    {
        cluster.coreTx( ( db, tx ) ->
        {
            tx.execute( format( NODE_CREATE, NODE_INDEX, asStrList( LABEL.name() ), asStrList( PROP, PROP2 ) ) ).close();
            tx.execute( format( RELATIONSHIP_CREATE, REL_INDEX, asStrList( REL.name() ), asStrList( PROP ) ) ).close();
            tx.execute( format( NODE_CREATE, NODE_INDEX_EC, asStrList( LABEL.name() ), asStrList( PROP, PROP2, EC_PROP ) ) ).close();
            tx.execute( format( RELATIONSHIP_CREATE, REL_INDEX_EC, asStrList( REL.name() ), asStrList( PROP, EC_PROP ) ) ).close();
            tx.commit();
        } );

        awaitCatchup();

        cluster.coreTx( ( db, tx ) ->
        {
            Node node1 = tx.createNode( LABEL );
            node1.setProperty( PROP, "This is an integration test." );
            node1.setProperty( EC_PROP, true );
            Node node2 = tx.createNode( LABEL );
            node2.setProperty( PROP2, "This is a related integration test." );
            node2.setProperty( EC_PROP, true );
            Relationship rel = node1.createRelationshipTo( node2, REL );
            rel.setProperty( PROP, "They relate" );
            rel.setProperty( EC_PROP, true );
            nodeId1 = node1.getId();
            nodeId2 = node2.getId();
            relId1 = rel.getId();
            tx.commit();
        } );

        awaitCatchup();

        verifyIndexContents( NODE_INDEX, "integration", true, nodeId1, nodeId2 );
        verifyIndexContents( NODE_INDEX_EC, "integration", true, nodeId1, nodeId2 );
        verifyIndexContents( NODE_INDEX, "test", true, nodeId1, nodeId2 );
        verifyIndexContents( NODE_INDEX_EC, "test", true, nodeId1, nodeId2 );
        verifyIndexContents( NODE_INDEX, "related", true, nodeId2 );
        verifyIndexContents( NODE_INDEX_EC, "related", true, nodeId2 );
        verifyIndexContents( REL_INDEX, "relate", false, relId1 );
        verifyIndexContents( REL_INDEX_EC, "relate", false, relId1 );
    }

    @Test
    void fulltextSettingsMustBeReplicatedToAllClusterMembers() throws Exception
    {
        String analyserNodeIndex = new UrlOrEmail().getName();
        boolean eventuallyConsistentNodeIndex = true;
        String analyzerRelIndex = new Arabic().getName();
        boolean eventuallyConsistentRelIndex = false;

        cluster.coreTx( ( db, tx ) ->
        {
            String nodeString = asConfigString( asProcedureConfigMap( analyserNodeIndex, eventuallyConsistentNodeIndex ) );
            String relString = asConfigString( asProcedureConfigMap( analyzerRelIndex, eventuallyConsistentRelIndex ) );
            tx
                    .execute(
                            format( NODE_CREATE_WITH_CONFIG, NODE_INDEX, asStrList( LABEL.name() ), asStrList( PROP, PROP2 ), nodeString ) )
                    .close();
            tx
                    .execute( format( RELATIONSHIP_CREATE_WITH_CONFIG, REL_INDEX, asStrList( REL.name() ), asStrList( PROP, PROP2 ),
                            relString ) )
                    .close();
            tx.commit();
        } );

        awaitCatchup();

        Map<String,Value> nodeIndexConfig = asConfigMap( analyserNodeIndex, eventuallyConsistentNodeIndex );
        Map<String,Value> relIndexConfig = asConfigMap( analyzerRelIndex, eventuallyConsistentRelIndex );
        verifyIndexConfig( NODE_INDEX, nodeIndexConfig );
        verifyIndexConfig( REL_INDEX, relIndexConfig );
    }

    private void verifyIndexConfig( String indexName, Map<String,Value> expectedIndexConfig ) throws IndexNotFoundKernelException
    {
        for ( CoreClusterMember member : cluster.coreMembers() )
        {
            verifyIndexConfig( member.defaultDatabase(), indexName, expectedIndexConfig );
        }
        for ( ReadReplica member : cluster.readReplicas() )
        {
            verifyIndexConfig( member.defaultDatabase(), indexName, expectedIndexConfig );
        }
    }

    private void verifyIndexConfig( GraphDatabaseFacade db, String indexName, Map<String,Value> expectedIndexConfig )
            throws IndexNotFoundKernelException
    {
        try ( Transaction tx = db.beginTx() )
        {
            Map<String,Value> actualIndexConfig = getIndexConfig( db, (InternalTransaction) tx, indexName );
            assertEquals( expectedIndexConfig, actualIndexConfig );
            tx.commit();
        }
    }

    private Map<String,Value> getIndexConfig( GraphDatabaseFacade db, InternalTransaction transaction, String indexName ) throws IndexNotFoundKernelException
    {
        IndexDescriptor indexReference = transaction.kernelTransaction().schemaRead().indexGetForName( indexName );
        IndexingService indexingService = db.getDependencyResolver().resolveDependency( IndexingService.class );
        IndexProxy indexProxy = indexingService.getIndexProxy( indexReference );
        return indexProxy.indexConfig();
    }

    private void awaitCatchup() throws InterruptedException
    {
        MutableLongSet appliedTransactions = new LongHashSet();
        Consumer<ClusterMember> awaitPopulationAndCollectionAppliedTransactionId = member ->
        {
            GraphDatabaseAPI db = member.defaultDatabase();
            try ( Transaction tx = db.beginTx() )
            {
                tx.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
                tx.execute( AWAIT_REFRESH ).close();
                DependencyResolver dependencyResolver = db.getDependencyResolver();
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
            catch ( NotFoundException nfe )
            {
                // This can happen due to a race inside `tx.schema().awaitIndexesOnline`, where the list of indexes are provided by the SchemaCache, which is
                // updated during command application in commit, but the index status is then fetched from the IndexMap, which is updated when the applier is
                // closed during commit (which comes later in the commit process than command application). So we are told by the SchemaCache that an index
                // exists, but we are then also told by the IndexMap that the index does not exist, hence this NotFoundException. Normally, these two are
                // protected by the schema locks that are taken on index create and index status check. However, those locks are 1) incorrectly managed around
                // index population, and 2) those locks are NOT TAKEN in Causal Cluster command replication!
                // So we need to anticipate this, and if the race happens, we simply have to try again. But yeah, it needs to be fixed properly at some point.
                appliedTransactions.add( -1L );
                appliedTransactions.add( -2L );
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

    private void verifyIndexContents( String index, String queryString, boolean queryNodes, long... entityIds ) throws Exception
    {
        for ( CoreClusterMember member : cluster.coreMembers() )
        {
            verifyIndexContents( member.defaultDatabase(), index, queryString, entityIds, queryNodes );
        }
        for ( ReadReplica member : cluster.readReplicas() )
        {
            verifyIndexContents( member.defaultDatabase(), index, queryString, entityIds, queryNodes );
        }
    }

    private void verifyIndexContents( GraphDatabaseService db, String index, String queryString, long[] entityIds, boolean queryNodes ) throws Exception
    {
        try ( Transaction transaction = db.beginTx() )
        {
            List<Long> expected = Arrays.stream( entityIds ).boxed().collect( Collectors.toList() );
            String queryCall = queryNodes ? QUERY_NODES : QUERY_RELS;
            try ( Result result = transaction.execute( format( queryCall, index, queryString ) ) )
            {
                Set<Long> results = new HashSet<>();
                while ( result.hasNext() )
                {
                    results.add( ((Entity) result.next().get( queryNodes ? NODE : RELATIONSHIP )).getId() );
                }
                String errorMessage = errorMessage( results, expected ) + " (" + db + ", leader is " + cluster.awaitLeader() + ") query = " + queryString;
                assertEquals( expected.size(), results.size(), errorMessage );
                int i = 0;
                while ( !results.isEmpty() )
                {
                    assertTrue( results.remove( expected.get( i++ ) ), errorMessage );
                }
            }
            transaction.commit();
        }
    }

    private static String errorMessage( Set<Long> actual, List<Long> expected )
    {
        return format( "Query results differ from expected, expected %s but got %s", expected, actual );
    }
}
