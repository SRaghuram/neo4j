/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.common.Cluster.dataMatchesEventually;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.neo4j.graphdb.schema.ConstraintType.NODE_KEY;
import static org.neo4j.graphdb.schema.ConstraintType.UNIQUENESS;
import static org.neo4j.internal.helpers.collection.Iterables.single;

@ClusterExtension
@TestInstance( PER_METHOD )
class ClusterIndexProcedureIT
{
    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

    @BeforeEach
    void beforeEach() throws Exception
    {
        var clusterConfig = clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 3 )
                .withTimeout( 1000, SECONDS );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
    }

    @Test
    void createIndexProcedureMustPropagate() throws Exception
    {
        // create an index
        cluster.coreTx( ( db, tx ) ->
        {
            db.execute( "CALL db.createIndex( \":Person(name)\", \"lucene+native-3.0\")" ).close();
            tx.commit();
        } );

        // node created just to be able to use dataMatchesEventually as a barrier
        CoreClusterMember leader = cluster.coreTx( ( db, tx ) ->
        {
            Node person = tx.createNode( Label.label( "Person" ) );
            person.setProperty( "name", "Bo Burnham" );
            tx.commit();
        } );

        // node creation is guaranteed to happen after index creation
        dataMatchesEventually( leader, cluster.coreMembers() );
        dataMatchesEventually( leader, cluster.readReplicas() );

        // now the indexes must exist, so we wait for them to come online
        cluster.coreMembers().forEach( ClusterIndexProcedureIT::awaitIndexOnline );
        cluster.readReplicas().forEach( ClusterIndexProcedureIT::awaitIndexOnline );

        // verify indexes
        cluster.coreMembers().forEach( core -> verifyIndexes( core.defaultDatabase() ) );
        cluster.readReplicas().forEach( rr -> verifyIndexes( rr.defaultDatabase() ) );
    }

    @Test
    void createUniquePropertyConstraintMustPropagate() throws Exception
    {
        // create a constraint
        CoreClusterMember leader = cluster.coreTx( ( db, tx ) ->
        {
            db.execute( "CALL db.createUniquePropertyConstraint( \":Person(name)\", \"lucene+native-3.0\")" ).close();
            tx.commit();
        } );

        // node created just to be able to use dataMatchesEventually as a barrier
        cluster.coreTx( ( db, tx ) ->
        {
            Node person = tx.createNode( Label.label( "Person" ) );
            person.setProperty( "name", "Bo Burnham" );
            tx.commit();
        } );

        // node creation is guaranteed to happen after constraint creation
        dataMatchesEventually( leader, cluster.coreMembers() );
        dataMatchesEventually( leader, cluster.readReplicas() );

        // verify indexes
        cluster.coreMembers().forEach( core -> verifyIndexes( core.defaultDatabase() ) );
        cluster.readReplicas().forEach( rr -> verifyIndexes( rr.defaultDatabase() ) );

        // verify constraints
        cluster.coreMembers().forEach( core -> verifyConstraints( core.defaultDatabase(), UNIQUENESS ) );
        cluster.readReplicas().forEach( rr -> verifyConstraints( rr.defaultDatabase(), UNIQUENESS ) );
    }

    @Test
    void createNodeKeyConstraintMustPropagate() throws Exception
    {
        // create a node key
        CoreClusterMember leader = cluster.coreTx( ( db, tx ) ->
        {
            db.execute( "CALL db.createNodeKey( \":Person(name)\", \"lucene+native-3.0\")" ).close();
            tx.commit();
        } );

        // node created just to be able to use dataMatchesEventually as a barrier
        cluster.coreTx( ( db, tx ) ->
        {
            Node person = tx.createNode( Label.label( "Person" ) );
            person.setProperty( "name", "Bo Burnham" );
            tx.commit();
        } );

        // node creation is guaranteed to happen after constraint creation
        dataMatchesEventually( leader, cluster.coreMembers() );
        dataMatchesEventually( leader, cluster.readReplicas() );

        // verify indexes
        cluster.coreMembers().forEach( core -> verifyIndexes( core.defaultDatabase() ) );
        cluster.readReplicas().forEach( rr -> verifyIndexes( rr.defaultDatabase() ) );

        // verify node keys
        cluster.coreMembers().forEach( core -> verifyConstraints( core.defaultDatabase(), NODE_KEY ) );
        cluster.readReplicas().forEach( rr -> verifyConstraints( rr.defaultDatabase(), NODE_KEY ) );
    }

    private static void awaitIndexOnline( ClusterMember member )
    {
        GraphDatabaseAPI db = member.defaultDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.commit();
        }
    }

    private static void verifyIndexes( GraphDatabaseAPI db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            // only one index
            Iterator<IndexDefinition> indexes = db.schema().getIndexes().iterator();
            assertTrue( indexes.hasNext(), "has one index" );
            IndexDefinition indexDefinition = indexes.next();
            assertFalse( indexes.hasNext(), "not more than one index" );

            Label label = single( indexDefinition.getLabels() );
            String property = indexDefinition.getPropertyKeys().iterator().next();

            // with correct pattern and provider
            assertEquals( "Person", label.name(), "correct label" );
            assertEquals( "name", property, "correct property" );
            assertCorrectProvider( db, label, property );

            tx.commit();
        }
    }

    private static void verifyConstraints( GraphDatabaseAPI db, ConstraintType expectedConstraintType )
    {
        try ( Transaction tx = db.beginTx() )
        {
            // only one index
            Iterator<ConstraintDefinition> constraints = db.schema().getConstraints().iterator();
            assertTrue( constraints.hasNext(), "has one index" );
            ConstraintDefinition constraint = constraints.next();
            assertFalse( constraints.hasNext(), "not more than one index" );

            Label label = constraint.getLabel();
            String property = constraint.getPropertyKeys().iterator().next();
            ConstraintType constraintType = constraint.getConstraintType();

            // with correct pattern and provider
            assertEquals( "Person", label.name(), "correct label" );
            assertEquals( "name", property, "correct property" );
            assertEquals( expectedConstraintType, constraintType, "correct constraint type" );

            tx.commit();
        }
    }

    private static void assertCorrectProvider( GraphDatabaseAPI db, Label label, String property )
    {
        ThreadToStatementContextBridge bridge = db.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );
        KernelTransaction kernelTransaction = bridge.getKernelTransactionBoundToThisThread( false, db.databaseId() );
        TokenRead tokenRead = kernelTransaction.tokenRead();
        int labelId = tokenRead.nodeLabel( label.name() );
        int propId = tokenRead.propertyKey( property );
        SchemaRead schemaRead = kernelTransaction.schemaRead();
        IndexDescriptor index = schemaRead.index( labelId, propId );
        assertEquals( "lucene+native", index.getIndexProvider().getKey(), "correct provider key" );
        assertEquals( "3.0", index.getIndexProvider().getVersion(), "correct provider version" );
    }
}
