/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.builtinprocs;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterators.single;

@EnterpriseDbmsExtension( configurationCallback = "enableBolt" )
class ProcedureResourcesIT
{
    @Inject
    private GraphDatabaseAPI db;
    @Inject
    private GlobalProcedures globalProcedures;

    private final String indexDefinition = ":Label(prop)";
    private final String ftsNodesIndex = "'ftsNodes'";
    private final String ftsRelsIndex = "'ftsRels'";
    private final String ftsRelsIndexToDrop = "'ftsRelsToDrop'";
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private String indexName;

    @ExtensionCallback
    static void enableBolt( TestDatabaseManagementServiceBuilder builder )
    {

        builder.setConfig( BoltConnector.enabled, true )
                .setConfig( BoltConnector.listen_address, new SocketAddress( "localhost", 0 ) );
    }

    @AfterAll
    void tearDown() throws InterruptedException
    {
        executor.shutdown();
        executor.awaitTermination( 5, TimeUnit.SECONDS );
    }

    @Test
    void allProcedures() throws Exception
    {
        // when
        createIndex();
        createFulltextIndexes();
        for ( ProcedureSignature procedure : globalProcedures.getAllProcedures() )
        {
            // then
            initialData();
            ProcedureData procedureData = null;
            try
            {
                procedureData = procedureDataFor( procedure );
                verifyProcedureCloseAllAcquiredKernelStatements( procedureData );
            }
            catch ( Exception e )
            {
                throw new Exception( "Failed on procedure: \"" + procedureData + "\"", e );
            }
            clearDb();
        }
    }

    private void initialData()
    {
        Label unusedLabel = Label.label( "unusedLabel" );
        RelationshipType unusedRelType = RelationshipType.withName( "unusedRelType" );
        String unusedPropKey = "unusedPropKey";
        try ( Transaction tx = db.beginTx() )
        {
            Node node1 = tx.createNode( unusedLabel );
            node1.setProperty( unusedPropKey, "value" );
            Node node2 = tx.createNode( unusedLabel );
            node2.setProperty( unusedPropKey, 1 );
            node1.createRelationshipTo( node2, unusedRelType );
            tx.commit();
        }
    }

    private void verifyProcedureCloseAllAcquiredKernelStatements( ProcedureData proc ) throws ExecutionException, InterruptedException
    {
        if ( proc.skip )
        {
            return;
        }
        String failureMessage = "Failed on procedure " + proc.name;
        try ( Transaction outer = db.beginTx() )
        {
            String procedureQuery = proc.buildProcedureQuery();
            exhaust( outer.execute( procedureQuery ) ).close();
            exhaust( outer.execute( "MATCH (mo:Label) WHERE mo.prop = 'n/a' RETURN mo" ) ).close();
            executeInOtherThread( "CREATE(mo:Label) SET mo.prop = 'val' RETURN mo" );
            Result result = outer.execute( "MATCH (mo:Label) WHERE mo.prop = 'val' RETURN mo" );
            assertTrue( result.hasNext(), failureMessage );
            Map<String,Object> next = result.next();
            assertNotNull( next.get( "mo" ), failureMessage );
            exhaust( result );
            result.close();
            outer.commit();
        }
    }

    private static Result exhaust( Result execute )
    {
        while ( execute.hasNext() )
        {
            execute.next();
        }
        return execute;
    }

    private void createIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( "CREATE INDEX ON " + indexDefinition );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 5, TimeUnit.SECONDS );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            int labelId = ktx.tokenRead().nodeLabel( "Label" );
            int propId = ktx.tokenRead().propertyKey( "prop" );
            IndexDescriptor index = single( ktx.schemaRead().index( SchemaDescriptor.forLabel( labelId, propId ) ) );
            indexName = index.getName();
            tx.commit();
        }
    }

    private void createFulltextIndexes()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( "call db.index.fulltext.createNodeIndex(" + ftsNodesIndex + ", ['Label'], ['prop'])" ).close();
            tx.execute( "call db.index.fulltext.createRelationshipIndex(" + ftsRelsIndex + ", ['Label'], ['prop'])" ).close();
            tx.execute( "call db.index.fulltext.createRelationshipIndex(" + ftsRelsIndexToDrop + ", ['Label', 'ToDrop'], ['prop'])" ).close();
            tx.commit();
        }
    }

    private void clearDb()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( "MATCH (n) DETACH DELETE n" ).close();
            tx.commit();
        }
    }

    private static class ProcedureData
    {
        private final String name;
        private final List<Object> params = new ArrayList<>();
        private boolean skip;

        private ProcedureData( ProcedureSignature procedure )
        {
            this.name = procedure.name().toString();
        }

        private void withParam( Object param )
        {
            this.params.add( param );
        }

        private String buildProcedureQuery()
        {
            StringJoiner stringJoiner = new StringJoiner( ",", "CALL " + name + "(", ")" );
            for ( Object parameter : params )
            {
                stringJoiner.add( parameter.toString() );
            }
            return stringJoiner.toString();
        }

        @Override
        public String toString()
        {
            return buildProcedureQuery();
        }
    }

    private ProcedureData procedureDataFor( ProcedureSignature procedure )
    {
        ProcedureData proc = new ProcedureData( procedure );
        switch ( proc.name )
        {
        case "db.createProperty":
            proc.withParam( "'propKey'" );
            break;
        case "db.resampleIndex":
            proc.withParam( "'" + indexName + "'" );
            break;
        case "db.createRelationshipType":
            proc.withParam( "'RelType'" );
            break;
        case "dbms.queryJmx":
            proc.withParam( "'*:*'" );
            break;
        case "db.awaitIndex":
            proc.withParam( "'" + indexName + "'" );
            proc.withParam( 100 );
            break;
        case "db.createLabel":
            proc.withParam( "'OtherLabel'" );
            break;
        case "dbms.killQuery":
            proc.withParam( "'query-1234'" );
            break;
        case "dbms.killQueries":
            proc.withParam( "['query-1234']" );
            break;
        case "dbms.killConnection":
            proc.withParam( "'bolt-1234'" );
            break;
        case "dbms.killConnections":
            proc.withParam( "['bolt-1234']" );
            break;
        case "dbms.killTransaction":
            proc.withParam( "'database-transaction-1234'" );
            break;
        case "dbms.killTransactions":
            proc.withParam( "['database-transaction-1234']" );
            break;
        case "tx.setMetaData":
            proc.withParam( "{realUser:'MyMan'}" );
            break;
        case "dbms.listActiveLocks":
            proc.withParam( "'query-1234'" );
            break;
        case "dbms.setConfigValue":
            proc.withParam( "'dbms.logs.query.enabled'" );
            proc.withParam( "'off'" );
            break;
        case "db.createIndex":
            proc.withParam( "'This is my index name'" );
            proc.withParam( "['Person']" );
            proc.withParam( "['name']" );
            proc.withParam( "'lucene+native-3.0'" );
            break;
        case "db.createNodeKey":
            // Grabs schema lock an so can not execute concurrently with node creation
            proc.skip = true;
            break;
        case "db.createUniquePropertyConstraint":
            // Grabs schema lock an so can not execute concurrently with node creation
            proc.skip = true;
            break;
        case "db.index.fulltext.queryNodes":
            proc.withParam( ftsNodesIndex );
            proc.withParam( "'value'" );
            break;
        case "db.index.fulltext.queryRelationships":
            proc.withParam( ftsRelsIndex );
            proc.withParam( "'value'" );
            break;
        case "db.index.fulltext.drop":
            proc.withParam( ftsRelsIndexToDrop );
            break;
        case "db.index.fulltext.createRelationshipIndex":
            // Grabs schema lock an so can not execute concurrently with node creation
            proc.skip = true;
            break;
        case "db.index.fulltext.createNodeIndex":
            // Grabs schema lock an so can not execute concurrently with node creation
            proc.skip = true;
            break;
        case "db.stats.retrieve":
            proc.withParam( "'TOKENS'" );
            break;
        case "db.stats.retrieveAllAnonymized":
            proc.withParam( "'myGraphToken'" );
            break;
        case "db.stats.collect":
            proc.withParam( "'QUERIES'" );
            break;
        case "db.stats.stop":
            proc.withParam( "'QUERIES'" );
            break;
        case "db.stats.clear":
            proc.withParam( "'QUERIES'" );
            break;
        case "dbms.cluster.routing.getRoutingTable":
            proc.withParam( "{}" );
            break;
        case "dbms.routing.getRoutingTable":
            proc.withParam( "{}" );
            break;
        case "dbms.scheduler.profile":
            proc.withParam( "'sample'" );
            proc.withParam( "'CheckPoint'" );
            proc.withParam( "'0s'" );
            break;
        case "db.indexDetails":
            proc.withParam( "\"" + indexName + "\"" );
            break;
        case "dbms.database.state":
            proc.withParam( "\"" + SYSTEM_DATABASE_NAME + "\"" );
            break;
        case "dbms.upgradeStatusDetails":
        case "dbms.upgradeStatus":
        case "dbms.upgradeDetails":
        case "dbms.upgrade":
            // Must run against system database
            proc.skip = true;
            break;
        default:
        }
        return proc;
    }

    private void executeInOtherThread( String query ) throws ExecutionException, InterruptedException
    {
        Future<?> future = executor.submit( () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                exhaust( tx.execute( query ) );
                tx.commit();
            }
        } );
        future.get();
    }

}
