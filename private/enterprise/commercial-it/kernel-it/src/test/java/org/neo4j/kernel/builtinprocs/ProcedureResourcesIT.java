/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.builtinprocs;

import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import com.neo4j.test.rule.CommercialDbmsRule;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Settings;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.test.rule.DbmsRule;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ProcedureResourcesIT
{
    @Rule
    public DbmsRule db = new CommercialDbmsRule()
            .withSetting( OnlineBackupSettings.online_backup_enabled, Settings.FALSE );

    private final String indexDefinition = ":Label(prop)";
    private final String ftsNodesIndex = "'ftsNodes'";
    private final String ftsRelsIndex = "'ftsRels'";
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @After
    public void tearDown() throws InterruptedException
    {
        executor.shutdown();
        executor.awaitTermination( 5, TimeUnit.SECONDS );
    }

    @Test
    public void allProcedures() throws Exception
    {
        // when
        createIndex();
        createFulltextIndexes();
        for ( ProcedureSignature procedure : db.getDependencyResolver().resolveDependency( GlobalProcedures.class ).getAllProcedures() )
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
            Node node1 = db.createNode( unusedLabel );
            node1.setProperty( unusedPropKey, "value" );
            Node node2 = db.createNode( unusedLabel );
            node2.setProperty( unusedPropKey, 1 );
            node1.createRelationshipTo( node2, unusedRelType );
            tx.success();
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
            exhaust( db.execute( procedureQuery ) ).close();
            exhaust( db.execute( "MATCH (mo:Label) WHERE mo.prop = 'n/a' RETURN mo" ) ).close();
            executeInOtherThread( "CREATE(mo:Label) SET mo.prop = 'val' RETURN mo" );
            Result result = db.execute( "MATCH (mo:Label) WHERE mo.prop = 'val' RETURN mo" );
            assertTrue( failureMessage, result.hasNext() );
            Map<String,Object> next = result.next();
            assertNotNull( failureMessage, next.get( "mo" ) );
            exhaust( result );
            result.close();
            outer.success();
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
            db.execute( "CREATE INDEX ON " + indexDefinition );
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 5, TimeUnit.SECONDS );
            tx.success();
        }
    }

    private void createFulltextIndexes()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( "call db.index.fulltext.createNodeIndex(" + ftsNodesIndex + ", ['Label'], ['prop'])" ).close();
            db.execute( "call db.index.fulltext.createRelationshipIndex(" + ftsRelsIndex + ", ['Label'], ['prop'])" ).close();
            tx.success();
        }
    }

    private void clearDb()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( "MATCH (n) DETACH DELETE n" ).close();
            tx.success();
        }
    }

    private static class ProcedureData
    {
        private final String name;
        private final List<Object> params = new ArrayList<>();
        private String setupQuery;
        private String postQuery;
        private boolean skip;

        private ProcedureData( ProcedureSignature procedure )
        {
            this.name = procedure.name().toString();
        }

        private void withParam( Object param )
        {
            this.params.add( param );
        }

        private void withSetup( String setupQuery, String postQuery )
        {
            this.setupQuery = setupQuery;
            this.postQuery = postQuery;
        }

        private String buildProcedureQuery()
        {
            StringJoiner stringJoiner = new StringJoiner( ",", "CALL " + name + "(", ")" );
            for ( Object parameter : params )
            {
                stringJoiner.add( parameter.toString() );
            }
            if ( setupQuery != null && postQuery != null )
            {
                return setupQuery + " " + stringJoiner.toString() + " " + postQuery;
            }
            else
            {
                return stringJoiner.toString();
            }
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
            proc.withParam( "'" + indexDefinition + "'" );
            break;
        case "db.createRelationshipType":
            proc.withParam( "'RelType'" );
            break;
        case "dbms.queryJmx":
            proc.withParam( "'*:*'" );
            break;
        case "db.awaitIndex":
            proc.withParam( "'" + indexDefinition + "'" );
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
            proc.withParam( "'transaction-1234'" );
            break;
        case "dbms.killTransactions":
            proc.withParam( "['transaction-1234']" );
            break;
        case "dbms.setTXMetaData":
            proc.withParam( "{realUser:'MyMan'}" );
            break;
        case "dbms.listActiveLocks":
            proc.withParam( "'query-1234'" );
            break;
        case "dbms.setConfigValue":
            proc.withParam( "'dbms.logs.query.enabled'" );
            proc.withParam( "'false'" );
            break;
        case "db.createIndex":
            proc.withParam( "':Person(name)'" );
            proc.withParam( "'lucene+native-2.0'" );
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
            proc.withParam( ftsRelsIndex );
            break;
        case "db.index.fulltext.createRelationshipIndex":
            // Grabs schema lock an so can not execute concurrently with node creation
            proc.skip = true;
            break;
        case "db.index.fulltext.createNodeIndex":
            // Grabs schema lock an so can not execute concurrently with node creation
            proc.skip = true;
            break;
        case "db.index.fulltext.awaitIndex":
            proc.withParam( ftsNodesIndex );
            proc.withParam( 100 );
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
                exhaust( db.execute( query ) );
                tx.success();
            }
        } );
        future.get();
    }

}
