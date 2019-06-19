/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.database;

import com.google.common.collect.Lists;
import com.neo4j.bench.client.database.Store;
import com.neo4j.bench.client.model.Edition;
import com.neo4j.bench.client.process.HasPid;
import com.neo4j.bench.client.process.Pid;
import com.neo4j.bench.client.util.BenchmarkUtil;
import com.neo4j.bench.macro.execution.CountingResultVisitor;
import com.neo4j.commercial.edition.factory.CommercialDatabaseManagementServiceBuilder;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.dbms.StoreInfoCommand;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class EmbeddedDatabase implements Database
{
    private final Store store;
    private final DatabaseManagementService managementService;
    private final GraphDatabaseService db;
    private final CountingResultVisitor resultVisitor;

    public static void recreateSchema( Store store, Edition edition, Path neo4jConfig, Schema schema )
    {
        System.out.println( "Dropping schema..." );
        try ( EmbeddedDatabase db = EmbeddedDatabase.startWith( store, edition, neo4jConfig ) )
        {
            db.dropSchema();
            Schema droppedSchema = db.getSchema();
            if ( !droppedSchema.isEmpty() )
            {
                throw new RuntimeException( "Failed to drop all database schema.\n" +
                                            "Remaining database schema:\n" +
                                            droppedSchema.toString() );
            }
        }

        System.out.println( "Deleting index directory and transaction logs..." );
        store.removeIndexDir();
        store.removeTxLogs();

        System.out.println( "Recreating schema..." );
        try ( EmbeddedDatabase db = EmbeddedDatabase.startWith( store, edition, neo4jConfig ) )
        {
            db.createSchema( schema );
            System.out.println( "Verifying recreated schema..." );
            EmbeddedDatabase.verifySchema( db, schema );
        }
    }

    public static void verifyStoreFormat( Store store )
    {
        ArrayList<String> outputFromAdminTool = new ArrayList<>();

        StoreInfoCommand storeInfoCommand = new StoreInfoCommand( outputFromAdminTool::add );
        try
        {
            storeInfoCommand.execute( new String[]{"--store=" + store.graphDbDirectory().toString()} );
        }
        catch ( IncorrectUsage | CommandFailed incorrectUsage )
        {
            throw new RuntimeException( "Did not find the store or the admin command have changed: " + incorrectUsage );
        }
        if ( isStoreSuperseded( outputFromAdminTool ) )
        {
            throw new RuntimeException( "Store not updated, please update. Got this output from the admin tool: " + String.join( "\n", outputFromAdminTool ) );
        }
    }

    private static boolean isStoreSuperseded( ArrayList<String> outputFromAdminTool )
    {
        //if the output contains superseded we know that there is a new store format that we should upgrade to.
        return outputFromAdminTool.stream().anyMatch( message -> message.contains( "superseded" ) );
    }

    public static void verifySchema( Store store, Edition edition, Path neo4jConfig, Schema expectedSchema )
    {
        try ( EmbeddedDatabase db = EmbeddedDatabase.startWith( store, edition, neo4jConfig ) )
        {
            verifySchema( db, expectedSchema );
        }
    }

    private static void verifySchema( EmbeddedDatabase db, Schema expectedSchema )
    {
        Schema.assertEqual( expectedSchema, db.getSchema() );
    }

    public static EmbeddedDatabase startWith( Store store, Edition edition, Path neo4jConfig )
    {
        DatabaseManagementService managementService = newDb( store, edition, neo4jConfig );
        return new EmbeddedDatabase( store, managementService );
    }

    private EmbeddedDatabase( Store store, DatabaseManagementService managementService )
    {
        requireNonNull( store );
        requireNonNull( managementService );
        this.store = store;
        this.managementService = managementService;
        this.db = managementService.database( store.graphDbDirectory().getFileName().toString() );
        this.resultVisitor = new CountingResultVisitor();
    }

    private boolean isRunning()
    {
        return db != null && db.isAvailable( SECONDS.toMillis( 10 ) );
    }

    private static DatabaseManagementService newDb( Store store, Edition edition, Path neo4jConfig )
    {
        DatabaseManagementServiceBuilder builder = newBuilder( store, edition );
        if ( null != neo4jConfig )
        {
            builder.loadPropertiesFromFile( neo4jConfig.toAbsolutePath().toString() );
        }
        return builder.build();
    }

    private static DatabaseManagementServiceBuilder newBuilder( Store store, Edition edition )
    {
        switch ( edition )
        {
        case COMMUNITY:
            return new DatabaseManagementServiceBuilder( store.topLevelDirectory().toFile() );
        case ENTERPRISE:
            return new CommercialDatabaseManagementServiceBuilder( store.topLevelDirectory().toFile() );
        default:
            throw new RuntimeException( "Unrecognized edition: " + edition );
        }
    }

    public Store store()
    {
        return store;
    }

    @Override
    public Pid pid()
    {
        return HasPid.getPid();
    }

    @Override
    public int execute( String query, Map<String,Object> parameters, boolean inTx, boolean shouldRollback )
    {
        return inTx
               ? executeInTx( query, parameters, shouldRollback )
               : execute( query, parameters );
    }

    private int executeInTx( String query, Map<String,Object> parameters, boolean shouldRollback )
    {
        try ( Transaction tx = db.beginTx() )
        {
            int rowCount = execute( query, parameters );
            if ( shouldRollback )
            {
                tx.failure();
            }
            else
            {
                tx.success();
            }
            return rowCount;
        }
    }

    private int execute( String query, Map<String,Object> parameters )
    {
        resultVisitor.reset();
        Result result = db.execute( query, parameters );
        result.accept( resultVisitor );
        return resultVisitor.count();
    }

    public GraphDatabaseService db()
    {
        return db;
    }

    public Schema getSchema()
    {
        try ( Transaction tx = db.beginTx() )
        {
            List<Schema.SchemaEntry> entries = new ArrayList<>();

            db.schema()
              .getConstraints()
              .forEach( constraint -> entries.add( constraintEntryFor( constraint ) ) );
            db.schema()
              .getIndexes()
              .forEach( index ->
                        {
                            if ( !index.isConstraintIndex() )
                            {
                                entries.add( new Schema.IndexSchemaEntry( index.getLabel(), Lists.newArrayList( index.getPropertyKeys() ) ) );
                            }
                        } );

            return new Schema( entries );
        }
    }

    private static Schema.SchemaEntry constraintEntryFor( ConstraintDefinition constraint )
    {
        switch ( constraint.getConstraintType() )
        {
        case NODE_KEY:
            return new Schema.NodeKeySchemaEntry( constraint.getLabel(), Lists.newArrayList( constraint.getPropertyKeys() ) );
        case UNIQUENESS:
            return new Schema.NodeUniqueSchemaEntry( constraint.getLabel(), constraint.getPropertyKeys().iterator().next() );
        case NODE_PROPERTY_EXISTENCE:
            return new Schema.NodeExistsSchemaEntry( constraint.getLabel(), constraint.getPropertyKeys().iterator().next() );
        case RELATIONSHIP_PROPERTY_EXISTENCE:
            return new Schema.RelationshipExistsSchemaEntry( constraint.getRelationshipType(), constraint.getPropertyKeys().iterator().next() );
        default:
            throw new RuntimeException( "Unrecognized constraint type: " + constraint.getConstraintType() );
        }
    }

    private void dropSchema()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().getConstraints().forEach( ConstraintDefinition::drop );
            db.schema().getIndexes().forEach( IndexDefinition::drop );
            tx.success();
        }
    }

    private void createSchema( Schema schema )
    {
        try ( Transaction tx = db.beginTx() )
        {
            schema.constraints()
                  .stream()
                  .map( Schema.SchemaEntry::createStatement )
                  .forEach( db::execute );
            schema.indexes()
                  .stream()
                  .map( Schema.SchemaEntry::createStatement )
                  .forEach( db::execute );
            tx.success();
        }
        waitForSchema();
    }

    private void waitForSchema()
    {
        try ( Transaction ignore = db.beginTx() )
        {
            for ( IndexDefinition index : db.schema().getIndexes() )
            {
                assertIndexNotFailed( db, index );
                if ( db.schema().getIndexState( index ) != org.neo4j.graphdb.schema.Schema.IndexState.ONLINE )
                {
                    while ( db.schema().getIndexState( index ) == org.neo4j.graphdb.schema.Schema.IndexState.POPULATING )
                    {
                        Thread.sleep( 500 );
                    }
                    assertIndexNotFailed( db, index );
                }
            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error while waiting for indexes to come online", e );
        }
    }

    private static void assertIndexNotFailed( GraphDatabaseService db, IndexDefinition index )
    {
        if ( db.schema().getIndexState( index ) == org.neo4j.graphdb.schema.Schema.IndexState.FAILED )
        {
            throw new RuntimeException(
                    format( "Index (%s,%s) failed to build:\n%s",
                            index.getLabel(),
                            index.getPropertyKeys(),
                            db.schema().getIndexFailure( index )
                    )
            );
        }
    }

    @Override
    public String toString()
    {
        return "Neo4j EmbeddedDatabase\n" +
               "\t* Path:    " + store.topLevelDirectory().toAbsolutePath() + "\n" +
               "\t* Running: " + isRunning() + "\n" +
               "\t* Size:    " + BenchmarkUtil.bytesToString( store.bytes() );
    }

    @Override
    public void close()
    {
        if ( isRunning() )
        {
            managementService.shutdown();
        }
    }
}
