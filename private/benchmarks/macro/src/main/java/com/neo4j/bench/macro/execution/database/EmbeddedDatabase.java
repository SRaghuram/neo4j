/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.database;

import com.google.common.collect.Lists;
import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.process.HasPid;
import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.macro.execution.CountingResultVisitor;
import com.neo4j.bench.model.model.Neo4jConfig;
import com.neo4j.bench.model.options.Edition;
import com.neo4j.dbms.api.EnterpriseDatabaseManagementServiceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.cli.ExecutionContext;
import org.neo4j.commandline.dbms.StoreInfoCommand;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;

public class EmbeddedDatabase implements Database
{
    private static final Logger LOG = LoggerFactory.getLogger( EmbeddedDatabase.class );

    private final Store store;
    private final DatabaseManagementService managementService;
    private final GraphDatabaseService db;
    private final CountingResultVisitor resultVisitor;

    public static void recreateSchema( Store store, Edition edition, Path neo4jConfigFile, Schema schema )
    {
        Neo4jConfig neo4jConfig = Neo4jConfigBuilder.fromFile( neo4jConfigFile ).build();
        recreateSchema( store, edition, neo4jConfig, schema );
    }

    public static void recreateSchema( Store store, Edition edition, Neo4jConfig neo4jConfig, Schema schema )
    {
        LOG.debug( "Dropping schema..." );
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

        LOG.debug( "Deleting index directory and transaction logs..." );
        store.removeIndexDir();
        store.removeTxLogs();

        LOG.debug( "Recreating schema..." );
        try ( EmbeddedDatabase db = EmbeddedDatabase.startWith( store, edition, neo4jConfig ) )
        {
            db.createSchema( schema );
            LOG.debug( "Verifying recreated schema..." );
            EmbeddedDatabase.verifySchema( db, schema );
        }
    }

    public static void verifyStoreFormat( Store store )
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream out = new PrintStream( baos );

        StoreInfoCommand storeInfoCommand = new StoreInfoCommand( new ExecutionContext( Path.of( "" ), Path.of( "" ), out, System.err,
                                                                                        new DefaultFileSystemAbstraction() ) );
        CommandLine.populateCommand( storeInfoCommand, store.graphDbDirectory().toAbsolutePath().toString() );
        storeInfoCommand.execute();
        out.flush();
        if ( isStoreSuperseded( baos.toString() ) )
        {
            throw new RuntimeException( "Store not updated, please update. Got this output from the admin tool: " + baos );
        }
    }

    private static boolean isStoreSuperseded( String output )
    {
        //if the output contains superseded we know that there is a new store format that we should upgrade to.
        return output.contains( "superseded" );
    }

    public static void verifySchema( Store store, Edition edition, Path neo4jConfigFile, Schema expectedSchema )
    {
        Neo4jConfig neo4jConfig = Neo4jConfigBuilder.fromFile( neo4jConfigFile ).build();
        verifySchema( store, edition, neo4jConfig, expectedSchema );
    }

    public static void verifySchema( Store store, Edition edition, Neo4jConfig neo4jConfig, Schema expectedSchema )
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

    public static EmbeddedDatabase startWith( Store store, Edition edition, Path neo4jConfigFile )
    {
        Neo4jConfig neo4jConfig = Neo4jConfigBuilder.fromFile( neo4jConfigFile ).build();
        return startWith( store, edition, neo4jConfig );
    }

    public static EmbeddedDatabase startWith( Store store, Edition edition, Neo4jConfig neo4jConfig )
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
        this.db = managementService.database( store.databaseName().name() );
        this.resultVisitor = new CountingResultVisitor();
    }

    private boolean isRunning()
    {
        return db != null && db.isAvailable( MINUTES.toMillis( 5 ) );
    }

    private static DatabaseManagementService newDb( Store store, Edition edition, Neo4jConfig neo4jConfig )
    {
        DatabaseManagementServiceBuilder builder = newBuilder( store, edition );
        if ( null != neo4jConfig )
        {
            builder.setConfigRaw( neo4jConfig.toMap() );
        }
        return builder.build();
    }

    private static DatabaseManagementServiceBuilder newBuilder( Store store, Edition edition )
    {
        switch ( edition )
        {
        case COMMUNITY:
            return new DatabaseManagementServiceBuilder( store.topLevelDirectory() );
        case ENTERPRISE:
            return new EnterpriseDatabaseManagementServiceBuilder( store.topLevelDirectory() );
        default:
            throw new RuntimeException( "Unrecognized edition: " + edition );
        }
    }

    public GraphDatabaseService inner()
    {
        return db;
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
    public int execute( String query, Map<String,Object> parameters, boolean executeInTx, boolean shouldRollback )
    {
        return executeInTx
               ? executeInTx( query, parameters, shouldRollback )
               : execute( db, query, parameters );
    }

    private int executeInTx( String query, Map<String,Object> parameters, boolean shouldRollback )
    {
        try ( Transaction tx = db.beginTx() )
        {
            int rowCount = execute( tx, query, parameters );
            if ( shouldRollback )
            {
                tx.rollback();
            }
            else
            {
                tx.commit();
            }
            return rowCount;
        }
    }

    private int execute( Transaction transaction, String query, Map<String,Object> parameters )
    {
        resultVisitor.reset();
        try ( Result result = transaction.execute( query, parameters ) )
        {
            result.accept( resultVisitor );
        }
        return resultVisitor.count();
    }

    private int execute( GraphDatabaseService db, String query, Map<String,Object> parameters )
    {
        resultVisitor.reset();
        return db.executeTransactionally( query, parameters, result ->
        {
            result.accept( resultVisitor );
            return resultVisitor.count();
        } );
    }

    public GraphDatabaseService db()
    {
        return db;
    }

    public Schema getSchema()
    {
        try ( Transaction tx = db.beginTx() )
        {
            var schema = tx.schema();
            List<Schema.SchemaEntry> entries = new ArrayList<>();

            schema
                    .getConstraints()
                    .forEach( constraint -> entries.add( constraintEntryFor( constraint ) ) );
            schema
                    .getIndexes()
                    .forEach( index ->
                              {
                                  for ( Label label : index.getLabels() )
                                  {
                                      if ( !index.isConstraintIndex() )
                                      {
                                          entries.add( new Schema.IndexSchemaEntry( label, Lists.newArrayList( index.getPropertyKeys() ) ) );
                                      }
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
            var schema = tx.schema();
            schema.getConstraints().forEach( ConstraintDefinition::drop );
            schema.getIndexes().forEach( IndexDefinition::drop );
            tx.commit();
        }
    }

    private void createSchema( Schema schema )
    {
        try ( Transaction tx = db.beginTx() )
        {
            schema.constraints()
                  .stream()
                  .map( Schema.SchemaEntry::createStatement )
                  .forEach( tx::execute );
            schema.indexes()
                  .stream()
                  .map( Schema.SchemaEntry::createStatement )
                  .forEach( tx::execute );
            tx.commit();
        }
        waitForSchema();
    }

    private void waitForSchema()
    {
        try ( Transaction tx = db.beginTx() )
        {
            var schema = tx.schema();
            for ( IndexDefinition index : schema.getIndexes() )
            {
                assertIndexNotFailed( tx, index );
                if ( schema.getIndexState( index ) != org.neo4j.graphdb.schema.Schema.IndexState.ONLINE )
                {
                    while ( schema.getIndexState( index ) == org.neo4j.graphdb.schema.Schema.IndexState.POPULATING )
                    {
                        Thread.sleep( 500 );
                    }
                    assertIndexNotFailed( tx, index );
                }
            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error while waiting for indexes to come online", e );
        }
    }

    private static void assertIndexNotFailed( Transaction transaction, IndexDefinition index )
    {
        if ( transaction.schema().getIndexState( index ) == org.neo4j.graphdb.schema.Schema.IndexState.FAILED )
        {
            throw new RuntimeException(
                    format( "Index (%s,%s) failed to build:\n%s",
                            index.getLabels(),
                            index.getPropertyKeys(),
                            transaction.schema().getIndexFailure( index )
                    )
            );
        }
    }

    @Override
    public String toString()
    {
        return "Neo4j EmbeddedDatabase\n" +
               "\t* Path:    " + store.topLevelDirectory().toAbsolutePath() + "\n" +
               "\t* Size:    " + BenchmarkUtil.bytesToString( store.bytes() );
    }

    @Override
    public void close()
    {
        if ( isRunning() )
        {
            managementService.shutdown();
        }
        else
        {
            String warningMessage = "----------------------------------------------------------------------------------------------------------------\n" +
                                    "----------------------------------------  WARNING: Unclean Shutdown!  ------------------------------------------\n" +
                                    "----------------------------------------------------------------------------------------------------------------\n" +
                                    "Neo4j was not 'running' when database was closed.\n" +
                                    "It either [a] crashed (shutdown already), or [b] did not become available in time (in process of shutting down).\n" +
                                    "If [b], the next process may fail, due to not being able to acquire store lock." +
                                    "----------------------------------------------------------------------------------------------------------------\n";
            System.err.println( warningMessage );
        }
    }
}
