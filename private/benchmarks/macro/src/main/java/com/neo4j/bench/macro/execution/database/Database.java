package com.neo4j.bench.macro.execution.database;

import com.google.common.collect.Lists;
import com.neo4j.bench.client.database.Store;
import com.neo4j.bench.client.model.Edition;
import com.neo4j.bench.client.util.BenchmarkUtil;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.dbms.StoreInfoCommand;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

public class Database implements AutoCloseable
{
    private final Store store;
    private final GraphDatabaseService db;

    public static void recreateSchema( Store store, Edition edition, Path neo4jConfig, Schema schema )
    {
        System.out.println( "Dropping schema..." );
        try ( Database db = Database.startWith( store, edition, neo4jConfig ) )
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
        try ( Database db = Database.startWith( store, edition, neo4jConfig ) )
        {
            db.createSchema( schema );
            System.out.println( "Verifying recreated schema..." );
            Database.verifySchema( db, schema );
        }
    }

    public static void verifyStoreFormat( File storeDir )
    {
        ArrayList<String> outputFromAdminTool = new ArrayList<>();

        StoreInfoCommand storeInfoCommand = new StoreInfoCommand( outputFromAdminTool::add );
        try
        {
            storeInfoCommand.execute( new String[]{"--store=" + storeDir.toPath().toString()} );
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
        try ( Database db = Database.startWith( store, edition, neo4jConfig ) )
        {
            verifySchema( db, expectedSchema );
        }
    }

    private static void verifySchema( Database db, Schema expectedSchema )
    {
        Schema.assertEqual( expectedSchema, db.getSchema() );
    }

    public static Database startWith( Store store, Edition edition, Path neo4jConfig )
    {
        GraphDatabaseService db = newDb( store, edition, neo4jConfig );
        return new Database( store, db );
    }

    private Database( Store store, GraphDatabaseService db )
    {
        this.store = store;
        this.db = db;
    }

    public boolean isRunning()
    {
        return db != null && db.isAvailable( SECONDS.toMillis( 10 ) );
    }

    private static GraphDatabaseService newDb( Store store, Edition edition, Path neo4jConfig )
    {
        GraphDatabaseBuilder builder = newBuilder( store, edition );
        if ( null != neo4jConfig )
        {
            builder.loadPropertiesFromFile( neo4jConfig.toAbsolutePath().toString() );
        }
        return builder.newGraphDatabase();
    }

    private static GraphDatabaseBuilder newBuilder( Store store, Edition edition )
    {
        switch ( edition )
        {
        case COMMUNITY:
            return new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( store.graphDbDirectory().toFile() );
        case ENTERPRISE:
            return new EnterpriseGraphDatabaseFactory().newEmbeddedDatabaseBuilder( store.graphDbDirectory().toFile() );
        default:
            throw new RuntimeException( "Unrecognized edition: " + edition );
        }
    }

    public GraphDatabaseService db()
    {
        return db;
    }

    public Store store()
    {
        return store;
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
        return "Neo4j Database\n" +
               "\t* Path:    " + store.topLevelDirectory().toAbsolutePath() + "\n" +
               "\t* Running: " + isRunning() + "\n" +
               "\t* Size:    " + BenchmarkUtil.bytesToString( store.bytes() );
    }

    @Override
    public void close()
    {
        if ( isRunning() )
        {
            db.shutdown();
        }
    }
}
