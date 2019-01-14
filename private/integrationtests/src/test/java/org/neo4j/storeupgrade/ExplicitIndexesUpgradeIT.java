/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.storeupgrade;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Map;
import java.util.function.IntFunction;

import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.function.Factory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.index.lucene.ValueContext;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByConfigurationException;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.Unzip;
import org.neo4j.test.matchers.NestedThrowableMatcher;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.neo4j.helpers.collection.Iterators.single;
import static org.neo4j.index.impl.lucene.explicit.LuceneIndexImplementation.EXACT_CONFIG;
import static org.neo4j.index.impl.lucene.explicit.LuceneIndexImplementation.FULLTEXT_CONFIG;

public class ExplicitIndexesUpgradeIT
{
    @Rule
    public final TestDirectory testDir = TestDirectory.testDirectory();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    @Test
    public void successfulMigrationWithoutExplicitIndexes() throws Exception
    {
        prepareStore( "empty-explicit-index-db.zip" );
        GraphDatabaseService db = startDatabase( true );
        try
        {
            checkDbAccessible( db );
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void successfulMigrationExplicitIndexes() throws Exception
    {
        prepareStore( "explicit-index-db.zip" );

        GraphDatabaseService db = startDatabase( true );
        try
        {
            checkDbAccessible( db );
            checkIndexData( db );
        }
        finally
        {
            db.shutdown();
        }

        checkMigrationProgressFeedback();
    }

    @Test
    public void migrationShouldFailIfUpgradeNotAllowed() throws IOException
    {
        prepareStore( "explicit-index-db.zip" );
        expectedException.expect( new NestedThrowableMatcher( UpgradeNotAllowedByConfigurationException.class ) );

        startDatabase( false );
    }

    private static void checkDbAccessible( GraphDatabaseService db )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            assertNotNull( db.getNodeById( 1 ) );
            transaction.success();
        }
    }

    private GraphDatabaseService startDatabase( boolean allowUpgrade )
    {
        GraphDatabaseFactory factory = new TestGraphDatabaseFactory();
        GraphDatabaseBuilder builder = factory.newEmbeddedDatabaseBuilder( testDir.databaseDir() );
        builder.setConfig( GraphDatabaseSettings.allow_upgrade, Boolean.toString( allowUpgrade ) );
        builder.setConfig( GraphDatabaseSettings.pagecache_memory, "8m" );
        builder.setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE );
        return builder.newGraphDatabase();
    }

    private static void checkIndexData( GraphDatabaseService db )
    {
        IntFunction<String> keyFactory = basicKeyFactory();
        Factory<Node> readNodes = readNodes( db );
        readIndex( db, nodeIndex( db, "node-1", EXACT_CONFIG ), readNodes, keyFactory, stringValues() );
        readIndex( db, nodeIndex( db, "node-2", EXACT_CONFIG ), readNodes, keyFactory, intValues() );
        readIndex( db, nodeIndex( db, "node-3", FULLTEXT_CONFIG ), readNodes, keyFactory, stringValues() );
        readIndex( db, nodeIndex( db, "node-4", FULLTEXT_CONFIG ), readNodes, keyFactory, longValues() );
        Factory<Relationship> relationships = readRelationships( db );
        readIndex( db, relationshipIndex( db, "rel-1", EXACT_CONFIG ), relationships, keyFactory, stringValues() );
        readIndex( db, relationshipIndex( db, "rel-2", EXACT_CONFIG ), relationships, keyFactory, floatValues() );
        readIndex( db, relationshipIndex( db, "rel-3", FULLTEXT_CONFIG ), relationships, keyFactory, stringValues() );
        readIndex( db, relationshipIndex( db, "rel-4", FULLTEXT_CONFIG ), relationships, keyFactory, doubleValues() );
    }

    private void prepareStore( String store ) throws IOException
    {
        Unzip.unzip( getClass(), store, testDir.databaseDir() );
    }

    private static IntFunction<Object> intValues()
    {
        return ValueContext::numeric;
    }

    private static IntFunction<Object> longValues()
    {
        return value -> ValueContext.numeric( (long) value );
    }

    private static IntFunction<Object> floatValues()
    {
        return value -> ValueContext.numeric( (float) value );
    }

    private static IntFunction<Object> doubleValues()
    {
        return value -> ValueContext.numeric( (double) value );
    }

    private static IntFunction<Object> stringValues()
    {
        return value -> "value balue " + value;
    }

    private static Factory<Node> readNodes( final GraphDatabaseService db )
    {
        return new Factory<Node>()
        {
            private long id;

            @Override
            public Node newInstance()
            {
                return db.getNodeById( id++ );
            }
        };
    }

    private static Factory<Relationship> readRelationships( final GraphDatabaseService db )
    {
        return new Factory<Relationship>()
        {
            private long id;

            @Override
            public Relationship newInstance()
            {
                return db.getRelationshipById( id++ );
            }
        };
    }

    private static Index<Node> nodeIndex( GraphDatabaseService db, String name, Map<String,String> config )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Index<Node> index = db.index().forNodes( name, config );
            tx.success();
            return index;
        }
    }

    private static RelationshipIndex relationshipIndex( GraphDatabaseService db, String name, Map<String,String> config )
    {
        try ( Transaction tx = db.beginTx() )
        {
            RelationshipIndex index = db.index().forRelationships( name, config );
            tx.success();
            return index;
        }
    }

    private static <ENTITY extends PropertyContainer> void readIndex( GraphDatabaseService db, Index<ENTITY> index, Factory<ENTITY> entityFactory,
            IntFunction<String> keyFactory, IntFunction<Object> valueFactory )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < 10; i++ )
            {
                ENTITY entity = entityFactory.newInstance();
                String key = keyFactory.apply( i );
                Object value = valueFactory.apply( i );
                assertEquals( entity, single( index.get( key, value ) ) );
            }
            tx.success();
        }
    }

    private static IntFunction<String> basicKeyFactory()
    {
        return value -> "key-" + (value % 3);
    }

    private void checkMigrationProgressFeedback()
    {
        suppressOutput.getOutputVoice().containsMessage( "Starting upgrade of database" );
        suppressOutput.getOutputVoice().containsMessage( "Successfully finished upgrade of database" );
        suppressOutput.getOutputVoice().containsMessage( "10%" );
        suppressOutput.getOutputVoice().containsMessage( "100%" );
    }
}
