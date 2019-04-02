/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup;

import com.neo4j.test.TestCommercialGraphDatabaseFactory;
import org.eclipse.collections.impl.factory.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.nio.file.Path;

import org.neo4j.backup.impl.OnlineBackupContext;
import org.neo4j.backup.impl.OnlineBackupExecutor;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static co.unruly.matchers.OptionalMatchers.empty;
import static com.neo4j.causalclustering.core.TransactionBackupServiceProvider.BACKUP_SERVER_NAME;
import static com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings.online_backup_enabled;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.function.Predicates.instanceOf;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.graphdb.schema.ConstraintType.NODE_KEY;
import static org.neo4j.graphdb.schema.ConstraintType.NODE_PROPERTY_EXISTENCE;
import static org.neo4j.graphdb.schema.ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE;
import static org.neo4j.graphdb.schema.ConstraintType.UNIQUENESS;
import static org.neo4j.helpers.Exceptions.findCauseOrSuppressed;
import static org.neo4j.helpers.collection.Iterables.asSet;
import static org.neo4j.helpers.collection.Iterators.single;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.test.PortUtils.getConnectorAddress;

@ExtendWith( {TestDirectoryExtension.class, SuppressOutputExtension.class} )
class BackupSchemaIT
{
    private static final String DB_NAME = GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

    @Inject
    private TestDirectory testDirectory;

    private GraphDatabaseAPI db;

    @AfterEach
    void tearDown()
    {
        if ( db != null )
        {
            db.shutdown();
        }
    }

    @Test
    void shouldBackupSinglePropertyIndex() throws Exception
    {
        testBackup( new SinglePropertyIndex() );
    }

    @Test
    void shouldBackupCompositeIndex() throws Exception
    {
        testBackup( new CompositeIndex() );
    }

    @Test
    void shouldBackupFulltextNodeIndex() throws Exception
    {
        testBackup( new FulltextNodeIndex() );
    }

    @Test
    void shouldBackupFulltextRelationshipIndex() throws Exception
    {
        testBackup( new FulltextRelationshipIndex() );
    }

    @Test
    void shouldBackupUniqueNodePropertyConstraint() throws Exception
    {
        testBackup( new UniqueNodePropertyConstraint() );
    }

    @Test
    void shouldBackupNodeKey() throws Exception
    {
        testBackup( new NodeKey() );
    }

    @Test
    void shouldBackupNodePropertyExistenceConstraint() throws Exception
    {
        testBackup( new NodePropertyExistenceConstraint() );
    }

    @Test
    void shouldBackupRelationshipPropertyExistenceConstraint() throws Exception
    {
        testBackup( new RelationshipPropertyExistenceConstraint() );
    }

    private void testBackup( SchemaElement schemaElement ) throws Exception
    {
        db = startDb( testDirectory.databaseDir(), true );

        schemaElement.create( db );
        awaitIndexesOnline();
        schemaElement.populate( db );

        File backupDir = executeBackup();
        db.shutdown();

        db = startDb( backupDir, false );
        schemaElement.verify( db );
    }

    private File executeBackup() throws Exception
    {
        Path backupsDir = testDirectory.directory( "backups" ).toPath();

        OnlineBackupContext context = OnlineBackupContext.builder()
                .withAddress( "localhost", getConnectorAddress( db, BACKUP_SERVER_NAME ).getPort() )
                .withDatabaseId( new DatabaseId( DB_NAME ) )
                .withBackupDirectory( backupsDir )
                .build();

        LogProvider logProvider = FormattedLogProvider.toOutputStream( System.out );
        OnlineBackupExecutor executor = OnlineBackupExecutor.builder()
                .withUserLogProvider( logProvider )
                .withInternalLogProvider( logProvider )
                .build();

        executor.executeBackup( context );

        return backupsDir.resolve( DB_NAME ).toFile();
    }

    private void awaitIndexesOnline()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, MINUTES );
        }
    }

    private static GraphDatabaseAPI startDb( File dir, boolean backupEnabled )
    {
        return (GraphDatabaseAPI) new TestCommercialGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( dir )
                .setConfig( online_backup_enabled, Boolean.toString( backupEnabled ) )
                .setConfig( transaction_logs_root_path, dir.getParent() )
                .newGraphDatabase();
    }

    private static long countNodeIndexes( GraphDatabaseService db, String label, String... propertyKeys )
    {
        return Iterables.stream( db.schema().getIndexes( label( label ) ) )
                .filter( IndexDefinition::isNodeIndex )
                .filter( def -> hasPropertyKeys( def, propertyKeys ) )
                .count();
    }

    private static long countNodeConstraints( GraphDatabaseService db, String label, ConstraintType type, String... propertyKeys )
    {
        return Iterables.stream( db.schema().getConstraints( label( label ) ) )
                .filter( def -> def.isConstraintType( type ) )
                .filter( def -> hasPropertyKeys( def, propertyKeys ) )
                .count();
    }

    private static long countRelationshipConstraints( GraphDatabaseService db, String typeName, ConstraintType type, String... propertyKeys )
    {
        return Iterables.stream( db.schema().getConstraints( withName( typeName ) ) )
                .filter( def -> def.isConstraintType( type ) )
                .filter( def -> hasPropertyKeys( def, propertyKeys ) )
                .count();
    }

    private static boolean hasPropertyKeys( IndexDefinition definition, String... keys )
    {
        return asSet( definition.getPropertyKeys() ).equals( Sets.immutable.of( keys ) );
    }

    private static boolean hasPropertyKeys( ConstraintDefinition definition, String... keys )
    {
        return asSet( definition.getPropertyKeys() ).equals( Sets.immutable.of( keys ) );
    }

    private interface SchemaElement
    {
        void create( GraphDatabaseService db );

        void populate( GraphDatabaseService db );

        void verify( GraphDatabaseService db );
    }

    private static class SinglePropertyIndex implements SchemaElement
    {
        @Override
        public void create( GraphDatabaseService db )
        {
            db.execute( "CREATE INDEX ON :Person(name)" ).close();
        }

        @Override
        public void populate( GraphDatabaseService db )
        {
            db.execute( "UNWIND range(1, 42) AS x CREATE (:Person {name: '42'})" ).close();
        }

        @Override
        public void verify( GraphDatabaseService db )
        {
            try ( Transaction tx = db.beginTx() )
            {
                assertEquals( 1, countNodeIndexes( db, "Person", "name" ) );
                assertEquals( 42L, single( db.execute( "MATCH (n:Person {name: '42'}) RETURN count(n) AS count" ) ).get( "count" ) );
            }
        }
    }

    private static class CompositeIndex implements SchemaElement
    {
        @Override
        public void create( GraphDatabaseService db )
        {
            db.execute( "CREATE INDEX ON :Person(name, age)" ).close();
        }

        @Override
        public void populate( GraphDatabaseService db )
        {
            db.execute( "UNWIND range(1, 42) AS x CREATE (:Person {name: '42', age: 42})" ).close();
        }

        @Override
        public void verify( GraphDatabaseService db )
        {
            try ( Transaction tx = db.beginTx() )
            {
                assertEquals( 1, countNodeIndexes( db, "Person", "name", "age" ) );
                assertEquals( 42L, single( db.execute( "MATCH (n:Person {name: '42', age: 42}) RETURN count(n) AS count" ) ).get( "count" ) );
            }
        }
    }

    private static class FulltextNodeIndex implements SchemaElement
    {
        @Override
        public void create( GraphDatabaseService db )
        {
            db.execute( "CALL db.index.fulltext.createNodeIndex('idx',['Person'],['name'])" ).close();
        }

        @Override
        public void populate( GraphDatabaseService db )
        {
            db.execute( "UNWIND range(1, 42) AS x CREATE (:Person {name: '42'})" ).close();
        }

        @Override
        public void verify( GraphDatabaseService db )
        {
            Result result = db.execute( "CALL db.index.fulltext.queryNodes('idx', '42') YIELD node RETURN count(node) AS count" );
            assertEquals( 42L, single( result ).get( "count" ) );
        }
    }

    private static class FulltextRelationshipIndex implements SchemaElement
    {
        @Override
        public void create( GraphDatabaseService db )
        {
            db.execute( "CALL db.index.fulltext.createRelationshipIndex('idx',['LIKES'],['name'])" ).close();
        }

        @Override
        public void populate( GraphDatabaseService db )
        {
            db.execute( "UNWIND range(1, 42) AS x CREATE (:Person)-[:LIKES {name: '42'}]->(:Person)" ).close();
        }

        @Override
        public void verify( GraphDatabaseService db )
        {
            Result result = db.execute( "CALL db.index.fulltext.queryRelationships('idx', '42') YIELD relationship RETURN count(relationship) AS count" );
            assertEquals( 42L, single( result ).get( "count" ) );
        }
    }

    private static class UniqueNodePropertyConstraint implements SchemaElement
    {
        @Override
        public void create( GraphDatabaseService db )
        {
            db.execute( "CREATE CONSTRAINT ON (p:Person) ASSERT p.name IS UNIQUE" ).close();
        }

        @Override
        public void populate( GraphDatabaseService db )
        {
            db.execute( "UNWIND range(1, 42) AS x CREATE (:Person {name: toString(x)})" ).close();
        }

        @Override
        public void verify( GraphDatabaseService db )
        {
            try ( Transaction tx = db.beginTx() )
            {
                assertEquals( 1, countNodeConstraints( db, "Person", UNIQUENESS, "name" ) );
                for ( int i = 1; i <= 42; i++ )
                {
                    assertNotNull( db.findNode( label( "Person" ), "name", String.valueOf( i ) ) );
                }
            }
            QueryExecutionException error = assertThrows( QueryExecutionException.class, () -> db.execute( "CREATE (:Person {name: '1'})" ).close() );
            assertThat( findCauseOrSuppressed( error, instanceOf( IndexEntryConflictException.class ) ), not( empty() ) );
        }
    }

    private static class NodeKey implements SchemaElement
    {
        @Override
        public void create( GraphDatabaseService db )
        {
            db.execute( "CREATE CONSTRAINT ON (p:Person) ASSERT (p.name, p.age) IS NODE KEY" ).close();
        }

        @Override
        public void populate( GraphDatabaseService db )
        {
            db.execute( "UNWIND range(1, 42) AS x CREATE (:Person {name: toString(x), age: x})" ).close();
        }

        @Override
        public void verify( GraphDatabaseService db )
        {
            try ( Transaction tx = db.beginTx() )
            {
                assertEquals( 1, countNodeConstraints( db, "Person", NODE_KEY, "name", "age" ) );
                for ( int i = 1; i <= 42; i++ )
                {
                    Result result = db.execute( "MATCH (p:Person {name: $name, age: $age}) RETURN count(p) AS count",
                            map( "name", String.valueOf( i ), "age", i ) );
                    assertEquals( 1L, single( result ).get( "count" ) );
                }
            }
            QueryExecutionException error = assertThrows( QueryExecutionException.class, () -> db.execute( "CREATE (:Person {name: '1', age: 1})" ).close() );
            assertThat( findCauseOrSuppressed( error, instanceOf( IndexEntryConflictException.class ) ), not( empty() ) );
        }
    }

    private static class NodePropertyExistenceConstraint implements SchemaElement
    {
        @Override
        public void create( GraphDatabaseService db )
        {
            db.execute( "CREATE CONSTRAINT ON (p:Person) ASSERT exists(p.name)" ).close();
        }

        @Override
        public void populate( GraphDatabaseService db )
        {
        }

        @Override
        public void verify( GraphDatabaseService db )
        {
            try ( Transaction tx = db.beginTx() )
            {
                assertEquals( 1, countNodeConstraints( db, "Person", NODE_PROPERTY_EXISTENCE, "name" ) );
            }
            QueryExecutionException error = assertThrows( QueryExecutionException.class, () -> db.execute( "CREATE (:Person)" ).close() );
            assertThat( findCauseOrSuppressed( error, instanceOf( ConstraintViolationException.class ) ), not( empty() ) );
        }
    }

    private static class RelationshipPropertyExistenceConstraint implements SchemaElement
    {
        @Override
        public void create( GraphDatabaseService db )
        {
            db.execute( "CREATE CONSTRAINT ON ()-[like:LIKES]-() ASSERT exists(like.name)" ).close();
        }

        @Override
        public void populate( GraphDatabaseService db )
        {
        }

        @Override
        public void verify( GraphDatabaseService db )
        {
            try ( Transaction tx = db.beginTx() )
            {
                assertEquals( 1, countRelationshipConstraints( db, "LIKES", RELATIONSHIP_PROPERTY_EXISTENCE, "name" ) );
            }
            QueryExecutionException error = assertThrows( QueryExecutionException.class, () -> db.execute( "CREATE ()-[:LIKES]->()" ).close() );
            assertThat( findCauseOrSuppressed( error, instanceOf( ConstraintViolationException.class ) ), not( empty() ) );
        }
    }
}
