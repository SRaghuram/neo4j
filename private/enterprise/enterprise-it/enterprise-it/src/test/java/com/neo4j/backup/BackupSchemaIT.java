/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup;

import com.neo4j.backup.impl.OnlineBackupContext;
import com.neo4j.backup.impl.OnlineBackupExecutor;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.eclipse.collections.impl.factory.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.nio.file.Path;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.schema.NodePropertyExistenceException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.time.Clocks;

import static co.unruly.matchers.OptionalMatchers.empty;
import static com.neo4j.causalclustering.common.TransactionBackupServiceProvider.BACKUP_SERVER_NAME;
import static com.neo4j.configuration.OnlineBackupSettings.online_backup_enabled;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.databases_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.function.Predicates.instanceOf;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.graphdb.schema.ConstraintType.NODE_KEY;
import static org.neo4j.graphdb.schema.ConstraintType.NODE_PROPERTY_EXISTENCE;
import static org.neo4j.graphdb.schema.ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE;
import static org.neo4j.graphdb.schema.ConstraintType.UNIQUENESS;
import static org.neo4j.internal.helpers.Exceptions.findCauseOrSuppressed;
import static org.neo4j.internal.helpers.collection.Iterables.asSet;
import static org.neo4j.internal.helpers.collection.Iterators.single;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.test.PortUtils.getConnectorAddress;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
class BackupSchemaIT
{
    private static final String DB_NAME = GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

    @Inject
    private TestDirectory testDirectory;

    private GraphDatabaseAPI db;
    private static DatabaseManagementService managementService;

    @AfterEach
    void tearDown()
    {
        if ( db != null )
        {
            managementService.shutdown();
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
        db = startDb( testDirectory.homePath(), true );

        schemaElement.create( db );
        awaitIndexesOnline();
        schemaElement.populate( db );

        Path backupDir = executeBackup();
        managementService.shutdown();

        db = startDb( backupDir, false );
        schemaElement.verify( db );
    }

    private Path executeBackup() throws Exception
    {
        Path backupsDir = testDirectory.directory( "backups" ).toPath();

        var context = OnlineBackupContext.builder()
                .withAddress( "localhost", getConnectorAddress( db, BACKUP_SERVER_NAME ).getPort() )
                .withBackupDirectory( backupsDir )
                .withReportsDirectory( testDirectory.directory( "reports" ).toPath() );

        LogProvider logProvider = new Log4jLogProvider( System.out );
        OnlineBackupExecutor executor = OnlineBackupExecutor.builder()
                                                            .withUserLogProvider( logProvider )
                                                            .withInternalLogProvider( logProvider )
                                                            .withClock( Clocks.nanoClock() )
                                                            .build();

        executor.executeBackups( context );

        return backupsDir;
    }

    private void awaitIndexesOnline()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 1, MINUTES );
        }
    }

    private static GraphDatabaseAPI startDb( Path dir, boolean backupEnabled )
    {
        managementService = new TestEnterpriseDatabaseManagementServiceBuilder( dir )
                .setConfig( online_backup_enabled, backupEnabled )
                .setConfig( transaction_logs_root_path, dir.toAbsolutePath() )
                .setConfig( databases_root_path, dir.toAbsolutePath() )
                .build();
        return (GraphDatabaseAPI) managementService.database( DB_NAME );
    }

    private static long countNodeIndexes( Transaction tx, String label, String... propertyKeys )
    {
        return Iterables.stream( tx.schema().getIndexes( label( label ) ) )
                .filter( IndexDefinition::isNodeIndex )
                .filter( def -> hasPropertyKeys( def, propertyKeys ) )
                .count();
    }

    private static long countNodeConstraints( Transaction tx, String label, ConstraintType type, String... propertyKeys )
    {
        return Iterables.stream( tx.schema().getConstraints( label( label ) ) )
                .filter( def -> def.isConstraintType( type ) )
                .filter( def -> hasPropertyKeys( def, propertyKeys ) )
                .count();
    }

    private static long countRelationshipConstraints( Transaction tx, String typeName, ConstraintType type, String... propertyKeys )
    {
        return Iterables.stream( tx.schema().getConstraints( withName( typeName ) ) )
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
            try ( Transaction transaction = db.beginTx() )
            {
                transaction.execute( "CREATE INDEX FOR (n:Person) ON (n.name)" ).close();
                transaction.commit();
            }
        }

        @Override
        public void populate( GraphDatabaseService db )
        {
            try ( Transaction transaction = db.beginTx() )
            {
                transaction.execute( "UNWIND range(1, 42) AS x CREATE (:Person {name: '42'})" ).close();
                transaction.commit();
            }
        }

        @Override
        public void verify( GraphDatabaseService db )
        {
            try ( Transaction tx = db.beginTx() )
            {
                assertEquals( 1, countNodeIndexes( tx, "Person", "name" ) );
                assertEquals( 42L, single( tx.execute( "MATCH (n:Person {name: '42'}) RETURN count(n) AS count" ) ).get( "count" ) );
            }
        }
    }

    private static class CompositeIndex implements SchemaElement
    {
        @Override
        public void create( GraphDatabaseService db )
        {
            try ( Transaction transaction = db.beginTx() )
            {
                transaction.execute( "CREATE INDEX FOR (n:Person) ON (n.name, n.age)" ).close();
                transaction.commit();
            }
        }

        @Override
        public void populate( GraphDatabaseService db )
        {
            try ( Transaction transaction = db.beginTx() )
            {
                transaction.execute( "UNWIND range(1, 42) AS x CREATE (:Person {name: '42', age: 42})" ).close();
                transaction.commit();
            }
        }

        @Override
        public void verify( GraphDatabaseService db )
        {
            try ( Transaction tx = db.beginTx() )
            {
                assertEquals( 1, countNodeIndexes( tx, "Person", "name", "age" ) );
                assertEquals( 42L, single( tx.execute( "MATCH (n:Person {name: '42', age: 42}) RETURN count(n) AS count" ) ).get( "count" ) );
            }
        }
    }

    private static class FulltextNodeIndex implements SchemaElement
    {
        @Override
        public void create( GraphDatabaseService db )
        {
            try ( Transaction transaction = db.beginTx() )
            {
                transaction.execute( "CALL db.index.fulltext.createNodeIndex('idx',['Person'],['name'])" ).close();
                transaction.commit();
            }
        }

        @Override
        public void populate( GraphDatabaseService db )
        {
            try ( Transaction transaction = db.beginTx() )
            {
                transaction.execute( "UNWIND range(1, 42) AS x CREATE (:Person {name: '42'})" ).close();
                transaction.commit();
            }
        }

        @Override
        public void verify( GraphDatabaseService db )
        {
            try ( Transaction transaction = db.beginTx() )
            {
                Result result = transaction.execute( "CALL db.index.fulltext.queryNodes('idx', '42') YIELD node RETURN count(node) AS count" );
                assertEquals( 42L, single( result ).get( "count" ) );
                transaction.commit();
            }
        }
    }

    private static class FulltextRelationshipIndex implements SchemaElement
    {
        @Override
        public void create( GraphDatabaseService db )
        {
            try ( Transaction transaction = db.beginTx() )
            {
                transaction.execute( "CALL db.index.fulltext.createRelationshipIndex('idx',['LIKES'],['name'])" ).close();
                transaction.commit();
            }
        }

        @Override
        public void populate( GraphDatabaseService db )
        {
            try ( Transaction transaction = db.beginTx() )
            {
                transaction.execute( "UNWIND range(1, 42) AS x CREATE (:Person)-[:LIKES {name: '42'}]->(:Person)" ).close();
                transaction.commit();
            }
        }

        @Override
        public void verify( GraphDatabaseService db )
        {
            try ( Transaction transaction = db.beginTx() )
            {
                Result result = transaction
                        .execute( "CALL db.index.fulltext.queryRelationships('idx', '42') YIELD relationship RETURN count(relationship) AS count" );
                assertEquals( 42L, single( result ).get( "count" ) );
            }
        }
    }

    private static class UniqueNodePropertyConstraint implements SchemaElement
    {
        @Override
        public void create( GraphDatabaseService db )
        {
            try ( Transaction transaction = db.beginTx() )
            {
                transaction.execute( "CREATE CONSTRAINT ON (p:Person) ASSERT p.name IS UNIQUE" ).close();
                transaction.commit();
            }
        }

        @Override
        public void populate( GraphDatabaseService db )
        {
            try ( Transaction transaction = db.beginTx() )
            {
                transaction.execute( "UNWIND range(1, 42) AS x CREATE (:Person {name: toString(x)})" ).close();
                transaction.commit();
            }
        }

        @Override
        public void verify( GraphDatabaseService db )
        {
            try ( Transaction tx = db.beginTx() )
            {
                assertEquals( 1, countNodeConstraints( tx, "Person", UNIQUENESS, "name" ) );
                for ( int i = 1; i <= 42; i++ )
                {
                    assertNotNull( tx.findNode( label( "Person" ), "name", String.valueOf( i ) ) );
                }
            }
            try ( Transaction transaction = db.beginTx() )
            {
                QueryExecutionException error = assertThrows( QueryExecutionException.class,
                        () -> transaction.execute( "CREATE (:Person {name: '1'})" ).close() );
                assertThat( findCauseOrSuppressed( error, instanceOf( IndexEntryConflictException.class ) ), not( empty() ) );
            }
        }
    }

    private static class NodeKey implements SchemaElement
    {
        @Override
        public void create( GraphDatabaseService db )
        {
            try ( Transaction transaction = db.beginTx() )
            {
                transaction.execute( "CREATE CONSTRAINT ON (p:Person) ASSERT (p.name, p.age) IS NODE KEY" ).close();
                transaction.commit();
            }
        }

        @Override
        public void populate( GraphDatabaseService db )
        {
            try ( Transaction transaction = db.beginTx() )
            {
                transaction.execute( "UNWIND range(1, 42) AS x CREATE (:Person {name: toString(x), age: x})" ).close();
                transaction.commit();
            }
        }

        @Override
        public void verify( GraphDatabaseService db )
        {
            try ( Transaction tx = db.beginTx() )
            {
                assertEquals( 1, countNodeConstraints( tx, "Person", NODE_KEY, "name", "age" ) );
                for ( int i = 1; i <= 42; i++ )
                {
                    Result result = tx.execute( "MATCH (p:Person {name: $name, age: $age}) RETURN count(p) AS count",
                            map( "name", String.valueOf( i ), "age", i ) );
                    assertEquals( 1L, single( result ).get( "count" ) );
                }
            }
            try ( Transaction transaction = db.beginTx() )
            {
                QueryExecutionException error =
                        assertThrows( QueryExecutionException.class, () -> transaction.execute( "CREATE (:Person {name: '1', age: 1})" ).close() );
                assertThat( findCauseOrSuppressed( error, instanceOf( IndexEntryConflictException.class ) ), not( empty() ) );
            }
        }
    }

    private static class NodePropertyExistenceConstraint implements SchemaElement
    {
        @Override
        public void create( GraphDatabaseService db )
        {
            try ( Transaction transaction = db.beginTx() )
            {
                transaction.execute( "CREATE CONSTRAINT ON (p:Person) ASSERT exists(p.name)" ).close();
                transaction.commit();
            }
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
                assertEquals( 1, countNodeConstraints( tx, "Person", NODE_PROPERTY_EXISTENCE, "name" ) );
                ConstraintViolationException error = assertThrows( ConstraintViolationException.class, () -> {
                    tx.execute( "CREATE (:Person)" ).close();
                    tx.commit();
                } );
                assertThat( findCauseOrSuppressed( error, instanceOf( NodePropertyExistenceException.class ) ), not( empty() ) );
            }
        }
    }

    private static class RelationshipPropertyExistenceConstraint implements SchemaElement
    {
        @Override
        public void create( GraphDatabaseService db )
        {
            try ( Transaction transaction = db.beginTx() )
            {
                transaction.execute( "CREATE CONSTRAINT ON ()-[like:LIKES]-() ASSERT exists(like.name)" ).close();
                transaction.commit();
            }
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
                assertEquals( 1, countRelationshipConstraints( tx, "LIKES", RELATIONSHIP_PROPERTY_EXISTENCE, "name" ) );
                ConstraintViolationException error = assertThrows( ConstraintViolationException.class, () -> {
                    tx.execute( "CREATE ()-[:LIKES]->()" ).close();
                    tx.commit();
                } );
                assertThat( findCauseOrSuppressed( error, instanceOf( ConstraintViolationException.class ) ), not( empty() ) );
            }
        }
    }
}
