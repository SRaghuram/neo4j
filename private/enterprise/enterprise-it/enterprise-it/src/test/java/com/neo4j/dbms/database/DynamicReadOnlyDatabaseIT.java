/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.WriteOperationsNotAllowedException;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

import static java.lang.String.join;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only_databases;
import static org.neo4j.configuration.GraphDatabaseSettings.writable_databases;
import static org.neo4j.internal.helpers.collection.Iterators.count;
import static org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStoreSettings.enable_relationship_type_scan_store;
import static org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStoreSettings.enable_scan_stores_as_token_indexes;

@EnterpriseDbmsExtension( configurationCallback = "configure" )
class DynamicReadOnlyDatabaseIT
{
    @Inject
    private DatabaseManagementService managementService;
    @Inject
    private GraphDatabaseAPI database;

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( enable_relationship_type_scan_store, true );
    }

    @Test
    void canMakeDatabaseReadOnly()
    {
        makeDbReadOnly();
        var e = assertThrows( Exception.class, () ->
        {
            try ( Transaction transaction = database.beginTx() )
            {
                transaction.createNode();
                transaction.commit();
            }
        } );
        assertThat( e ).isInstanceOf( WriteOperationsNotAllowedException.class );
    }

    @Test
    void executeEmptyTransactionAfterWriteDbSwitch()
    {
        makeDbReadOnly();
        makeDbWritable();
        try ( var transaction = database.beginTx() )
        {
            // empty
        }
    }

    @Test
    void createNodeTransactionAfterWriteDbSwitch()
    {
        makeDbReadOnly();
        makeDbWritable();
        try ( var transaction = database.beginTx() )
        {
            transaction.createNode();
            transaction.commit();
        }
    }

    @Test
    void createNodeWithLabelTransactionAfterWriteDbSwitch()
    {
        makeDbReadOnly();
        makeDbWritable();
        try ( var transaction = database.beginTx() )
        {
            transaction.createNode( Label.label( "marker" ) );
            transaction.commit();
        }
    }

    @Test
    void acquireLocksAfterWriteDbSwitch()
    {
        makeDbReadOnly();
        makeDbWritable();
        try ( var transaction = database.beginTx() )
        {
            transaction.createNode( Label.label( "marker" ) ).getId();
            transaction.commit();
        }

        try ( var transaction = database.beginTx() )
        {
            try ( var transactionNodes = transaction.findNodes( Label.label( "marker" ) ) )
            {
                var markerNode = transactionNodes.next();
                var readLock = transaction.acquireReadLock( markerNode );
                var writeLock = transaction.acquireWriteLock( markerNode );
            }
            transaction.rollback();
        }
    }

    @Test
    void createNodeAndRelationshipsWithPropertiesAfterWriteDbSwitch()
    {
        makeDbReadOnly();
        makeDbWritable();
        try ( var transaction = database.beginTx() )
        {
            var node1 = transaction.createNode();
            var node2 = transaction.createNode();
            node1.setProperty( "a1", "a1" );
            node2.setProperty( "a2", "a2" );
            var relationship = node1.createRelationshipTo( node2, RelationshipType.withName( "marker" ) );
            relationship.setProperty( "rel1", "rel1" );
            transaction.commit();
        }
        try ( var transaction = database.beginTx() )
        {
            try ( var allNodes = transaction.getAllNodes().iterator() )
            {
                assertEquals( 2, count( allNodes ) );
            }
        }
    }

    @Test
    void createIndexAfterWriteDbSwitch()
    {
        makeDbReadOnly();
        String propertyName = "indexedProperty";
        Label indexedLabel = Label.label( "indexedLabel" );

        makeDbWritable();
        try ( var transaction = database.beginTx() )
        {
            transaction.schema().indexFor( indexedLabel ).on( propertyName ).create();
            transaction.commit();
        }
        try ( var transaction = database.beginTx() )
        {
            transaction.schema().awaitIndexesOnline( 1, TimeUnit.DAYS );
            assertEquals( 1, Iterables.count( transaction.schema().getIndexes() ) );
        }
        try ( var transaction = database.beginTx() )
        {
            Node node = transaction.createNode( indexedLabel );
            node.setProperty( propertyName, propertyName );
            transaction.commit();
        }
    }

    @Test
    void createFulltextIndexAfterWriteDbSwitch()
    {
        makeDbReadOnly();
        String propertyName = "indexedProperty";
        Label indexedLabel = Label.label( "indexedLabel" );

        makeDbWritable();

        createFulltextNodeIndex( database, indexedLabel, propertyName );
        try ( var transaction = database.beginTx() )
        {
            transaction.schema().awaitIndexesOnline( 1, TimeUnit.DAYS );
            assertEquals( 1, Iterables.count( transaction.schema().getIndexes() ) );
        }
        try ( var transaction = database.beginTx() )
        {
            Node node = transaction.createNode( indexedLabel );
            node.setProperty( propertyName, propertyName );
            transaction.commit();
        }
    }

    @Test
    void failToDropFulltextIndexAfterReadOnlyDbSwitch()
    {
        makeDbReadOnly();
        String propertyName = "indexedProperty";
        Label indexedLabel = Label.label( "indexedLabel" );

        makeDbWritable();

        createFulltextNodeIndex( database, indexedLabel, propertyName );
        try ( var transaction = database.beginTx() )
        {
            transaction.schema().awaitIndexesOnline( 1, TimeUnit.DAYS );
            assertEquals( 1, Iterables.count( transaction.schema().getIndexes() ) );
        }
        try ( var transaction = database.beginTx() )
        {
            Node node = transaction.createNode( indexedLabel );
            node.setProperty( propertyName, propertyName );
            transaction.commit();
        }

        makeDbReadOnly();

        var e = assertThrows( Throwable.class, () -> database.executeTransactionally( "CALL db.index.fulltext.drop('ftsNodes')" ) );
        assertThat( e ).hasRootCauseInstanceOf( WriteOperationsNotAllowedException.class );
    }

    @Test
    void failToDropLuceneIndexAfterReadOnlyDbSwitch()
    {
        String propertyName = "indexedProperty";
        Label indexedLabel = Label.label( "Label" );
        makeDbWritable();

        database.executeTransactionally( "CREATE INDEX testIndex FOR (n:Label) ON (n.indexedProperty) OPTIONS { indexProvider: 'lucene+native-3.0'}" );
        try ( var transaction = database.beginTx() )
        {
            transaction.schema().awaitIndexesOnline( 1, TimeUnit.DAYS );
            assertEquals( 1, Iterables.count( transaction.schema().getIndexes() ) );
        }

        assertDoesNotThrow( () ->
        {
            try ( var transaction = database.beginTx() )
            {
                Node node = transaction.createNode( indexedLabel );
                node.setProperty( propertyName, propertyName );
                transaction.commit();
            }
        } );

        makeDbReadOnly();

        try ( Transaction transaction = database.beginTx() )
        {
            assertThrows( WriteOperationsNotAllowedException.class, () -> transaction.schema().getIndexByName( "testIndex" ).drop() );
        }
    }

    @Test
    void markedAsReadOnlyDatabasePreserveNodesAfterRestart()
    {
        makeDbReadOnly();

        var e = assertThrows( Exception.class, () ->
        {
            try ( var tx = database.beginTx() )
            {
                tx.createNode();
            }
        } );
        assertThat( e ).isInstanceOf( WriteOperationsNotAllowedException.class );

        makeDbWritable();
        try ( Transaction tx = database.beginTx() )
        {
            tx.createNode();
            tx.commit();
        }
        makeDbReadOnly();

        restartDatabase();

        try ( var transaction = database.beginTx() )
        {
            assertEquals( 1, Iterables.count( transaction.getAllNodes() ) );
        }
    }

    @Test
    void markedAsReadOnlyDatabasePreserveLabelAndPropertyTokensAfterRestart()
    {
        var marker = Label.label( "marker" );
        var markerType = RelationshipType.withName( "markerType" );

        makeDbReadOnly();

        var e = assertThrows( Exception.class, () ->
        {
            try ( var tx = database.beginTx() )
            {
                tx.createNode();
            }
        } );
        assertThat( e ).isInstanceOf( WriteOperationsNotAllowedException.class );

        makeDbWritable();
        try ( var transaction = database.beginTx() )
        {
            for ( int i = 0; i < 100; i++ )
            {
                var start = transaction.createNode(marker);
                var end = transaction.createNode(marker);
                start.createRelationshipTo( end, markerType );
            }
            transaction.commit();
        }

        restartDatabase();

        try ( Transaction transaction = database.beginTx() )
        {
            assertEquals( 100, Iterators.count( transaction.findRelationships( markerType ) ) );
            assertEquals( 200, Iterators.count( transaction.findNodes( marker ) ) );
        }
    }

    @Test
    void markedAsReadOnlyDatabasePreserveIndexedValueAfterRestart()
    {
        var marker = Label.label( "marker" );
        var propertyName = "markerProperty";

        makeDbReadOnly();

        var e = assertThrows( Exception.class, () ->
        {
            try ( var tx = database.beginTx() )
            {
                tx.createNode();
            }
        } );
        assertThat( e ).isInstanceOf( WriteOperationsNotAllowedException.class );

        makeDbWritable();
        try ( var transaction = database.beginTx() )
        {
            transaction.schema().indexFor( marker ).on( propertyName ).create();
            transaction.commit();
        }
        try ( var transaction = database.beginTx() )
        {
            transaction.schema().awaitIndexesOnline( 1, TimeUnit.DAYS );
            assertEquals( 1, Iterables.count( transaction.schema().getIndexes() ) );
        }

        try ( var transaction = database.beginTx() )
        {
            for ( int i = 0; i < 100; i++ )
            {
                var node = transaction.createNode(marker);
                node.setProperty( propertyName, i );
            }
            transaction.commit();
        }

        restartDatabase();
        makeDbReadOnly();

        try ( Transaction transaction = database.beginTx() )
        {
            for ( int i = 0; i < 100; i++ )
            {
                assertNotNull( transaction.findNode( marker, propertyName, i ) );
            }
        }
    }

    private void restartDatabase()
    {
        managementService.shutdownDatabase( database.databaseName() );
        managementService.startDatabase( database.databaseName() );
        database = (GraphDatabaseAPI) managementService.database( database.databaseName() );
    }

    private static void createFulltextNodeIndex( GraphDatabaseService db, Label label, String... propertyKeys )
    {
        db.executeTransactionally( "CALL db.index.fulltext.createNodeIndex('ftsNodes', ['" + label.name() + "'], ['" + join( "','", propertyKeys ) + "'] )" );
    }

    private void makeDbReadOnly()
    {
        Config config = database.getDependencyResolver().resolveDependency( Config.class );
        var readOnlyDbs = new HashSet<>( config.get( read_only_databases ) );
        readOnlyDbs.add( database.databaseName() );
        config.setDynamic( read_only_databases, readOnlyDbs, "full" );

        var writableDatabases = new HashSet<>( config.get( writable_databases ) );
        writableDatabases.remove( database.databaseName() );
        config.setDynamic( writable_databases, writableDatabases, "full" );
    }

    private void makeDbWritable()
    {
        Config config = database.getDependencyResolver().resolveDependency( Config.class );
        var writableDatabases = new HashSet<>( config.get( writable_databases ) );
        writableDatabases.add( database.databaseName() );
        config.setDynamic( writable_databases, writableDatabases, "full" );

        var readOnlyDbs = new HashSet<>( config.get( read_only_databases ) );
        readOnlyDbs.remove( database.databaseName() );
        config.setDynamic( read_only_databases, readOnlyDbs, "full" );
    }
}
