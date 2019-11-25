/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt;

import com.neo4j.kernel.impl.pagecache.PageCacheWarmerExtensionFactory;
import com.neo4j.metrics.global.GlobalMetricsExtensionFactory;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.snapshot.TestTransactionVersionContextSupplier;
import org.neo4j.snapshot.TestVersionContext;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.bolt.BoltDriverHelper.graphDatabaseDriver;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@TestDirectoryExtension
class BoltSnapshotQueryExecutionIT
{
    @Inject
    private TestDirectory testDirectory;

    private TestTransactionVersionContextSupplier testContextSupplier;
    private TestVersionContext testCursorContext;
    private Driver driver;
    private GraphDatabaseService db;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp()
    {
        testContextSupplier = new TestTransactionVersionContextSupplier();
        var dependencies = new Dependencies();
        dependencies.satisfyDependencies( testContextSupplier );
        managementService = new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homeDir() )
                .setExternalDependencies( dependencies )
                .setConfig( BoltConnector.enabled, true )
                .setConfig( BoltConnector.listen_address, new SocketAddress( "localhost", 0  ) )
                .setConfig( GraphDatabaseSettings.snapshot_query, true )
                //  The global metrics extension and page cache warmer issue queries that can make our version contexts dirty.
                // If we don't remove these extensions, we might geb a count of 0 or more than 1 for `testCursorContext.getAdditionalAttempts()`,
                // depending on when the extension marks it as dirty
                .removeExtensions( extension -> extension instanceof GlobalMetricsExtensionFactory ||
                                                extension instanceof PageCacheWarmerExtensionFactory )
                .build();
        prepareCursorContext();
        db = managementService.database( DEFAULT_DATABASE_NAME );
        createData( db );
        connectDriver();
    }

    @AfterEach
    void tearDown()
    {
        if ( db != null )
        {
            managementService.shutdown();
        }
        IOUtils.closeAllSilently( driver );
    }

    @Test
    void executeQueryWithSingleRetry()
    {
        try ( Session session = driver.session() )
        {
            session.readTransaction( tx ->
            {
                Result result =  tx.run( "MATCH (n) RETURN n.c" );
                while ( result.hasNext() )
                {
                    assertEquals( "d", result.next().get( "n.c" ).asString() );
                }

                return null;
            } );

            assertEquals( 1, testCursorContext.getAdditionalAttempts() );
        }
    }

    @Test
    void queryThatModifiesDataAndSeesUnstableSnapshotShouldThrowException()
    {
        // We need to stay dirty because the driver will re-attempt the query with a TransientError and otherwise it will work the 2nd time.
        testCursorContext.stayDirty( true );
        TransientException e = assertThrows( TransientException.class, () ->
        {
            try ( Session session = driver.session() )
            {
                session.readTransaction( tx -> tx.run( "MATCH (n:toRetry) CREATE () RETURN n.c" ) );
            }
        } );
        assertEquals( "Unable to get clean data snapshot for query " +
                      "'MATCH (n:toRetry) CREATE () RETURN n.c' that performs updates.", e.getMessage() );
    }

    private void connectDriver()
    {
        driver = graphDatabaseDriver( boltURI() );
    }

    private URI boltURI()
    {
        ConnectorPortRegister connectorPortRegister = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( ConnectorPortRegister.class );
        HostnamePort boltHostNamePort = connectorPortRegister.getLocalAddress( "bolt" );
        return URI.create( "bolt://" + boltHostNamePort.getHost() + ":" + boltHostNamePort.getPort() );
    }

    private void prepareCursorContext()
    {
        testCursorContext = TestVersionContext.testCursorContext( managementService, DEFAULT_DATABASE_NAME );
        testContextSupplier.setCursorContext( testCursorContext );
    }

    private static void createData( GraphDatabaseService database )
    {
        Label label = Label.label( "toRetry" );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = transaction.createNode( label );
            node.setProperty( "c", "d" );
            transaction.commit();
        }
    }
}
