/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.bolt;

import com.neo4j.test.TestCommercialDatabaseManagementServiceBuilder;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.Settings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.StatementResult;
import org.neo4j.driver.Value;
import org.neo4j.driver.util.Pair;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@ExtendWith( TestDirectoryExtension.class )
class BoltSnapshotQueryExecutionIT
{
    @Inject
    private TestDirectory testDirectory;

    private Driver driver;
    private GraphDatabaseService db;
    private DatabaseManagementService managementService;

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
    void executeQueryWithSnapshotEngine()
    {
        executeQuery( "withSnapshotEngine", Settings.TRUE );
    }

    @Test
    void executeQueryWithoutSnapshotEngine()
    {
        executeQuery( "withoutSnapshotEngine", Settings.FALSE );
    }

    private void executeQuery( String directory, String useSnapshotEngineSettingValue )
    {
        managementService =
                new TestCommercialDatabaseManagementServiceBuilder( testDirectory.directory( directory ) )
                .setConfig( new BoltConnector( "bolt" ).type, "BOLT" )
                .setConfig( new BoltConnector( "bolt" ).enabled, "true" )
                .setConfig( new BoltConnector( "bolt" ).listen_address, "localhost:0" )
                .setConfig( GraphDatabaseSettings.snapshot_query, useSnapshotEngineSettingValue ).build();
        db = managementService.database( DEFAULT_DATABASE_NAME );
        initDatabase();
        connectDirver();
        verifyQueryExecution();
    }

    private void verifyQueryExecution()
    {
        try ( Session session = driver.session() )
        {
            session.readTransaction( tx ->
            {
                StatementResult statementResult = tx.run( "MATCH (n) RETURN n.name, n.profession, n.planet, n.city ORDER BY n.name" );
                List<String> fields = Arrays.asList( "n.name", "n.profession", "n.planet", "n.city" );
                Record amy = statementResult.next();
                assertEquals( amy.keys(), fields );
                assertPairs( amy.fields(), "n.name", "Amy",
                                   "n.profession", "Student",
                                   "n.planet", "Mars",
                                   "n.city", "null");
                Record fry = statementResult.next();
                assertEquals( fry.keys(), fields );
                assertPairs( fry.fields(), "n.name", "Fry",
                        "n.profession", "Delivery Boy",
                        "n.planet", "Earth",
                        "n.city", "New York");
                Record lila = statementResult.next();
                assertEquals( lila.keys(), fields );
                assertPairs( lila.fields(), "n.name", "Lila",
                        "n.profession", "Pilot",
                        "n.planet", "Earth",
                        "n.city", "New York");
                assertFalse( statementResult.hasNext() );
                return null;
            } );
        }
    }

    private void connectDirver()
    {
        driver = GraphDatabase.driver( boltURI(), Config.build().withoutEncryption().toConfig() );
    }

    private void initDatabase()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            Node fry = db.createNode();
            fry.setProperty( "name", "Fry" );
            fry.setProperty( "profession", "Delivery Boy" );
            fry.setProperty( "planet", "Earth" );
            fry.setProperty( "city", "New York" );
            Node lila = db.createNode();
            lila.setProperty( "name", "Lila" );
            lila.setProperty( "profession", "Pilot" );
            lila.setProperty( "planet", "Earth" );
            lila.setProperty( "city", "New York" );
            Node amy = db.createNode();
            amy.setProperty( "name", "Amy" );
            amy.setProperty( "profession", "Student" );
            amy.setProperty( "planet", "Mars" );
            transaction.success();
        }
    }

    private static void assertPairs( List<Pair<String,Value>> pairs, String key1, String value1, String key2, String value2, String key3, String value3,
            String key4, String value4 )
    {
        assertThat( pairs, Matchers.hasSize( 4 ) );
        validatePair( pairs.get( 0 ), key1, value1 );
        validatePair( pairs.get( 1 ), key2, value2 );
        validatePair( pairs.get( 2 ), key3, value3 );
        validatePair( pairs.get( 3 ), key4, value4 );
    }

    private static void validatePair( Pair<String,Value> pair, String key, String value )
    {
        assertEquals( key, pair.key() );
        assertEquals( value, pair.value().asString() );
    }

    private URI boltURI()
    {
        ConnectorPortRegister connectorPortRegister = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( ConnectorPortRegister.class );
        HostnamePort boltHostNamePort = connectorPortRegister.getLocalAddress( "bolt" );
        return URI.create( "bolt://" + boltHostNamePort.getHost() + ":" + boltHostNamePort.getPort() );
    }

}
