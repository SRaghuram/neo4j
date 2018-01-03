/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.enterprise.jmx;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.NeoServer;
import org.neo4j.server.enterprise.CommercialNeoServer;
import org.neo4j.server.enterprise.helpers.CommercialServerBuilder;
import org.neo4j.test.rule.CleanupRule;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ServerManagementIT
{

    private final CleanupRule cleanup = new CleanupRule();
    private final TestDirectory baseDir = TestDirectory.testDirectory();
    private final SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( suppressOutput ).around( baseDir ).around( cleanup );

    @Test
    public void shouldBeAbleToRestartServer() throws Exception
    {
        // Given
        String dataDirectory1 = baseDir.directory( "data1" ).getAbsolutePath();
        String dataDirectory2 = baseDir.directory( "data2" ).getAbsolutePath();

        Config config = Config.fromFile( CommercialServerBuilder
                    .serverOnRandomPorts()
                    .withDefaultDatabaseTuning()
                    .usingDataDir( dataDirectory1 )
                    .createConfigFiles() )
                .withHome( baseDir.directory() )
                .withSetting( GraphDatabaseSettings.logs_directory, baseDir.directory( "logs" ).getPath() )
                .build();

        // When
        NeoServer server = cleanup.add( new CommercialNeoServer( config, graphDbDependencies(), NullLogProvider
                .getInstance() ) );
        server.start();

        assertNotNull( server.getDatabase().getGraph() );
        assertEquals( config.get( DatabaseManagementSystemSettings.database_path ).getAbsolutePath(),
                server.getDatabase().getLocation().getAbsolutePath() );

        // Change the database location
        config.augment( DatabaseManagementSystemSettings.data_directory, dataDirectory2 );
        ServerManagement bean = new ServerManagement( server );
        bean.restartServer();

        // Then
        assertNotNull( server.getDatabase().getGraph() );
        assertEquals( config.get( DatabaseManagementSystemSettings.database_path ).getAbsolutePath(),
                server.getDatabase().getLocation().getAbsolutePath() );
    }

    private static GraphDatabaseDependencies graphDbDependencies()
    {
        return GraphDatabaseDependencies.newDependencies().userLogProvider( NullLogProvider.getInstance() );
    }
}
