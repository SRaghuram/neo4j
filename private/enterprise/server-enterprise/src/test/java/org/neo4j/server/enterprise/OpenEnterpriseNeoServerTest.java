/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.enterprise;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings.Mode;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings.mode;

@ExtendWith( TestDirectoryExtension.class )
class OpenEnterpriseNeoServerTest
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void checkExpectedDatabaseDirectory()
    {
        Config config = Config.builder().withServerDefaults().withSetting( mode, Mode.SINGLE.name() )
                .withSetting( GraphDatabaseSettings.neo4j_home, testDirectory.storeDir().getAbsolutePath() )
                .withSetting( new BoltConnector( "bolt" ).listen_address.name(), "localhost:0" )
                .withSetting( new BoltConnector( "http" ).listen_address.name(), "localhost:0" )
                .withSetting( new BoltConnector( "https" ).listen_address.name(), "localhost:0" )
                .withSetting( GraphDatabaseSettings.auth_enabled, "false" )
                .build();
        GraphDatabaseDependencies dependencies = GraphDatabaseDependencies.newDependencies().userLogProvider( NullLogProvider.getInstance() );
        OpenEnterpriseNeoServer server = new OpenEnterpriseNeoServer( config, dependencies );

        server.start();
        try
        {
            Path expectedPath = Paths.get( testDirectory.storeDir().getPath(), "data", "databases", "graph.db" );
            GraphDatabaseFacade graph = server.getDatabase().getGraph();
            assertEquals( expectedPath, graph.databaseLayout().databaseDirectory().toPath() );
        }
        finally
        {
            server.stop();
        }
    }
}
