/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import com.neo4j.kernel.impl.enterprise.configuration.CommercialEditionSettings.Mode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.kernel.impl.enterprise.configuration.CommercialEditionSettings.mode;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@ExtendWith( TestDirectoryExtension.class )
class CommercialNeoServerTest
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void checkExpectedDatabaseDirectory()
    {
        Config config = Config.newBuilder().set( GraphDatabaseSettings.SERVER_DEFAULTS )
                .set( mode, Mode.SINGLE.name() )
                .set( GraphDatabaseSettings.neo4j_home, testDirectory.storeDir().getAbsolutePath() )
                .set( BoltConnector.group( "bolt" ).listen_address, "localhost:0" )
                .set( BoltConnector.group( "http" ).listen_address, "localhost:0" )
                .set( BoltConnector.group( "https" ).listen_address, "localhost:0" )
                .build();
        GraphDatabaseDependencies dependencies = GraphDatabaseDependencies.newDependencies().userLogProvider( NullLogProvider.getInstance() );
        CommercialNeoServer server = new CommercialNeoServer( config, dependencies );

        server.start();
        try
        {
            Path expectedPath = Paths.get( testDirectory.storeDir().getPath(), "data", "databases", DEFAULT_DATABASE_NAME );
            GraphDatabaseFacade graph = server.getDatabaseService().getDatabase();
            assertEquals( expectedPath, graph.databaseLayout().databaseDirectory().toPath() );
        }
        finally
        {
            server.stop();
        }
    }
}
