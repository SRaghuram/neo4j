/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.discovery.SslSharedDiscoveryServiceFactory;
import com.neo4j.security.configuration.CommercialSecuritySettings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.neo4j.graphdb.facade.GraphDatabaseDependencies.newDependencies;
import static org.neo4j.kernel.impl.factory.DatabaseInfo.READ_REPLICA;

@ExtendWith( TestDirectoryExtension.class )
class CommercialCoreEditionModuleTest
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void editionDatabaseCreationOrder()
    {
        DatabaseManager manager = mock( DatabaseManager.class );
        Config config = Config.defaults( new BoltConnector( "bolt" ).enabled, Settings.TRUE );
        config.augment( CommercialSecuritySettings.system_graph_authorization_enabled, Settings.TRUE );
        PlatformModule platformModule = new PlatformModule( testDirectory.storeDir(), config, READ_REPLICA, newDependencies() );
        CommercialCoreEditionModule editionModule = new CommercialCoreEditionModule( platformModule, new SslSharedDiscoveryServiceFactory() );
        editionModule.createDatabases( manager, config );

        InOrder order = inOrder( manager );
        order.verify( manager ).createDatabase( eq( "system.db" ) );
        order.verify( manager ).createDatabase( eq( "graph.db" ) );
    }
}
