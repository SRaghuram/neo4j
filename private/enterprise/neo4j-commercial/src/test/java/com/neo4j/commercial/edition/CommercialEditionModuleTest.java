/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commercial.edition;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseExistsException;
import org.neo4j.dbms.database.DatabaseManagementServiceImpl;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.PlaceholderDatabaseIdRepository;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SkipThreadLeakageGuard;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.neo4j.graphdb.facade.GraphDatabaseDependencies.newDependencies;
import static org.neo4j.kernel.impl.factory.DatabaseInfo.COMMERCIAL;

@SkipThreadLeakageGuard
@ExtendWith( TestDirectoryExtension.class )
class CommercialEditionModuleTest
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void editionDatabaseCreationOrder() throws DatabaseExistsException
    {
        DatabaseManager<?> manager = mock( DatabaseManager.class );
        Config config = Config.defaults();
        DatabaseIdRepository databaseIdRepository = new PlaceholderDatabaseIdRepository( config );
        GlobalModule globalModule = new GlobalModule( testDirectory.storeDir(), config, COMMERCIAL, newDependencies() )
        {
            @Override
            protected LogService createLogService( LogProvider userLogProvider )
            {
                return NullLogService.getInstance();
            }
        };
        globalModule.getGlobalDependencies().satisfyDependency( mock( DatabaseManagementServiceImpl.class ) );
        CommercialEditionModule editionModule = new CommercialEditionModule( globalModule );
        editionModule.createDatabases( manager, config );

        InOrder order = inOrder( manager );
        order.verify( manager ).createDatabase( eq( databaseIdRepository.systemDatabase() ) );
        order.verify( manager ).createDatabase( eq( databaseIdRepository.defaultDatabase() ) );
    }
}
