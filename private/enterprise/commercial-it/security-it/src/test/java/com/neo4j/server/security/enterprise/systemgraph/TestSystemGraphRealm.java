/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.server.security.enterprise.auth.InMemoryRoleRepository;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphRealmTestHelper.TestDatabaseManager;

import java.util.function.Supplier;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.systemgraph.ContextSwitchingSystemGraphQueryExecutor;
import org.neo4j.server.security.systemgraph.QueryExecutor;
import org.neo4j.test.rule.TestDirectory;

import static org.mockito.Mockito.mock;

public class TestSystemGraphRealm extends TestBasicSystemGraphRealm
{
    static SystemGraphRealm testRealm( TestDatabaseManager dbManager, TestDirectory testDirectory, SecurityLog securityLog ) throws Throwable
    {
        Config config = Config.defaults();
        config.augment(  DatabaseManagementSystemSettings.auth_store_directory, testDirectory.directory( "data/dbms" ).toString()  );
        LogProvider logProvider = mock(LogProvider.class);
        FileSystemAbstraction fileSystem = testDirectory.getFileSystem();

        Supplier<UserRepository> migrationUserRepositorySupplier = () -> CommunitySecurityModule.getUserRepository( config, logProvider, fileSystem );
        Supplier<UserRepository> initialUserRepositorySupplier = () -> CommunitySecurityModule.getInitialUserRepository( config, logProvider, fileSystem );
        SystemGraphImportOptions importOptions = new SystemGraphImportOptions(
                false,
                false,
                false,
                false,
                InMemoryUserRepository::new,
                InMemoryRoleRepository::new,
                migrationUserRepositorySupplier,
                InMemoryRoleRepository::new,
                initialUserRepositorySupplier,
                InMemoryUserRepository::new
                );

        return testRealm( importOptions, securityLog, dbManager, config );
    }

    static SystemGraphRealm testRealm( SystemGraphImportOptions importOptions, SecurityLog securityLog, TestDatabaseManager dbManager, Config config )
            throws Throwable
    {
        ContextSwitchingSystemGraphQueryExecutor executor = new ContextSwitchingSystemGraphQueryExecutor( dbManager, new TestThreadToStatementContextBridge() );
        return testRealm( importOptions, securityLog, dbManager.getManagementService(), executor, config );
    }

    public static SystemGraphRealm testRealm( SystemGraphImportOptions importOptions, SecurityLog securityLog, DatabaseManagementService managementService,
            QueryExecutor executor, Config config ) throws Throwable
    {
        unregisterListeners( managementService, config );

        SystemGraphOperations systemGraphOperations = new SystemGraphOperations( executor, secureHasher );
        SystemGraphInitializer systemGraphInitializer =
                new SystemGraphInitializer( executor, systemGraphOperations, importOptions, secureHasher, securityLog, config );

        SystemGraphRealm realm = new SystemGraphRealm( systemGraphOperations, systemGraphInitializer, true, secureHasher, new BasicPasswordPolicy(),
                newRateLimitedAuthStrategy(), true, true );

        realm.initialize();
        realm.start();

        registerListeners();

        return realm;
    }
}
