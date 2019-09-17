/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.dbms.EnterpriseSystemGraphInitializer;
import com.neo4j.server.security.enterprise.auth.InMemoryRoleRepository;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphRealmTestHelper.TestDatabaseManager;

import java.util.function.Supplier;

import org.neo4j.configuration.Config;
import org.neo4j.cypher.security.TestBasicSystemGraphRealm;
import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
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
        Config config = Config.defaults( DatabaseManagementSystemSettings.auth_store_directory, testDirectory.directory( "data/dbms" ).toPath() );
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
        var threadToStatementContextBridge = dbManager.testSystemDb.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );
        var executor = new ContextSwitchingSystemGraphQueryExecutor( dbManager, threadToStatementContextBridge );
        return testRealm( importOptions, securityLog, dbManager, executor, config );
    }

    public static SystemGraphRealm testRealm( SystemGraphImportOptions importOptions, SecurityLog securityLog, DatabaseManager dbManager,
            QueryExecutor executor, Config config ) throws Throwable
    {

        SystemGraphOperations systemGraphOperations = new SystemGraphOperations( executor, secureHasher );
        EnterpriseSystemGraphInitializer systemGraphInitializer = new EnterpriseSystemGraphInitializer( dbManager, config );
        EnterpriseSecurityGraphInitializer securityGraphInitializer =
                new EnterpriseSecurityGraphInitializer( systemGraphInitializer, executor, securityLog, systemGraphOperations, importOptions, secureHasher );

        SystemGraphRealm realm = new SystemGraphRealm( systemGraphOperations, securityGraphInitializer, secureHasher, new BasicPasswordPolicy(),
                newRateLimitedAuthStrategy(), true, true );

        realm.initialize();
        realm.start();

        return realm;
    }
}
