/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise;

import com.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import com.neo4j.server.security.enterprise.auth.EnterpriseSecurityModule;
import com.neo4j.server.security.enterprise.auth.EnterpriseUserManager;
import com.neo4j.server.security.enterprise.auth.FileRoleRepository;
import com.neo4j.server.security.enterprise.auth.RoleRepository;
import com.neo4j.server.security.enterprise.auth.SecureHasher;
import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import com.neo4j.server.security.enterprise.systemgraph.ContextSwitchingSystemGraphQueryExecutor;
import com.neo4j.server.security.enterprise.systemgraph.QueryExecutor;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphImportOptions;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphInitializer;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphOperations;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphRealm;

import java.io.File;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import org.neo4j.cypher.internal.javacompat.QueryResultProvider;
import org.neo4j.cypher.result.QueryResult;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.module.DatabaseInitializer;
import org.neo4j.graphdb.security.WriteOperationsNotAllowedException;
import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.security.SecurityModule;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.AccessCapability;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.server.security.auth.UserRepository;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

@Service.Implementation( SecurityModule.class )
public class CommercialSecurityModule extends EnterpriseSecurityModule
{
    public static final String IMPORT_AUTH_COMMAND_NAME = "import-auth";
    public static final String USER_IMPORT_FILENAME = ".users.import";
    public static final String ROLE_IMPORT_FILENAME = ".roles.import";

    private DatabaseManager databaseManager;
    private boolean initSystemGraphOnStart;
    private Config config;
    private LogProvider logProvider;
    private FileSystemAbstraction fileSystem;
    private AccessCapability accessCapability;

    @SuppressWarnings( "WeakerAccess" )
    public CommercialSecurityModule()
    {
        super( "commercial-security-module" );
    }

    @Override
    public void setup( Dependencies dependencies ) throws KernelException
    {
        // This will be need as an input to the SystemGraphRealm later to be able to handle transactions
        org.neo4j.kernel.impl.util.Dependencies platformDependencies = (org.neo4j.kernel.impl.util.Dependencies) dependencies.dependencySatisfier();
        this.databaseManager = platformDependencies.resolveDependency( DatabaseManager.class );

        this.config = dependencies.config();
        this.logProvider = dependencies.logService().getUserLogProvider();
        this.fileSystem = dependencies.fileSystem();
        this.accessCapability = dependencies.accessCapability();

        if ( config.get( EnterpriseEditionSettings.mode ) == EnterpriseEditionSettings.Mode.CORE ||
                config.get( EnterpriseEditionSettings.mode ) == EnterpriseEditionSettings.Mode.READ_REPLICA )
        {
            initSystemGraphOnStart = false;
        }
        else
        {
            initSystemGraphOnStart = true;
        }

        super.setup( dependencies );
    }

    @Override
    protected EnterpriseUserManager createInternalRealm( Config config, LogProvider logProvider, FileSystemAbstraction fileSystem, JobScheduler jobScheduler,
            SecurityLog securityLog, AccessCapability accessCapability )
    {
        EnterpriseUserManager internalRealm = null;
        if ( securityConfig.hasNativeProvider )
        {
            internalRealm = createInternalFlatFileRealm( config, logProvider, fileSystem, jobScheduler );
        }
        else if ( ( (CommercialSecurityConfig) securityConfig ).hasSystemGraphProvider )
        {
            internalRealm = createSystemGraphRealm( config, logProvider, fileSystem, securityLog, accessCapability );
        }
        return internalRealm;
    }

    public Optional<DatabaseInitializer> getDatabaseInitializer()
    {
        if ( !((CommercialSecurityConfig) securityConfig).hasSystemGraphProvider )
        {
            return Optional.empty();
        }

        return Optional.of( database ->
        {
            QueryExecutor queryExecutor = new QueryExecutor()
            {
                @Override
                public void executeQuery( String query, Map<String,Object> params, QueryResult.QueryResultVisitor resultVisitor )
                {
                    try ( Transaction tx = database.beginTx() )
                    {
                        Result result = database.execute( query, params );
                        QueryResult queryResult = ((QueryResultProvider) result).queryResult();
                        queryResult.accept( resultVisitor );
                        tx.success();
                    }
                }

                @Override
                public Transaction beginTx()
                {
                    return database.beginTx();
                }
            };

            SecureHasher secureHasher = new SecureHasher();
            SystemGraphOperations systemGraphOperations = new SystemGraphOperations( queryExecutor, secureHasher );
            SystemGraphImportOptions importOptions = configureImportOptions( config, logProvider, fileSystem, accessCapability );
            Log log = logProvider.getLog( getClass() );
            SystemGraphInitializer initializer = new SystemGraphInitializer( queryExecutor, systemGraphOperations, importOptions, secureHasher, log );

            try
            {
                initializer.initializeSystemGraph();
            }
            catch ( Throwable e )
            {
                throw new RuntimeException( e );
            }
        } );
    }

    private SystemGraphRealm createSystemGraphRealm( Config config, LogProvider logProvider, FileSystemAbstraction fileSystem, SecurityLog securityLog,
            AccessCapability accessCapability )
    {
        ContextSwitchingSystemGraphQueryExecutor queryExecutor = new ContextSwitchingSystemGraphQueryExecutor( databaseManager,
                config.get( GraphDatabaseSettings.active_database ) );

        SecureHasher secureHasher = new SecureHasher();
        SystemGraphOperations systemGraphOperations = new SystemGraphOperations( queryExecutor, secureHasher );

        SystemGraphInitializer systemGraphInitializer = initSystemGraphOnStart ? new SystemGraphInitializer( queryExecutor, systemGraphOperations,
                configureImportOptions( config, logProvider, fileSystem, accessCapability ), secureHasher, securityLog ) : null;

        return new SystemGraphRealm(
                systemGraphOperations,
                systemGraphInitializer,
                initSystemGraphOnStart,
                secureHasher,
                new BasicPasswordPolicy(),
                createAuthenticationStrategy( config ),
                config.get( SecuritySettings.native_authentication_enabled ),
                config.get( SecuritySettings.native_authorization_enabled )
        );
    }

    private static SystemGraphImportOptions configureImportOptions( Config config, LogProvider logProvider, FileSystemAbstraction fileSystem,
            AccessCapability accessCapability )
    {
        File parentFile = CommunitySecurityModule.getUserRepositoryFile( config ).getParentFile();
        File userImportFile = new File( parentFile, USER_IMPORT_FILENAME );
        File roleImportFile = new File( parentFile, ROLE_IMPORT_FILENAME );

        boolean shouldPerformImport = fileSystem.fileExists( userImportFile ) || fileSystem.fileExists( roleImportFile );
        boolean mayPerformMigration = !shouldPerformImport && mayPerformMigration( accessCapability );
        boolean shouldPurgeImportRepositoriesAfterSuccessfulImport = shouldPerformImport;
        boolean shouldResetSystemGraphAuthBeforeImport = false;

        Supplier<UserRepository> importUserRepositorySupplier = () -> new FileUserRepository( fileSystem, userImportFile, logProvider );
        Supplier<RoleRepository> importRoleRepositorySupplier = () -> new FileRoleRepository( fileSystem, roleImportFile, logProvider );
        Supplier<UserRepository> migrationUserRepositorySupplier = () -> CommunitySecurityModule.getUserRepository( config, logProvider, fileSystem );
        Supplier<RoleRepository> migrationRoleRepositorySupplier = () -> EnterpriseSecurityModule.getRoleRepository( config, logProvider, fileSystem );
        Supplier<UserRepository> initialUserRepositorySupplier = () -> CommunitySecurityModule.getInitialUserRepository( config, logProvider, fileSystem );
        Supplier<UserRepository> defaultAdminRepositorySupplier = () -> getDefaultAdminRepository( config, logProvider, fileSystem );

        return new SystemGraphImportOptions(
                shouldPerformImport,
                mayPerformMigration,
                shouldPurgeImportRepositoriesAfterSuccessfulImport,
                shouldResetSystemGraphAuthBeforeImport,
                importUserRepositorySupplier,
                importRoleRepositorySupplier,
                migrationUserRepositorySupplier,
                migrationRoleRepositorySupplier,
                initialUserRepositorySupplier,
                defaultAdminRepositorySupplier
        );
    }

    private static SystemGraphImportOptions configureImportOptionsForOfflineImport( UserRepository importUserRepository, RoleRepository importRoleRepository,
            boolean shouldResetSystemGraphAuthBeforeImport )
    {
        boolean shouldPerformImport = true;
        boolean mayPerformMigration = false;
        boolean shouldPurgeImportRepositoriesAfterSuccesfulImport = false;

        Supplier<UserRepository> importUserRepositorySupplier = () -> importUserRepository;
        Supplier<RoleRepository> importRoleRepositorySupplier = () -> importRoleRepository;

        return new SystemGraphImportOptions(
                shouldPerformImport,
                mayPerformMigration,
                shouldPurgeImportRepositoriesAfterSuccesfulImport,
                shouldResetSystemGraphAuthBeforeImport,
                importUserRepositorySupplier,
                importRoleRepositorySupplier,
                /* migrationUserRepositorySupplier = */ null,
                /* migrationRoleRepositorySupplier = */ null,
                /* initialUserRepositorySupplier = */ null,
                /* defaultAdminRepositorySupplier = */ null
        );
    }

    private static boolean mayPerformMigration( AccessCapability accessCapability )
    {
        boolean mayPerformMigration = false;

        // TBD: Should we protect this with a dedicated setting?
        // The argument against using GraphDatabaseSettings.allow_upgrade is that it will also force a potential store upgrade
        //if ( config.get( SecuritySettings.allow_migration ) )
        {
            try
            {
                // Only perform migration if this neo4j instance can write (In a cluster, only the leader can write)
                accessCapability.assertCanWrite();
                mayPerformMigration = true;
            }
            catch ( WriteOperationsNotAllowedException e )
            {
                // Do nothing
            }
        }
        return mayPerformMigration;
    }

    @Override
    protected SecurityConfig getValidatedSecurityConfig( Config config )
    {
        CommercialSecurityConfig securityConfig = new CommercialSecurityConfig( config );
        securityConfig.validate();
        return securityConfig;
    }

    static class CommercialSecurityConfig extends SecurityConfig
    {
        final boolean hasSystemGraphProvider;

        CommercialSecurityConfig( Config config )
        {
            super( config );
            hasSystemGraphProvider = authProviders.contains( SecuritySettings.SYSTEM_GRAPH_REALM_NAME );
        }

        @Override
        protected void validate()
        {
            if ( hasSystemGraphProvider && !nativeAuthentication && !nativeAuthorization )
            {
                throw illegalConfiguration(
                        "System graph auth provider configured, but both authentication and authorization are disabled." );
            }

            if ( hasNativeProvider && hasSystemGraphProvider )
            {
                throw illegalConfiguration(
                        "Both system graph auth provider and native auth provider configured," +
                        " but they cannot be used together. Please remove one of them from the configuration." );
            }
            super.validate();
        }
    }

    // This is used by ImportAuthCommand for offline import of auth information
    public static SystemGraphRealm createSystemGraphRealmForOfflineImport( Config config,
            SecurityLog securityLog,
            DatabaseManager databaseManager,
            UserRepository importUserRepository, RoleRepository importRoleRepository,
            boolean shouldResetSystemGraphAuthBeforeImport )
    {
        ContextSwitchingSystemGraphQueryExecutor queryExecutor = new ContextSwitchingSystemGraphQueryExecutor( databaseManager, SYSTEM_DATABASE_NAME );
        SecureHasher secureHasher = new SecureHasher();
        SystemGraphImportOptions importOptions =
                configureImportOptionsForOfflineImport( importUserRepository, importRoleRepository, shouldResetSystemGraphAuthBeforeImport );

        SystemGraphOperations systemGraphOperations = new SystemGraphOperations( queryExecutor, secureHasher );
        SystemGraphInitializer systemGraphInitializer =
                new SystemGraphInitializer( queryExecutor, systemGraphOperations, importOptions, secureHasher, securityLog );

        return new SystemGraphRealm(
                systemGraphOperations,
                systemGraphInitializer,
                true,
                new SecureHasher(),
                new BasicPasswordPolicy(),
                createAuthenticationStrategy( config ),
                false,
                true // At least one of these needs to be true for the realm to consider imports
        );
    }
}
