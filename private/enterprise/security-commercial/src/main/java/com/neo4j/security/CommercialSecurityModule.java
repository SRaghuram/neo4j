/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security;

import com.neo4j.security.configuration.CommercialSecuritySettings;

import java.io.File;
import java.util.function.Supplier;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.security.WriteOperationsNotAllowedException;
import org.neo4j.helpers.Service;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.security.SecurityModule;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.AccessCapability;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.enterprise.auth.EnterpriseSecurityModule;
import org.neo4j.server.security.enterprise.auth.EnterpriseUserManager;
import org.neo4j.server.security.enterprise.auth.FileRoleRepository;
import org.neo4j.server.security.enterprise.auth.RoleRepository;
import org.neo4j.server.security.enterprise.auth.SecureHasher;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.neo4j.server.security.enterprise.log.SecurityLog;

import static com.neo4j.commandline.admin.security.ImportAuthCommand.ROLE_IMPORT_FILENAME;
import static com.neo4j.commandline.admin.security.ImportAuthCommand.USER_IMPORT_FILENAME;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

@Service.Implementation( SecurityModule.class )
public class CommercialSecurityModule extends EnterpriseSecurityModule
{
    // This will be need as an input to the SystemGraphRealm later to be able to handle transactions
    private static DatabaseManager databaseManager;

    public CommercialSecurityModule()
    {
        super( "commercial-security-module" );
    }

    @Override
    public void setup( Dependencies dependencies ) throws KernelException
    {
        databaseManager = ( (org.neo4j.kernel.impl.util.Dependencies) dependencies.dependencySatisfier() ).resolveDependency( DatabaseManager.class );
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

    private SystemGraphRealm createSystemGraphRealm( Config config, LogProvider logProvider, FileSystemAbstraction fileSystem, SecurityLog securityLog,
            AccessCapability accessCapability )
    {
        return new SystemGraphRealm(
                new SystemGraphExecutor( databaseManager, config.get( GraphDatabaseSettings.active_database ) ),
                new SecureHasher(),
                new BasicPasswordPolicy(),
                createAuthenticationStrategy( config ),
                ( (CommercialSecurityConfig) securityConfig ).systemGraphAuthentication,
                ( (CommercialSecurityConfig) securityConfig ).systemGraphAuthentication,
                securityLog,
                configureImportOptions( config, logProvider, fileSystem, accessCapability )
        );
    }

    private static SystemGraphImportOptions configureImportOptions( Config config, LogProvider logProvider, FileSystemAbstraction fileSystem,
            AccessCapability accessCapability )
    {
        File parentFile = CommunitySecurityModule.getUserRepositoryFile( config ).getParentFile();
        File userImportFile = new File( parentFile, USER_IMPORT_FILENAME );
        File roleImportFile = new File( parentFile, ROLE_IMPORT_FILENAME );

        boolean shouldPerformImport = fileSystem.fileExists( userImportFile ) || fileSystem.fileExists( roleImportFile );
        boolean mayPerformMigration = !shouldPerformImport && mayPerformMigration( config, accessCapability );
        boolean shouldPurgeImportRepositoriesAfterSuccesfulImport = shouldPerformImport;
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
                shouldPurgeImportRepositoriesAfterSuccesfulImport,
                shouldResetSystemGraphAuthBeforeImport,
                importUserRepositorySupplier,
                importRoleRepositorySupplier,
                migrationUserRepositorySupplier,
                migrationRoleRepositorySupplier,
                initialUserRepositorySupplier,
                defaultAdminRepositorySupplier
        );
    }

    private static SystemGraphImportOptions configureImportOptionsForOfflineImport( Config config, LogProvider logProvider,
            UserRepository importUserRepository, RoleRepository importRoleRepository, boolean shouldResetSystemGraphAuthBeforeImport )
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

    private static boolean mayPerformMigration( Config config, AccessCapability accessCapability )
    {
        boolean mayPerformMigration = false;

        // TODO: Should we use a new dedicated setting for this?
        if ( config.get( GraphDatabaseSettings.allow_upgrade ) )
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
        final boolean systemGraphAuthentication;
        final boolean systemGraphAuthorization;

        CommercialSecurityConfig( Config config )
        {
            super( config );
            hasSystemGraphProvider = authProviders.contains( SecuritySettings.SYSTEM_GRAPH_REALM_NAME );
            systemGraphAuthentication = config.get( CommercialSecuritySettings.system_graph_authentication_enabled );
            systemGraphAuthorization = config.get( CommercialSecuritySettings.system_graph_authorization_enabled );
            internal_security_enabled = internal_security_enabled || systemGraphAuthentication || systemGraphAuthorization;
        }

        @Override
        protected void validate()
        {
            if ( !nativeAuthentication && !systemGraphAuthentication && !ldapAuthentication && !pluginAuthentication )
            {
                throw illegalConfiguration( "All authentication providers are disabled." );
            }

            if ( !nativeAuthorization && !systemGraphAuthorization && !ldapAuthorization && !pluginAuthorization )
            {
                throw illegalConfiguration( "All authorization providers are disabled." );
            }

            if ( hasNativeProvider && !nativeAuthentication && !nativeAuthorization )
            {
                throw illegalConfiguration(
                        "Native auth provider configured, but both authentication and authorization are disabled." );
            }

            if ( hasSystemGraphProvider && !systemGraphAuthentication && !systemGraphAuthorization )
            {
                throw illegalConfiguration(
                        "System graph auth provider configured, but both authentication and authorization are disabled." );
            }

            if ( hasLdapProvider && !ldapAuthentication && !ldapAuthorization )
            {
                throw illegalConfiguration(
                        "LDAP auth provider configured, but both authentication and authorization are disabled." );
            }

            if ( !pluginAuthProviders.isEmpty() && !pluginAuthentication && !pluginAuthorization )
            {
                throw illegalConfiguration(
                        "Plugin auth provider configured, but both authentication and authorization are disabled." );
            }
            if ( propertyAuthorization && !parsePropertyPermissions() )
            {
                throw illegalConfiguration(
                        "Property level authorization is enabled but there is a error in the permissions mapping." );
            }

            if ( hasNativeProvider && hasSystemGraphProvider )
            {
                throw illegalConfiguration(
                        "Both system graph auth provider and native auth provider configured," +
                        " but they cannot be used together. Please remove one of them from the configuration." );
            }
        }

        @Override
        protected boolean onlyPluginAuthentication()
        {
            return !nativeAuthentication && !systemGraphAuthentication && !ldapAuthentication && pluginAuthentication;
        }

        @Override
        protected boolean onlyPluginAuthorization()
        {
            return !nativeAuthorization && !systemGraphAuthorization && !ldapAuthorization && pluginAuthorization;
        }
    }

    // This is used by ImportAuthCommand for offline import of auth information
    public static SystemGraphRealm createSystemGraphRealmForOfflineImport( Config config, LogProvider logProvider,
            SecurityLog securityLog,
            DatabaseManager databaseManager,
            UserRepository importUserRepository, RoleRepository importRoleRepository,
            boolean shouldResetSystemGraphAuthBeforeImport )
    {
        return new SystemGraphRealm(
                new SystemGraphExecutor( databaseManager, SYSTEM_DATABASE_NAME ),
                new SecureHasher(),
                new BasicPasswordPolicy(),
                createAuthenticationStrategy( config ),
                false,
                false,
                securityLog,
                configureImportOptionsForOfflineImport( config, logProvider,
                        importUserRepository, importRoleRepository,
                        shouldResetSystemGraphAuthBeforeImport )
        );
    }
}
