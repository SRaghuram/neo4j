/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security;

import com.neo4j.security.configuration.CommercialSecuritySettings;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Service;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.security.SecurityModule;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.enterprise.auth.EnterpriseSecurityModule;
import org.neo4j.server.security.enterprise.auth.EnterpriseUserManager;
import org.neo4j.server.security.enterprise.auth.SecureHasher;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;

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
    protected EnterpriseUserManager createInternalRealm( Config config, LogProvider logProvider,
            FileSystemAbstraction fileSystem, JobScheduler jobScheduler )
    {
        EnterpriseUserManager internalRealm = null;
        if ( securityConfig.hasNativeProvider )
        {
            internalRealm = createInternalFlatFileRealm( config, logProvider, fileSystem, jobScheduler );
        }
        else if ( ( (CommercialSecurityConfig) securityConfig ).hasSystemGraphProvider )
        {
            internalRealm = createSystemGraphRealm( config, logProvider, fileSystem);
        }
        return internalRealm;
    }

    private SystemGraphRealm createSystemGraphRealm( Config config, LogProvider logProvider, FileSystemAbstraction fileSystem )
    {
        return new SystemGraphRealm(
                new SystemGraphExecutor( databaseManager, config.get( GraphDatabaseSettings.active_database ) ),
                new SecureHasher(),
                new BasicPasswordPolicy(),
                createAuthenticationStrategy( config ),
                ( (CommercialSecurityConfig) securityConfig ).systemGraphAuthentication,
                ( (CommercialSecurityConfig) securityConfig ).systemGraphAuthentication,
                CommunitySecurityModule.getInitialUserRepository( config, logProvider, fileSystem ),
                getDefaultAdminRepository( config, logProvider, fileSystem )
        );
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
}
