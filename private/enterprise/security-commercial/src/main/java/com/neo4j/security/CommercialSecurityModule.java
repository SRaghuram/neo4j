/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security;

import org.apache.shiro.realm.Realm;

import java.util.List;

import org.neo4j.dbms.database.DatabaseManager;
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
    // This will be need as an input to the NativeGraphRealm later to be able to handle transactions
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
            FileSystemAbstraction fileSystem, JobScheduler jobScheduler, List<Realm> realms )
    {
        EnterpriseUserManager internalRealm = null;
        if ( securityConfig.hasNativeProvider )
        {
            internalRealm = createInternalFlatFileRealm( config, logProvider, fileSystem, jobScheduler );
            realms.add( (Realm) internalRealm );
        }
        else if ( ( (CommercialSecurityConfig) securityConfig ).hasNativeGraphProvider )
        {
            internalRealm = createNativeGraphRealm( config, logProvider, fileSystem);
            realms.add( (Realm) internalRealm );
        }
        return internalRealm;
    }

    private NativeGraphRealm createNativeGraphRealm( Config config, LogProvider logProvider, FileSystemAbstraction fileSystem )
    {
        return new NativeGraphRealm(
                new BasicPasswordPolicy(),
                createAuthenticationStrategy( config ),
                config.get( SecuritySettings.native_authentication_enabled ),
                config.get( SecuritySettings.native_authorization_enabled ),
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
        final boolean hasNativeGraphProvider;

        CommercialSecurityConfig( Config config )
        {
            super( config );
            hasNativeGraphProvider = authProviders.contains( SecuritySettings.NATIVE_GRAPH_REALM_NAME );
        }

        @Override
        protected void validate()
        {
            if ( hasNativeGraphProvider && !nativeAuthentication && !nativeAuthorization )
            {
                throw illegalConfiguration(
                        "Native graph auth provider configured, but both authentication and authorization are disabled." );
            }

            if ( hasNativeProvider && hasNativeGraphProvider )
            {
                throw illegalConfiguration(
                        "Both native auth provider and native graph auth provider configured," +
                        " but they cannot be used together. Please remove one of them from the configuration." );
            }
            super.validate();
        }
    }
}
