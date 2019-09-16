/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.enterprise;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.IpFamily;
import com.neo4j.causalclustering.discovery.akka.AkkaDiscoveryServiceFactory;
import com.neo4j.causalclustering.read_replica.ReadReplica;
import com.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import com.neo4j.server.security.enterprise.auth.EnterpriseAuthAndUserManager;
import com.neo4j.server.security.enterprise.auth.EnterpriseUserManager;
import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.string.UTF8;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ADMIN;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ARCHITECT;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.EDITOR;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLISHER;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.READER;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.function.Predicates.await;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
class SystemGraphSecurityReplicationIT
{
    private static final int DEFAULT_TIMEOUT_MS = 20_000;

    @Inject
    private TestDirectory testDir;

    private Cluster cluster;

    @BeforeEach
    void setup() throws Exception
    {
        Map<String,String> params = stringMap(
                GraphDatabaseSettings.auth_enabled.name(), TRUE,
                SecuritySettings.authentication_providers.name(), SecuritySettings.NATIVE_REALM_NAME,
                SecuritySettings.authorization_providers.name(), SecuritySettings.NATIVE_REALM_NAME
        );

        int noOfCoreMembers = 3;
        int noOfReadReplicas = 3;

        DiscoveryServiceFactory discoveryServiceFactory = new AkkaDiscoveryServiceFactory();
        cluster = new Cluster( testDir.absolutePath(), noOfCoreMembers, noOfReadReplicas, discoveryServiceFactory, params,
                emptyMap(), params, emptyMap(), Standard.LATEST_NAME, IpFamily.IPV4, false );

        cluster.start();
    }

    @AfterEach
    void cleanup()
    {
        if ( cluster != null )
        {
            cluster.shutdown();
        }
    }

    @Test
    void shouldHaveInitialRoles() throws Exception
    {
        Set<String> expectedRoles = new HashSet<>( asList( ADMIN, ARCHITECT, EDITOR, PUBLISHER, READER ) );

        for ( CoreClusterMember core : cluster.coreMembers() )
        {
            await( () -> getAllRoleNames( core.defaultDatabase() ).equals( expectedRoles ), DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );
        }

        for ( ReadReplica replica : cluster.readReplicas() )
        {
            await( () -> getAllRoleNames( replica.defaultDatabase() ).equals( expectedRoles ), DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );
        }
    }

    @Test
    void shouldReplicateNewUser() throws Exception
    {
        String username = "martin";
        String password = "235711";

        cluster.systemTx( ( db, tx ) ->
        {
            newUser( db, username, password );
            tx.commit();
        } );

        for ( CoreClusterMember core : cluster.coreMembers() )
        {
            await( () -> userCanLogin( username, password, core.defaultDatabase() ), DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );
        }

        for ( ReadReplica replica : cluster.readReplicas() )
        {
            await( () -> userCanLogin( username, password, replica.defaultDatabase() ), DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );
        }
    }

    private void newUser( GraphDatabaseFacade db, String username, String password )
    {
        try
        {
            userManager( db ).newUser( username, UTF8.encode( password ), false );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    private Set<String> getAllRoleNames( GraphDatabaseFacade db )
    {
        try
        {
            return userManager( db ).getAllRoleNames();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    private boolean userCanLogin( String username, String password, GraphDatabaseFacade db )
    {
        EnterpriseAuthManager authManager = authManager( db );
        AuthSubject subject = login( authManager, username, password );
        return AuthenticationResult.SUCCESS == subject.getAuthenticationResult();
    }

    private AuthSubject login( EnterpriseAuthManager authManager, String username, String password )
    {
        EnterpriseLoginContext loginContext;
        try
        {
            loginContext = authManager.login( authToken( username, password ) );
        }
        catch ( InvalidAuthTokenException e )
        {
            throw new RuntimeException( e );
        }
        return loginContext.subject();
    }

    private EnterpriseUserManager userManager( GraphDatabaseFacade db )
    {
        EnterpriseAuthManager enterpriseAuthManager = authManager( db );
        if ( enterpriseAuthManager instanceof EnterpriseAuthAndUserManager )
        {
            return ((EnterpriseAuthAndUserManager) enterpriseAuthManager).getUserManager();
        }
        throw new RuntimeException( "The configuration used does not have a user manager" );
    }

    private EnterpriseAuthManager authManager( GraphDatabaseFacade db )
    {
        return db.getDependencyResolver().resolveDependency( EnterpriseAuthManager.class );
    }
}
