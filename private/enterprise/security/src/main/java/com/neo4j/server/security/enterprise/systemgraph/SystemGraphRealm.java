/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.server.security.enterprise.auth.RealmLifecycle;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.auth.ShiroAuthorizationInfoProvider;
import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.subject.PrincipalCollection;

import java.util.Set;

import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.systemgraph.BasicSystemGraphRealm;
import org.neo4j.server.security.systemgraph.SecurityGraphInitializer;

/**
 * Shiro realm using a Neo4j graph to store users and roles
 */
public class SystemGraphRealm extends BasicSystemGraphRealm implements RealmLifecycle, ShiroAuthorizationInfoProvider
{
    private final boolean authorizationEnabled;
    private final SystemGraphOperations systemGraphOperations;

    public SystemGraphRealm( SystemGraphOperations systemGraphOperations, SecurityGraphInitializer systemGraphInitializer,
             AuthenticationStrategy authenticationStrategy, boolean authenticationEnabled,
            boolean authorizationEnabled )
    {
        super( systemGraphOperations, systemGraphInitializer, authenticationStrategy, authenticationEnabled );
        setName( SecuritySettings.NATIVE_REALM_NAME );
        this.authorizationEnabled = authorizationEnabled;
        this.systemGraphOperations = systemGraphOperations;

        setAuthorizationCachingEnabled( true );
    }

    @Override
    public void initialize()
    {
    }

    @Override
    protected AuthorizationInfo doGetAuthorizationInfo( PrincipalCollection principals )
    {
        if ( !authorizationEnabled )
        {
            return null;
        }

        String username = (String) getAvailablePrincipal( principals );
        if ( username == null )
        {
            return null;
        }

        return systemGraphOperations.doGetAuthorizationInfo( username );
    }

    @Override
    protected Object getAuthorizationCacheKey( PrincipalCollection principals )
    {
        return getAvailablePrincipal( principals );
    }

    @Override
    public AuthorizationInfo getAuthorizationInfoSnapshot( PrincipalCollection principalCollection )
    {
        return getAuthorizationInfo( principalCollection );
    }

    public Set<ResourcePrivilege> getPrivilegesForRoles( Set<String> roles )
    {
        return systemGraphOperations.getPrivilegeForRoles( roles );
    }

    public void clearCacheForRoles()
    {
        systemGraphOperations.clearCacheForRoles();
    }
}
