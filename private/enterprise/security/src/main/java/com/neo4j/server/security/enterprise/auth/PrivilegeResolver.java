/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.server.security.enterprise.auth.Resource.DatabaseResource;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphRealm;
import org.apache.shiro.cache.CacheManager;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.internal.kernel.api.security.ProcedureSegment;
import org.neo4j.internal.kernel.api.security.Segment;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

import static com.neo4j.server.security.enterprise.auth.ResourcePrivilege.GrantOrDeny.GRANT;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ACCESS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.EXECUTE;

public class PrivilegeResolver
{
    private final SystemGraphRealm systemGraphRealm;
    private final String upgradeUsername;
    private final Boolean restrictUpgrade;

    private final Map<String, Set<ResourcePrivilege>> roleToPrivilege = new HashMap<>();

    private ResourcePrivilege accessOnSystem;
    private ResourcePrivilege executeUpgrade;

    public PrivilegeResolver( SystemGraphRealm systemGraphRealm, Config config )
    {
        this.systemGraphRealm = systemGraphRealm;
        this.upgradeUsername = config.get( GraphDatabaseInternalSettings.upgrade_username );
        this.restrictUpgrade = config.get( GraphDatabaseInternalSettings.restrict_upgrade );

        try
        {
            // ACCESS ON DATABASE system
            accessOnSystem = new ResourcePrivilege( GRANT, ACCESS, new DatabaseResource(), Segment.ALL, SYSTEM_DATABASE_NAME );
            // EXECUTE dbms.upgrade* ON DBMS
            ProcedureSegment segment = new ProcedureSegment( "dbms.upgrade*" );
            executeUpgrade = new ResourcePrivilege( GRANT, EXECUTE, new DatabaseResource(), segment, SYSTEM_DATABASE_NAME );
        }
        catch ( InvalidArgumentsException e )
        {
            throw new IllegalStateException( "Statically created privilege not accepted...", e );
        }

    }

    HashSet<ResourcePrivilege> getPrivileges( Set<String> roles, String username )
    {
        HashSet<ResourcePrivilege> privileges = getPrivilegesForRoles( roles );
        privileges.addAll( getUserPrivileges( username ) );
        return privileges;
    }

    private HashSet<ResourcePrivilege> getPrivilegesForRoles( Set<String> roles )
    {
        HashSet<ResourcePrivilege> privileges = systemGraphRealm.getPrivilegesForRoles( roles );
        for ( String role : roles )
        {
            privileges.addAll( roleToPrivilege.getOrDefault( role, Collections.emptySet() ) );
        }
        return privileges;
    }

    private Set<ResourcePrivilege> getUserPrivileges( String username )
    {
        if ( restrictUpgrade && upgradeUsername.equals( username ) )
        {
            HashSet<ResourcePrivilege> privileges = new HashSet<>();
            privileges.add( accessOnSystem );
            privileges.add( executeUpgrade );
            return privileges;
        }
        return Collections.emptySet();
    }

    void initAndSetCacheManager( CacheManager cacheManager )
    {
        systemGraphRealm.init();
        systemGraphRealm.setCacheManager( cacheManager );
        systemGraphRealm.initialize();
    }

    public void start()
    {
        systemGraphRealm.start();
    }

    void clearCacheForRoles()
    {
        systemGraphRealm.clearCacheForRoles();
    }
}
