/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.server.security.enterprise.auth.Resource.DatabaseResource;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.SpecialDatabase;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphRealm;
import org.apache.shiro.cache.CacheManager;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.security.ProcedureSegment;
import org.neo4j.internal.kernel.api.security.Segment;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

import static com.neo4j.server.security.enterprise.auth.ResourcePrivilege.GrantOrDeny.GRANT;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ACCESS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DATABASE_MANAGEMENT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.EXECUTE_BOOSTED;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.START_DATABASE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.STOP_DATABASE;

public class PrivilegeResolver
{
    private final SystemGraphRealm systemGraphRealm;
    private final String upgradeUsername;
    private final Boolean restrictUpgrade;
    private final ResourcePrivilege accessOnSystem;
    private final ResourcePrivilege executeBoostedUpgrade;
    private final ResourcePrivilege createDropDatabase;
    private final ResourcePrivilege startDatabase;
    private final ResourcePrivilege stopDatabase;

    private final String roleToBoostAll;
    private final String roleToBoostMapping;

    private final Map<String, Set<ResourcePrivilege>> roleToPrivilege = new HashMap<>();

    public PrivilegeResolver( SystemGraphRealm systemGraphRealm, Config config )
    {
        this.systemGraphRealm = systemGraphRealm;
        this.upgradeUsername = config.get( GraphDatabaseInternalSettings.upgrade_username );
        this.restrictUpgrade = config.get( GraphDatabaseInternalSettings.restrict_upgrade );
        this.roleToBoostAll = config.get( GraphDatabaseSettings.default_allowed );
        this.roleToBoostMapping = config.get( GraphDatabaseSettings.procedure_roles );

        try
        {
            initProcedurePrivilegesFromConfig();

            // Privileges for the operator user

            // ACCESS ON DATABASE system
            accessOnSystem = new ResourcePrivilege( GRANT, ACCESS, new DatabaseResource(), Segment.ALL, SYSTEM_DATABASE_NAME );
            // EXECUTE BOOSTED dbms.upgrade* ON DBMS
            ProcedureSegment segment = new ProcedureSegment( "dbms.upgrade*" );
            executeBoostedUpgrade = new ResourcePrivilege( GRANT, EXECUTE_BOOSTED, new DatabaseResource(), segment, SYSTEM_DATABASE_NAME );
            // CRETE & DROP DATABASE ON DBMS
            createDropDatabase = new ResourcePrivilege( GRANT, DATABASE_MANAGEMENT, new DatabaseResource(), Segment.ALL,
                    ResourcePrivilege.SpecialDatabase.ALL );
            // START DATABASE for all databases
            startDatabase = new ResourcePrivilege( GRANT, START_DATABASE, new DatabaseResource(), Segment.ALL, ResourcePrivilege.SpecialDatabase.ALL );
            // STOP DATABASE for all databases
            stopDatabase = new ResourcePrivilege( GRANT, STOP_DATABASE, new DatabaseResource(), Segment.ALL, ResourcePrivilege.SpecialDatabase.ALL );
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
            privileges.add( executeBoostedUpgrade );
            privileges.add( createDropDatabase );
            privileges.add( startDatabase );
            privileges.add( stopDatabase );
            return privileges;
        }
        return Collections.emptySet();
    }

    private void initProcedurePrivilegesFromConfig() throws InvalidArgumentsException
    {
        final String SETTING_DELIMITER = ";";
        final String MAPPING_DELIMITER = ":";
        final String ROLES_DELIMITER = ",";

        if ( !roleToBoostAll.isBlank() )
        {
            HashSet<ResourcePrivilege> privilegeSet = new HashSet<>();
            privilegeSet.add( new ResourcePrivilege( GRANT, EXECUTE_BOOSTED, new DatabaseResource(), ProcedureSegment.ALL, SpecialDatabase.ALL ) );
            roleToPrivilege.put( roleToBoostAll, privilegeSet );
        }

        for ( String procToRoleSpec : roleToBoostMapping.split( SETTING_DELIMITER ) )
        {
            String[] spec = procToRoleSpec.split( MAPPING_DELIMITER );
            if ( spec.length == 2 )
            {
                ProcedureSegment procSegment = new ProcedureSegment( spec[0].trim() );
                ResourcePrivilege privilege = new ResourcePrivilege( GRANT, EXECUTE_BOOSTED, new DatabaseResource(), procSegment, SpecialDatabase.ALL );
                for ( String role : spec[1].split( ROLES_DELIMITER ) )
                {
                    Set<ResourcePrivilege> privileges = roleToPrivilege.computeIfAbsent( role.trim(), x -> new HashSet<>() );
                    privileges.add( privilege );
                }
            }
        }
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
