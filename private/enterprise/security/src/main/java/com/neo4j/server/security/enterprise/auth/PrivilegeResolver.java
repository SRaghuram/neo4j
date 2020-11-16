/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.server.security.enterprise.auth.Resource.DatabaseResource;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.SpecialDatabase;
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphRealm;
import org.apache.shiro.cache.CacheManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.security.FunctionSegment;
import org.neo4j.internal.kernel.api.security.ProcedureSegment;
import org.neo4j.internal.kernel.api.security.Segment;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

import static com.neo4j.server.security.enterprise.auth.ResourcePrivilege.GrantOrDeny.DENY;
import static com.neo4j.server.security.enterprise.auth.ResourcePrivilege.GrantOrDeny.GRANT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DATABASE_MANAGEMENT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.EXECUTE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.EXECUTE_BOOSTED;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.START_DATABASE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.STOP_DATABASE;

public class PrivilegeResolver
{
    public static final String EXECUTE_BOOSTED_FROM_CONFIG = "execute_boosted_from_config";

    private final SystemGraphRealm systemGraphRealm;
    private final Boolean restrictUpgrade;
    private final Boolean blockCreateDrop;
    private final Boolean blockStartStop;

    private final ResourcePrivilege denyExecuteUpgrade;
    private final ResourcePrivilege denyStartDatabase;
    private final ResourcePrivilege denyStopDatabase;
    private final ResourcePrivilege denyCreateDropDatabase;

    private final String roleToBoostAll;
    private final String roleToBoostMapping;

    private final Map<String, Set<ResourcePrivilege>> roleToPrivilege = new HashMap<>();

    public PrivilegeResolver( SystemGraphRealm systemGraphRealm, Config config )
    {
        this.systemGraphRealm = systemGraphRealm;
        this.restrictUpgrade = config.get( GraphDatabaseInternalSettings.block_upgrade_procedures );
        this.roleToBoostAll = config.get( GraphDatabaseSettings.default_allowed );
        this.roleToBoostMapping = config.get( GraphDatabaseSettings.procedure_roles );
        this.blockCreateDrop = config.get( GraphDatabaseInternalSettings.block_create_drop_database );
        this.blockStartStop = config.get( GraphDatabaseInternalSettings.block_start_stop_database );

        try
        {
            initProcedurePrivilegesFromConfig();

            // Privileges for the PUBLIC role when using GraphDatabaseInternalSettings.restrict_upgrade
            ProcedureSegment segment = new ProcedureSegment( "dbms.upgrade*" );
            denyExecuteUpgrade = new ResourcePrivilege( DENY, EXECUTE, new DatabaseResource(), segment, ResourcePrivilege.SpecialDatabase.ALL );

            // Privileges for the PUBLIC role when using GraphDatabaseInternalSettings.block_create_drop_database
            // DENY CREATE & DROP DATABASE ON DBMS
            denyCreateDropDatabase = new ResourcePrivilege( DENY, DATABASE_MANAGEMENT, new DatabaseResource(), Segment.ALL,
                    ResourcePrivilege.SpecialDatabase.ALL );

            // Privileges for the PUBLIC role when using GraphDatabaseInternalSettings.block_start_stop_database
            // DENY START DATABASE for all databases
            denyStartDatabase = new ResourcePrivilege( DENY, START_DATABASE, new DatabaseResource(), Segment.ALL, ResourcePrivilege.SpecialDatabase.ALL );
            // DENY STOP DATABASE for all databases
            denyStopDatabase = new ResourcePrivilege( DENY, STOP_DATABASE, new DatabaseResource(), Segment.ALL, ResourcePrivilege.SpecialDatabase.ALL );
        }
        catch ( InvalidArgumentsException e )
        {
            throw new IllegalStateException( "Statically created privilege not accepted...", e );
        }

    }

    HashSet<ResourcePrivilege> getPrivileges( Set<String> roles )
    {
        HashSet<ResourcePrivilege> privileges = systemGraphRealm.getPrivilegesForRoles( roles );
        for ( String role : roles )
        {
            privileges.addAll( roleToPrivilege.getOrDefault( role, Collections.emptySet() ) );
            if ( role.equals( PredefinedRoles.PUBLIC ) )
            {
                if ( restrictUpgrade )
                {
                    privileges.add( denyExecuteUpgrade );
                }
                if ( blockCreateDrop )
                {
                    privileges.add( denyCreateDropDatabase );
                }
                if ( blockStartStop )
                {
                    privileges.add( denyStartDatabase );
                    privileges.add( denyStopDatabase );
                }
            }
        }
        return privileges;
    }

    private void initProcedurePrivilegesFromConfig() throws InvalidArgumentsException
    {
        final String SETTING_DELIMITER = ";";
        final String MAPPING_DELIMITER = ":";
        final String ROLES_DELIMITER = ",";

        HashSet<ResourcePrivilege> allMappedBoostPrivileges = new HashSet<>();

        for ( String procToRoleSpec : roleToBoostMapping.split( SETTING_DELIMITER ) )
        {
            // All procedures in dbms.security.procedures.roles should result in a temporary privilege:
            // GRANT EXECUTE BOOSTED PROCEDURE procedure ON DBMS TO role1 [,role2 ...]
            // GRANT EXECUTE BOOSTED FUNCTION function ON DBMS TO role1 [,role2 ...]
            String[] spec = procToRoleSpec.split( MAPPING_DELIMITER );
            if ( spec.length == 2 )
            {
                ProcedureSegment procSegment = new ProcedureSegment( spec[0].trim() );
                FunctionSegment funcSegment = new FunctionSegment( spec[0].trim() );
                ResourcePrivilege procPrivilege = new ResourcePrivilege( GRANT, EXECUTE_BOOSTED, new DatabaseResource(), procSegment, SpecialDatabase.ALL );
                ResourcePrivilege funcPrivilege = new ResourcePrivilege( GRANT, EXECUTE_BOOSTED, new DatabaseResource(), funcSegment, SpecialDatabase.ALL );
                allMappedBoostPrivileges.add( procPrivilege );
                allMappedBoostPrivileges.add( funcPrivilege );
                for ( String role : spec[1].split( ROLES_DELIMITER ) )
                {
                    Set<ResourcePrivilege> privileges = roleToPrivilege.computeIfAbsent( role.trim(), x -> new HashSet<>() );
                    privileges.add( procPrivilege );
                    privileges.add( funcPrivilege );
                }
            }
        }

        if ( !roleToBoostAll.isBlank() )
        {
            // The role specified with dbms.security.procedures.default_allowed, should have a temporary privilege:
            // GRANT EXECUTE BOOSTED PROCEDURE * ON DBMS TO roleToBoostAll
            // GRANT EXECUTE BOOSTED FUNCTION * ON DBMS TO roleToBoostAll
            // All procedures in dbms.security.procedures.roles that aren't mapped to roleToBoostAll should have a temporary privilege:
            // DENY EXECUTE BOOSTED PROCEDURE procedure ON DBMS TO roleToBoostAll
            // DENY EXECUTE BOOSTED FUNCTION procedure ON DBMS TO roleToBoostAll
            Set<ResourcePrivilege> privilegeSet = roleToPrivilege.computeIfAbsent( roleToBoostAll, x -> new HashSet<>() );
            privilegeSet.add( new ResourcePrivilege( GRANT, EXECUTE_BOOSTED, new DatabaseResource(), ProcedureSegment.ALL, SpecialDatabase.ALL ) );
            privilegeSet.add( new ResourcePrivilege( GRANT, EXECUTE_BOOSTED, new DatabaseResource(), FunctionSegment.ALL, SpecialDatabase.ALL ) );

            allMappedBoostPrivileges.removeAll( privilegeSet );
            for ( ResourcePrivilege privilege : allMappedBoostPrivileges )
            {
                privilegeSet.add( new ResourcePrivilege( DENY, EXECUTE_BOOSTED, new DatabaseResource(), privilege.getSegment(), SpecialDatabase.ALL ) );
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

    List<Map<String,String>> getPrivilegesGrantedThroughConfig()
    {
        ArrayList<Map<String,String>> result = new ArrayList<>();
        for ( var entry : roleToPrivilege.entrySet() )
        {
            String role = entry.getKey();
            for ( var privilege : entry.getValue() )
            {
                String segmentString;
                if ( privilege.getSegment() instanceof ProcedureSegment )
                {
                    ProcedureSegment segment = (ProcedureSegment) privilege.getSegment();
                    segmentString = String.format( "PROCEDURE(%s)", segment.equals( ProcedureSegment.ALL ) ? "*" : segment.getProcedure() );
                }
                else
                {
                    FunctionSegment segment = (FunctionSegment) privilege.getSegment();
                    segmentString = String.format( "FUNCTION(%s)", segment.equals( FunctionSegment.ALL ) ? "*" : segment.getFunction() );
                }
                result.add( Map.of(
                        "role", role,
                        "graph", "*",
                        "segment", segmentString,
                        "resource", "database",
                        "action", EXECUTE_BOOSTED_FROM_CONFIG,
                        "access", privilege.getPrivilegeType().relType
                ) );
            }
        }
        return result;
    }
}
