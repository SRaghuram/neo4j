/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph.versions;

import com.github.benmanes.caffeine.cache.Cache;
import com.neo4j.server.security.enterprise.auth.Resource;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.systemgraph.versions.PrivilegeStore.PRIVILEGE;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.logging.Log;
import org.neo4j.server.security.systemgraph.ComponentVersion;
import org.neo4j.util.Preconditions;

import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLIC;
import static java.lang.String.format;
import static org.neo4j.server.security.systemgraph.ComponentVersion.LATEST_ENTERPRISE_SECURITY_COMPONENT_VERSION;

/**
 * This is the EnterpriseSecurityComponent version for Neo4j 4.2-drop6.
 */
public class EnterpriseSecurityComponentVersion_6_42D6 extends SupportedEnterpriseSecurityComponentVersion
{
    private final KnownEnterpriseSecurityComponentVersion previous;

    public EnterpriseSecurityComponentVersion_6_42D6( Log log, KnownEnterpriseSecurityComponentVersion previous )
    {
        super( ComponentVersion.ENTERPRISE_SECURITY_42D6, log );
        this.previous = previous;
    }

    @Override
    public void setUpDefaultPrivileges( Transaction tx, PrivilegeStore privilegeStore )
    {
        previous.setUpDefaultPrivileges( tx, privilegeStore );
        this.setVersionProperty( tx, version );

        Node allDb = mergeNode( tx, DATABASE_ALL_LABEL, Map.of( "name", "*" ) );

        // Create new privilege for execute functions
        Node funcQualifier = mergeNode( tx, Label.label( "FunctionQualifierAll" ), Map.of(
                "type", "function",
                "label", "*"
        ) );

        Node funcSegment = mergeSegment( tx, allDb, funcQualifier );

        Node dbResource = mergeNode( tx, RESOURCE_LABEL, Map.of(
                "type", Resource.Type.DATABASE.toString(),
                "arg1", "",
                "arg2", ""
        ) );

        Node functionPriv = tx.createNode( PRIVILEGE_LABEL );
        setupPrivilegeNode( functionPriv, PrivilegeAction.EXECUTE.toString(), funcSegment, dbResource );
        privilegeStore.setPrivilege( PRIVILEGE.EXECUTE_ALL_FUNCTIONS, functionPriv );
    }

    @Override
    public void grantDefaultPrivileges( Transaction tx, Node role, String predefinedRole, PrivilegeStore privilegeStore )
    {
        if ( predefinedRole.equals( PUBLIC ) )
        {
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.EXECUTE_ALL_FUNCTIONS ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.ACCESS_DEFAULT ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.EXECUTE_ALL_PROCEDURES ), GRANTED );
        }
        else
        {
            previous.grantDefaultPrivileges( tx, role, predefinedRole, privilegeStore );
        }
    }

    @Override
    public void upgradeSecurityGraph( Transaction tx, KnownEnterpriseSecurityComponentVersion latest )
    {
        Preconditions.checkState( latest.version == LATEST_ENTERPRISE_SECURITY_COMPONENT_VERSION,
                format("Latest version should be %s but was %s", LATEST_ENTERPRISE_SECURITY_COMPONENT_VERSION, latest.version ));
        setVersionProperty( tx, latest.version );
    }

    @Override
    boolean supportsUpdateAction( PrivilegeAction action )
    {
        return previous.supportsUpdateAction( action );
    }

    @Override
    public Set<ResourcePrivilege> getPrivilegeForRoles( Transaction tx, List<String> roleNames, Cache<String,Set<ResourcePrivilege>> privilegeCache )
    {
        return super.currentGetPrivilegeForRoles( tx, roleNames, privilegeCache );
    }

    @Override
    public PrivilegeBuilder makePrivilegeBuilder( ResourcePrivilege.GrantOrDeny privilegeType, String action )
    {
        return new PrivilegeBuilder( privilegeType, action );
    }
}
