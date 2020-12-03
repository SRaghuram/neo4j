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
import org.neo4j.internal.kernel.api.security.FunctionSegment;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.internal.kernel.api.security.Segment;
import org.neo4j.logging.Log;

import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLIC;
import static com.neo4j.server.security.enterprise.systemgraph.EnterpriseSecurityGraphComponentVersion.ENTERPRISE_SECURITY_42D4;

/**
 * This is the EnterpriseSecurityComponent version for Neo4j 4.2-drop4.
 */
public class EnterpriseSecurityComponentVersion_5_42D4 extends SupportedEnterpriseSecurityComponentVersion
{
    private final KnownEnterpriseSecurityComponentVersion previous;

    public EnterpriseSecurityComponentVersion_5_42D4( Log log, KnownEnterpriseSecurityComponentVersion previous )
    {
        super( ENTERPRISE_SECURITY_42D4, log );
        this.previous = previous;
    }

    @Override
    public void setUpDefaultPrivileges( Transaction tx, PrivilegeStore privilegeStore )
    {
        previous.setUpDefaultPrivileges( tx, privilegeStore );
        this.setVersionProperty( tx, version );

        Node allDb = mergeNode( tx, DATABASE_ALL_LABEL, Map.of( "name", "*" ) );

        // Create new privilege for execute procedures
        Node procQualifier = mergeNode( tx, Label.label( "ProcedureQualifierAll" ), Map.of(
                "type", "procedure",
                "label", "*"
        ) );

        Node procSegment = mergeSegment( tx, allDb, procQualifier );

        Node dbResource = mergeNode( tx, RESOURCE_LABEL, Map.of(
                "type", Resource.Type.DATABASE.toString(),
                "arg1", "",
                "arg2", ""
        ) );

        Node procedurePriv = tx.createNode( PRIVILEGE_LABEL );
        setupPrivilegeNode( procedurePriv, PrivilegeAction.EXECUTE.toString(), procSegment, dbResource );
        privilegeStore.setPrivilege( PRIVILEGE.EXECUTE_ALL_PROCEDURES, procedurePriv );
    }

    @Override
    public void grantDefaultPrivileges( Node role, String predefinedRole, PrivilegeStore privilegeStore )
    {
        if ( predefinedRole.equals( PUBLIC ) )
        {
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.ACCESS_DEFAULT ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.EXECUTE_ALL_PROCEDURES ), GRANTED );
        }
        else
        {
            previous.grantDefaultPrivileges( role, predefinedRole, privilegeStore );
        }
    }

    @Override
    public void upgradeSecurityGraph( Transaction tx, int fromVersion ) throws Exception
    {
        if ( fromVersion < version )
        {
            previous.upgradeSecurityGraph( tx, fromVersion );
            this.setVersionProperty( tx, version );
            // GRANT EXECUTE PROCEDURE * ON DBMS TO PUBLIC
            Node publicRole = mergeNode( tx, ROLE_LABEL, Map.of( "name", PUBLIC ) );
            grantExecuteProcedurePrivilegeTo( tx, publicRole );
        }
    }

    @Override
    boolean supportsUpdateAction( PrivilegeAction action )
    {
        switch ( action )
        {
        // Execute procedures
        case EXECUTE:
        case EXECUTE_BOOSTED:
        case EXECUTE_ADMIN:
            return true;

        default:
            return previous.supportsUpdateAction( action );
        }
    }

    @Override
    public void assertUpdateWithAction( PrivilegeAction action, ResourcePrivilege.SpecialDatabase specialDatabase, Segment segment )
            throws UnsupportedOperationException
    {
        if ( !supportsUpdateAction( action ) || segment == FunctionSegment.ALL )
        {
            throw unsupportedAction();
        }
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
