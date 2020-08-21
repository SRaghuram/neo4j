/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph.versions;

import com.github.benmanes.caffeine.cache.Cache;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;

import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.logging.Log;

import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLIC;
import static com.neo4j.server.security.enterprise.systemgraph.EnterpriseSecurityGraphComponent.LATEST_VERSION;

public class EnterpriseVersion_5_42d3 extends SupportedEnterpriseVersion
{
    private final KnownEnterpriseSecurityComponentVersion previous;
    private Node procedurePriv;

    public EnterpriseVersion_5_42d3( Log log, KnownEnterpriseSecurityComponentVersion previous )
    {
        super( LATEST_VERSION, VERSION_42D3, log, true );
        this.previous = previous;
    }

    @Override
    public boolean migrationSupported()
    {
        return true;
    }

    @Override
    public boolean runtimeSupported()
    {
        return true;
    }

    @Override
    public void setUpDefaultPrivileges( Transaction tx )
    {
        super.setUpDefaultPrivileges( tx );
        this.setVersionProperty( tx, version );

        // Create new privilege for execute procedures
        Node procQualifier = tx.createNode( Label.label( "ProcedureQualifierAll" ) );
        procQualifier.setProperty( "type", "procedure" );
        procQualifier.setProperty( "label", "*" );

        Node procSegment = tx.createNode( SEGMENT_LABEL );
        procSegment.createRelationshipTo( procQualifier, QUALIFIED );
        procSegment.createRelationshipTo( allDb, FOR );

        procedurePriv = tx.createNode( PRIVILEGE_LABEL );
        setupPrivilegeNode( procedurePriv, PrivilegeAction.EXECUTE.toString(), procSegment, dbResource );
    }

    @Override
    public void assignDefaultPrivileges( Node role, String predefinedRole )
    {
        super.assignDefaultPrivileges( role, predefinedRole );
        if ( predefinedRole.equals( PUBLIC ) )
        {
            role.createRelationshipTo( procedurePriv, GRANTED );
        }
    }

    @Override
    boolean supportsUpdateAction( PrivilegeAction action )
    {
        switch ( action )
        {
        // Execute procedures
        case EXECUTE:
            return true;

        default:
            return previous.supportsUpdateAction( action );
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
