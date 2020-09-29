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
import org.neo4j.internal.kernel.api.security.FunctionSegment;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.internal.kernel.api.security.Segment;
import org.neo4j.logging.Log;
import org.neo4j.server.security.systemgraph.ComponentVersion;
import org.neo4j.util.Preconditions;

import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLIC;
import static java.lang.String.format;
import static org.neo4j.server.security.systemgraph.ComponentVersion.LATEST_ENTERPRISE_SECURITY_COMPONENT_VERSION;

/**
 * This is the EnterpriseSecurityComponent version for Neo4j 4.2-drop4.
 */
public class EnterpriseSecurityComponentVersion_5_42D4 extends SupportedEnterpriseSecurityComponentVersion
{
    private final KnownEnterpriseSecurityComponentVersion previous;
    private Node procedurePriv;

    public EnterpriseSecurityComponentVersion_5_42D4( Log log, KnownEnterpriseSecurityComponentVersion previous )
    {
        super( ComponentVersion.ENTERPRISE_SECURITY_42D4, log );
        this.previous = previous;
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
    public void upgradeSecurityGraph( Transaction tx, KnownEnterpriseSecurityComponentVersion latest )
    {
        Preconditions.checkState( latest.version == LATEST_ENTERPRISE_SECURITY_COMPONENT_VERSION,
                format("Latest version should be %s but was %s", LATEST_ENTERPRISE_SECURITY_COMPONENT_VERSION, latest.version ));
        setVersionProperty( tx, latest.version );
        Node publicRole = tx.findNode( ROLE_LABEL, "name", PUBLIC );
        grantExecuteFunctionPrivilegeTo( tx, publicRole );
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
