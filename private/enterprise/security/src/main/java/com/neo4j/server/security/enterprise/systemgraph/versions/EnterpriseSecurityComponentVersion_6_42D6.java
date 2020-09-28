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
import org.neo4j.server.security.systemgraph.ComponentVersion;

import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLIC;

/**
 * This is the EnterpriseSecurityComponent version for Neo4j 4.2-drop6.
 */
public class EnterpriseSecurityComponentVersion_6_42D6 extends SupportedEnterpriseSecurityComponentVersion
{
    private final KnownEnterpriseSecurityComponentVersion previous;
    private Node functionPriv;
    private Node procedurePriv;

    public EnterpriseSecurityComponentVersion_6_42D6( Log log, KnownEnterpriseSecurityComponentVersion previous )
    {
        super( ComponentVersion.ENTERPRISE_SECURITY_42D6, log );
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

        // Create new privilege for execute functions
        Node funcQualifier = tx.createNode( Label.label( "FunctionQualifierAll" ) );
        funcQualifier.setProperty( "type", "function" );
        funcQualifier.setProperty( "label", "*" );

        Node funcSegment = tx.createNode( SEGMENT_LABEL );
        funcSegment.createRelationshipTo( funcQualifier, QUALIFIED );
        funcSegment.createRelationshipTo( allDb, FOR );

        functionPriv = tx.createNode( PRIVILEGE_LABEL );
        setupPrivilegeNode( functionPriv, PrivilegeAction.EXECUTE.toString(), funcSegment, dbResource );
    }

    @Override
    public void assignDefaultPrivileges( Node role, String predefinedRole )
    {
        super.assignDefaultPrivileges( role, predefinedRole );
        if ( predefinedRole.equals( PUBLIC ) )
        {
            role.createRelationshipTo( functionPriv, GRANTED );
            role.createRelationshipTo( procedurePriv, GRANTED );
        }
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
