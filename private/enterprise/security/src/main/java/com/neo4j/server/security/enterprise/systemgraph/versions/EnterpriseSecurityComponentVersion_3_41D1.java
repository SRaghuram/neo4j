/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph.versions;

import com.github.benmanes.caffeine.cache.Cache;
import com.neo4j.server.security.enterprise.auth.Resource;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.SpecialDatabase;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.internal.kernel.api.security.ProcedureSegment;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.logging.Log;
import org.neo4j.server.security.systemgraph.ComponentVersion;
import org.neo4j.util.Preconditions;

import static com.neo4j.server.security.enterprise.auth.ResourcePrivilege.GrantOrDeny.GRANT;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ADMIN;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ARCHITECT;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.EDITOR;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLIC;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLISHER;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.READER;
import static java.lang.String.format;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.EXECUTE;
import static org.neo4j.server.security.systemgraph.ComponentVersion.LATEST_ENTERPRISE_SECURITY_COMPONENT_VERSION;

/**
 * This is the EnterpriseSecurityComponent version for Neo4j 4.1-drop1.
 */
public class EnterpriseSecurityComponentVersion_3_41D1 extends SupportedEnterpriseSecurityComponentVersion
{
    private final KnownEnterpriseSecurityComponentVersion previous;

    public EnterpriseSecurityComponentVersion_3_41D1( Log log, KnownEnterpriseSecurityComponentVersion previous )
    {
        super( ComponentVersion.ENTERPRISE_SECURITY_41D1, log );
        this.previous = previous;
    }

    @Override
    public boolean detected( Transaction tx )
    {
        return nodesWithLabelExist( tx, DATABASE_ALL_LABEL ) &&
               nodesWithLabelExist( tx, DATABASE_DEFAULT_LABEL ) &&
               componentNotInVersionNode( tx );
    }

    // INITIALIZATION

    @Override
    public void setUpDefaultPrivileges( Transaction tx, PrivilegeStore privilegeStore )
    {
        previous.setUpDefaultPrivileges( tx, privilegeStore );

        Node allDb = mergeNode( tx, DATABASE_ALL_LABEL, Map.of( "name", "*" ) );

        // Create a DatabaseDefault node
        Node defaultDb = mergeNode( tx, DATABASE_DEFAULT_LABEL, Map.of( "name", "DEFAULT" ) );

        Node labelQualifier = mergeNode( tx, Label.label( "LabelQualifierAll" ), Map.of(
                "type", "node",
                "label", "*"
        ) );

        Node relQualifier = mergeNode( tx, Label.label( "RelationshipQualifierAll" ), Map.of(
                "type", "relationship",
                "label", "*"
        ) );

        Node dbQualifier = mergeNode( tx, Label.label( "DatabaseQualifier" ), Map.of(
                "type", "database",
                "label", ""
        ) );

        // Create initial segments nodes and connect them with Database and qualifiers
        Node labelSegment = mergeSegment( tx, allDb, labelQualifier );
        Node relSegment = mergeSegment( tx, allDb, relQualifier );
        Node dbSegment = mergeSegment( tx, allDb, dbQualifier );
        Node defaultDbSegment = mergeSegment( tx, defaultDb, dbQualifier );

        Node graphResource = mergeNode( tx, Label.label( "Resource" ), Map.of(
                "type", Resource.Type.GRAPH.toString(),
                "arg1", "",
                "arg2", ""
        ) );

        Node allPropResource = mergeNode( tx, Label.label( "Resource" ), Map.of(
                "type", Resource.Type.ALL_PROPERTIES.toString(),
                "arg1", "",
                "arg2", ""
        ) );

        Node dbResource = mergeNode( tx, RESOURCE_LABEL, Map.of(
                "type", Resource.Type.DATABASE.toString(),
                "arg1", "",
                "arg2", ""
        ) );

        // Create initial privilege nodes and connect them with resources and segments
        Node matchNodePriv = tx.createNode( PRIVILEGE_LABEL );
        Node matchRelPriv = tx.createNode( PRIVILEGE_LABEL );
        Node writeNodePriv = tx.createNode( PRIVILEGE_LABEL );
        Node writeRelPriv = tx.createNode( PRIVILEGE_LABEL );
        Node defaultAccessPriv = tx.createNode( PRIVILEGE_LABEL );
        Node indexPriv = tx.createNode( PRIVILEGE_LABEL );
        Node constraintPriv = tx.createNode( PRIVILEGE_LABEL );

        setupPrivilegeNode( matchNodePriv, PrivilegeAction.MATCH.toString(), labelSegment, allPropResource );
        setupPrivilegeNode( matchRelPriv, PrivilegeAction.MATCH.toString(), relSegment, allPropResource );
        setupPrivilegeNode( writeNodePriv, PrivilegeAction.WRITE.toString(), labelSegment, graphResource );
        setupPrivilegeNode( writeRelPriv, PrivilegeAction.WRITE.toString(), relSegment, graphResource );
        setupPrivilegeNode( defaultAccessPriv, PrivilegeAction.ACCESS.toString(), defaultDbSegment, dbResource );
        setupPrivilegeNode( indexPriv, PrivilegeAction.INDEX.toString(), dbSegment, dbResource );
        setupPrivilegeNode( constraintPriv, PrivilegeAction.CONSTRAINT.toString(), dbSegment, dbResource );

        privilegeStore.setPrivilege( PrivilegeStore.PRIVILEGE.MATCH_NODE, matchNodePriv );
        privilegeStore.setPrivilege( PrivilegeStore.PRIVILEGE.MATCH_RELATIONSHIP, matchRelPriv );
        privilegeStore.setPrivilege( PrivilegeStore.PRIVILEGE.WRITE_NODE, writeNodePriv );
        privilegeStore.setPrivilege( PrivilegeStore.PRIVILEGE.WRITE_RELATIONSHIP, writeRelPriv );
        privilegeStore.setPrivilege( PrivilegeStore.PRIVILEGE.ACCESS_DEFAULT, defaultAccessPriv );
        privilegeStore.setPrivilege( PrivilegeStore.PRIVILEGE.INDEX, indexPriv );
        privilegeStore.setPrivilege( PrivilegeStore.PRIVILEGE.CONSTRAINT, constraintPriv );
    }

    @Override
    public void grantDefaultPrivileges( Transaction tx, Node role, String predefinedRole, PrivilegeStore privilegeStore )
    {
        switch ( predefinedRole )
        {
        case ADMIN:
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.ADMIN ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.CONSTRAINT ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.INDEX ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.TOKEN ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.WRITE_NODE ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.WRITE_RELATIONSHIP ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.MATCH_NODE ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.MATCH_RELATIONSHIP ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.ACCESS_ALL ), GRANTED );
            break;

        case ARCHITECT:
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.CONSTRAINT ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.INDEX ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.TOKEN ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.WRITE_NODE ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.WRITE_RELATIONSHIP ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.MATCH_NODE ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.MATCH_RELATIONSHIP ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.ACCESS_ALL ), GRANTED );
            break;

        case PUBLISHER:
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.TOKEN ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.WRITE_NODE ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.WRITE_RELATIONSHIP ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.MATCH_NODE ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.MATCH_RELATIONSHIP ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.ACCESS_ALL ), GRANTED );
            break;

        case EDITOR:
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.WRITE_NODE ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.WRITE_RELATIONSHIP ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.MATCH_NODE ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.MATCH_RELATIONSHIP ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.ACCESS_ALL ), GRANTED );
            break;

        case READER:
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.MATCH_NODE ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.MATCH_RELATIONSHIP ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.ACCESS_ALL ), GRANTED );
            break;

        case PUBLIC:
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.ACCESS_DEFAULT ), GRANTED );
            break;

        default:
        }

    }

    // UPGRADE

    @Override
    public void upgradeSecurityGraph( Transaction tx, KnownEnterpriseSecurityComponentVersion latest )
    {
        Preconditions.checkState( latest.version == LATEST_ENTERPRISE_SECURITY_COMPONENT_VERSION,
                format("Latest version should be %s but was %s", LATEST_ENTERPRISE_SECURITY_COMPONENT_VERSION, latest.version ));
        log.info( String.format( "Upgrading security model from %s by adding version information", this.description ) );
        // Upgrade from 4.1.0-Drop01 to 4.1.x, which means add the Version node
        setVersionProperty( tx, latest.version );
        Node publicRole = tx.findNode( ROLE_LABEL, "name", PUBLIC );
        grantExecuteProcedurePrivilegeTo( tx, publicRole );
        grantExecuteFunctionPrivilegeTo( tx, publicRole );
    }

    // RUNTIME

    @Override
    boolean supportsUpdateAction( PrivilegeAction action )
    {
        switch ( action )
        {
        // User management
        case CREATE_USER:
        case DROP_USER:
        case SHOW_USER:
        case ALTER_USER:
        case USER_MANAGEMENT:

        // Database management
        case CREATE_DATABASE:
        case DROP_DATABASE:
        case DATABASE_MANAGEMENT:

        // Privilege management
        case SHOW_PRIVILEGE:
        case ASSIGN_PRIVILEGE:
        case REMOVE_PRIVILEGE:
        case PRIVILEGE_MANAGEMENT:

        case DBMS_ACTIONS:

        case TRANSACTION_MANAGEMENT:
        case SHOW_TRANSACTION:
        case TERMINATE_TRANSACTION:
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
    Set<ResourcePrivilege> getTemporaryPrivileges() throws InvalidArgumentsException
    {
        return Collections
                .singleton( new ResourcePrivilege( GRANT, EXECUTE, new Resource.DatabaseResource(), ProcedureSegment.ALL, SpecialDatabase.ALL ) );
    }

    @Override
    public PrivilegeBuilder makePrivilegeBuilder( ResourcePrivilege.GrantOrDeny privilegeType, String action )
    {
        return new PrivilegeBuilder( privilegeType, action );
    }
}
