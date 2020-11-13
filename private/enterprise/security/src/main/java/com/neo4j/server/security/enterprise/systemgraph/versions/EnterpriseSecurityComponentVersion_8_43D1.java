/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph.versions;

import com.github.benmanes.caffeine.cache.Cache;
import com.neo4j.server.security.enterprise.auth.Resource;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import com.neo4j.server.security.enterprise.systemgraph.versions.PrivilegeStore.PRIVILEGE;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.logging.Log;
import org.neo4j.server.security.systemgraph.ComponentVersion;

/**
 * This is the EnterpriseSecurityComponent version for Neo4j 4.3-drop1.
 */
public class EnterpriseSecurityComponentVersion_8_43D1 extends SupportedEnterpriseSecurityComponentVersion
{
    private final KnownEnterpriseSecurityComponentVersion previous;

    public EnterpriseSecurityComponentVersion_8_43D1( Log log, KnownEnterpriseSecurityComponentVersion previous )
    {
        super( ComponentVersion.ENTERPRISE_SECURITY_43D1, log );
        this.previous = previous;
    }

    @Override
    public void setUpDefaultPrivileges( Transaction tx, PrivilegeStore privilegeStore )
    {
        previous.setUpDefaultPrivileges( tx, privilegeStore );
        this.setVersionProperty( tx, version );
        createNewPrivileges( tx, privilegeStore );
    }

    private void createNewPrivileges( Transaction tx, PrivilegeStore privilegeStore )
    {
        Node allDb = mergeNode( tx, DATABASE_ALL_LABEL, Map.of( "name", "*" ) );

        Node dbQualifier = mergeNode( tx, Label.label( "DatabaseQualifier" ), Map.of( "type", "database", "label", "" ) );
        Node userQualifier = mergeNode( tx, Label.label( "UserQualifierAll" ), Map.of( "type", "user", "label", "*" ) );

        Node userSegment = mergeSegment( tx, allDb, userQualifier );
        Node dbSegment = mergeSegment( tx, allDb, dbQualifier );

        Node dbResource = mergeNode( tx, RESOURCE_LABEL, Map.of( "type", Resource.Type.DATABASE.toString(), "arg1", "", "arg2", "" ) );

        Node transactionPriv = tx.createNode( PRIVILEGE_LABEL );
        Node startDatabasePriv = tx.createNode( PRIVILEGE_LABEL );
        Node stopDatabasePriv = tx.createNode( PRIVILEGE_LABEL );
        Node dbmsActionsPriv = tx.createNode( PRIVILEGE_LABEL );

        setupPrivilegeNode( transactionPriv, PrivilegeAction.TRANSACTION_MANAGEMENT.toString(), userSegment, dbResource );
        setupPrivilegeNode( startDatabasePriv, PrivilegeAction.START_DATABASE.toString(), dbSegment, dbResource );
        setupPrivilegeNode( stopDatabasePriv, PrivilegeAction.STOP_DATABASE.toString(), dbSegment, dbResource );
        setupPrivilegeNode( dbmsActionsPriv, PrivilegeAction.DBMS_ACTIONS.toString(), dbSegment, dbResource );

        privilegeStore.setPrivilege( PRIVILEGE.TRANSACTIONS, transactionPriv );
        privilegeStore.setPrivilege( PRIVILEGE.START_DATABASE, startDatabasePriv );
        privilegeStore.setPrivilege( PRIVILEGE.STOP_DATABASE, stopDatabasePriv );
        privilegeStore.setPrivilege( PRIVILEGE.ALL_DBMS, dbmsActionsPriv );
    }

    @Override
    public void grantDefaultPrivileges( Node role, String predefinedRole, PrivilegeStore privilegeStore )
    {
        if ( predefinedRole.equals( PredefinedRoles.ADMIN ) )
        {
            // ADMIN privilege has been replaced by TRANSACTION_MANAGEMENT, START_DATABASE, STOP_DATABASE and DBMS_ACTIONS
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.TRANSACTIONS ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.START_DATABASE ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.STOP_DATABASE ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.ALL_DBMS ), GRANTED );

            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.CONSTRAINT ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.INDEX ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.TOKEN ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.WRITE_NODE ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.WRITE_RELATIONSHIP ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.MATCH_NODE ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.MATCH_RELATIONSHIP ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PrivilegeStore.PRIVILEGE.ACCESS_ALL ), GRANTED );
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

            // ADMIN privilege has been replaced by TRANSACTION_MANAGEMENT, START_DATABASE, STOP_DATABASE and DBMS_ACTIONS
            Node adminPrivNode = tx.findNode( PRIVILEGE_LABEL, "action", PrivilegeAction.ADMIN.toString() );
            if ( adminPrivNode != null )
            {
                PrivilegeStore privilegeStore = new PrivilegeStore();
                createNewPrivileges( tx, privilegeStore );
                Set<Node> rolesGrantedAdmin = new HashSet<>();
                adminPrivNode.getRelationships( GRANTED ).forEach( r -> rolesGrantedAdmin.add( r.getOtherNode( adminPrivNode ) ) );
                for ( Node role : rolesGrantedAdmin )
                {
                    role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.TRANSACTIONS ), GRANTED );
                    role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.START_DATABASE ), GRANTED );
                    role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.STOP_DATABASE ), GRANTED );
                    role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.ALL_DBMS ), GRANTED );
                }
                for ( Relationship rel : adminPrivNode.getRelationships() )
                {
                    rel.delete();
                }
                adminPrivNode.delete();
            }
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
