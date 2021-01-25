/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph.versions;

import com.github.benmanes.caffeine.cache.Cache;
import com.neo4j.server.security.enterprise.auth.Resource;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.SpecialDatabase;
import com.neo4j.server.security.enterprise.systemgraph.versions.PrivilegeStore.PRIVILEGE;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.internal.kernel.api.security.ProcedureSegment;
import org.neo4j.internal.kernel.api.security.Segment;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.logging.Log;

import static com.neo4j.server.security.enterprise.auth.ResourcePrivilege.GrantOrDeny.GRANT;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ADMIN;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ARCHITECT;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.EDITOR;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLISHER;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.READER;
import static com.neo4j.server.security.enterprise.systemgraph.EnterpriseSecurityGraphComponentVersion.ENTERPRISE_SECURITY_40;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.EXECUTE;

/**
 * This is the EnterpriseSecurityComponent version for Neo4j 4.0
 */
public class EnterpriseSecurityComponentVersion_2_40 extends SupportedEnterpriseSecurityComponentVersion
{
    private final KnownEnterpriseSecurityComponentVersion previous;

    public EnterpriseSecurityComponentVersion_2_40( Log log, KnownEnterpriseSecurityComponentVersion previous )
    {
        super( ENTERPRISE_SECURITY_40, log );
        this.previous = previous;
    }

    @Override
    public boolean detected( Transaction tx )
    {
        return nodesWithLabelExist( tx, DATABASE_ALL_LABEL ) &&
               !nodesWithLabelExist( tx, DATABASE_DEFAULT_LABEL ) &&
               componentNotInVersionNode( tx );
    }

    // INITIALIZATION

    @Override
    public void setUpDefaultPrivileges( Transaction tx, PrivilegeStore privilegeStore )
    {
        // Create a DatabaseAll node
        Node allDb = tx.createNode( DATABASE_ALL_LABEL );
        allDb.setProperty( "name", "*" );

        // Create initial qualifier nodes
        Node labelQualifier = tx.createNode( Label.label( "LabelQualifierAll" ) );
        labelQualifier.setProperty( "type", "node" );
        labelQualifier.setProperty( "label", "*" );

        Node relQualifier = tx.createNode( Label.label( "RelationshipQualifierAll" ) );
        relQualifier.setProperty( "type", "relationship" );
        relQualifier.setProperty( "label", "*" );

        Node dbQualifier = tx.createNode( Label.label( "DatabaseQualifier" ) );
        dbQualifier.setProperty( "type", "database" );
        dbQualifier.setProperty( "label", "" );

        // Create initial segments nodes and connect them with DatabaseAll and qualifiers
        Node labelSegment = tx.createNode( SEGMENT_LABEL );
        labelSegment.createRelationshipTo( labelQualifier, QUALIFIED );
        labelSegment.createRelationshipTo( allDb, FOR );

        Node relSegment = tx.createNode( SEGMENT_LABEL );
        relSegment.createRelationshipTo( relQualifier, QUALIFIED );
        relSegment.createRelationshipTo( allDb, FOR );

        Node dbSegment = tx.createNode( SEGMENT_LABEL );
        dbSegment.createRelationshipTo( dbQualifier, QUALIFIED );
        dbSegment.createRelationshipTo( allDb, FOR );

        // Create initial resource nodes
        Node graphResource = tx.createNode( RESOURCE_LABEL );
        graphResource.setProperty( "type", Resource.Type.GRAPH.toString() );
        graphResource.setProperty( "arg1", "" );
        graphResource.setProperty( "arg2", "" );

        Node allPropResource = tx.createNode( RESOURCE_LABEL );
        allPropResource.setProperty( "type", Resource.Type.ALL_PROPERTIES.toString() );
        allPropResource.setProperty( "arg1", "" );
        allPropResource.setProperty( "arg2", "" );

        Node dbResource = tx.createNode( RESOURCE_LABEL );
        dbResource.setProperty( "type", Resource.Type.DATABASE.toString() );
        dbResource.setProperty( "arg1", "" );
        dbResource.setProperty( "arg2", "" );

        // Create initial privilege nodes and connect them with resources and segments
        Node traverseNodePriv = tx.createNode( PRIVILEGE_LABEL );
        Node traverseRelPriv = tx.createNode( PRIVILEGE_LABEL );
        Node readNodePriv = tx.createNode( PRIVILEGE_LABEL );
        Node readRelPriv = tx.createNode( PRIVILEGE_LABEL );
        Node writeNodePriv = tx.createNode( PRIVILEGE_LABEL );
        Node writeRelPriv = tx.createNode( PRIVILEGE_LABEL );
        Node accessPriv = tx.createNode( PRIVILEGE_LABEL );
        Node tokenPriv = tx.createNode( PRIVILEGE_LABEL );
        Node schemaPriv = tx.createNode( PRIVILEGE_LABEL );
        Node adminPriv = tx.createNode( PRIVILEGE_LABEL );

        setupPrivilegeNode( traverseNodePriv, PrivilegeAction.TRAVERSE.toString(), labelSegment, graphResource );
        setupPrivilegeNode( traverseRelPriv, PrivilegeAction.TRAVERSE.toString(), relSegment, graphResource );
        setupPrivilegeNode( readNodePriv, PrivilegeAction.READ.toString(), labelSegment, allPropResource );
        setupPrivilegeNode( readRelPriv, PrivilegeAction.READ.toString(), relSegment, allPropResource );
        setupPrivilegeNode( writeNodePriv, PrivilegeAction.WRITE.toString(), labelSegment, allPropResource );
        setupPrivilegeNode( writeRelPriv, PrivilegeAction.WRITE.toString(), relSegment, allPropResource );
        setupPrivilegeNode( accessPriv, PrivilegeAction.ACCESS.toString(), dbSegment, dbResource );
        setupPrivilegeNode( tokenPriv, PrivilegeAction.TOKEN.toString(), dbSegment, dbResource );
        setupPrivilegeNode( schemaPriv, "schema", dbSegment, dbResource );
        setupPrivilegeNode( adminPriv, PrivilegeAction.ADMIN.toString(), dbSegment, dbResource );

        privilegeStore.setPrivilege( PRIVILEGE.TRAVERSE_NODE, traverseNodePriv );
        privilegeStore.setPrivilege( PRIVILEGE.TRAVERSE_RELATIONSHIP, traverseRelPriv );
        privilegeStore.setPrivilege( PRIVILEGE.READ_NODE_PROPERTY, readNodePriv );
        privilegeStore.setPrivilege( PRIVILEGE.READ_RELATIONSHIP_PROPERTY, readRelPriv );
        privilegeStore.setPrivilege( PRIVILEGE.WRITE_NODE, writeNodePriv );
        privilegeStore.setPrivilege( PRIVILEGE.WRITE_RELATIONSHIP, writeRelPriv );
        privilegeStore.setPrivilege( PRIVILEGE.ACCESS_ALL, accessPriv );
        privilegeStore.setPrivilege( PRIVILEGE.TOKEN, tokenPriv );
        privilegeStore.setPrivilege( PRIVILEGE.SCHEMA, schemaPriv );
        privilegeStore.setPrivilege( PRIVILEGE.ADMIN, adminPriv );
    }

    @Override
    public void grantDefaultPrivileges( Node role, String predefinedRole, PrivilegeStore privilegeStore )
    {
        switch ( predefinedRole )
        {
        case ADMIN:
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.ADMIN ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.SCHEMA ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.TOKEN ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.WRITE_NODE ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.WRITE_RELATIONSHIP ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.TRAVERSE_NODE ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.TRAVERSE_RELATIONSHIP ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.READ_NODE_PROPERTY ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.READ_RELATIONSHIP_PROPERTY ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.ACCESS_ALL ), GRANTED );
            break;

        case ARCHITECT:
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.SCHEMA ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.TOKEN ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.WRITE_NODE ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.WRITE_RELATIONSHIP ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.TRAVERSE_NODE ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.TRAVERSE_RELATIONSHIP ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.READ_NODE_PROPERTY ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.READ_RELATIONSHIP_PROPERTY ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.ACCESS_ALL ), GRANTED );
            break;

        case PUBLISHER:
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.TOKEN ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.WRITE_NODE ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.WRITE_RELATIONSHIP ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.TRAVERSE_NODE ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.TRAVERSE_RELATIONSHIP ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.READ_NODE_PROPERTY ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.READ_RELATIONSHIP_PROPERTY ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.ACCESS_ALL ), GRANTED );
            break;

        case EDITOR:
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.WRITE_NODE ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.WRITE_RELATIONSHIP ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.TRAVERSE_NODE ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.TRAVERSE_RELATIONSHIP ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.READ_NODE_PROPERTY ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.READ_RELATIONSHIP_PROPERTY ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.ACCESS_ALL ), GRANTED );
            break;

        case READER:
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.TRAVERSE_NODE ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.TRAVERSE_RELATIONSHIP ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.READ_NODE_PROPERTY ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.READ_RELATIONSHIP_PROPERTY ), GRANTED );
            role.createRelationshipTo( privilegeStore.getPrivilege( PRIVILEGE.ACCESS_ALL ), GRANTED );
            break;

        default:
        }
    }

    // UPGRADE

    @Override
    public void upgradeSecurityGraph( Transaction tx, int fromVersion ) throws Exception
    {
        if ( fromVersion < version )
        {
            // this will create roles
            previous.upgradeSecurityGraph( tx, fromVersion );
            // Grant default privileges to the roles
            PrivilegeStore privilegeStore = new PrivilegeStore();
            setUpDefaultPrivileges( tx, privilegeStore );
            List<Node> roles = tx.findNodes( ROLE_LABEL ).stream().collect( Collectors.toList() );
            for ( Node role : roles )
            {
                grantDefaultPrivileges( role, role.getProperty( "name" ).toString(), privilegeStore );
            }
        }
    }

    // RUNTIME

    @Override
    boolean supportsUpdateAction( PrivilegeAction action )
    {
        switch ( action )
        {
        // Database privileges
        case ACCESS:
        case START_DATABASE:
        case STOP_DATABASE:

        case CREATE_INDEX:
        case DROP_INDEX:
        case INDEX:
        case CREATE_CONSTRAINT:
        case DROP_CONSTRAINT:
        case CONSTRAINT:

        case CREATE_LABEL:
        case CREATE_RELTYPE:
        case CREATE_PROPERTYKEY:
        case TOKEN:

        case DATABASE_ACTIONS:

        // Role management
        case CREATE_ROLE:
        case DROP_ROLE:
        case ASSIGN_ROLE:
        case REMOVE_ROLE:
        case SHOW_ROLE:
        case ROLE_MANAGEMENT:

        // Graph privileges
        case TRAVERSE:
        case READ:
        case MATCH:
        case WRITE:

            return true;
        default:
            return false;
        }
    }

    @Override
    public void assertUpdateWithAction( PrivilegeAction action, SpecialDatabase specialDatabase, Segment segment ) throws UnsupportedOperationException
    {
        if ( !supportsUpdateAction( action ) || SpecialDatabase.DEFAULT == specialDatabase )
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
    Set<ResourcePrivilege> getTemporaryPrivileges() throws InvalidArgumentsException
    {
        return Collections
                .singleton( new ResourcePrivilege( GRANT, EXECUTE, new Resource.DatabaseResource(), ProcedureSegment.ALL, SpecialDatabase.ALL ) );
    }

    @Override
    public PrivilegeBuilder makePrivilegeBuilder( ResourcePrivilege.GrantOrDeny privilegeType, String action )
    {
        return new PrivilegeBuilder_2_40( privilegeType, action );
    }
}
