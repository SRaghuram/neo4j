/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.kernel.impl.newapi.Labels;

import static com.neo4j.server.security.enterprise.auth.ResourcePrivilege.GrantOrDeny.DENY;
import static com.neo4j.server.security.enterprise.auth.ResourcePrivilege.GrantOrDeny.GRANT;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.READ;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TRAVERSE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.WRITE;
import static org.neo4j.token.api.TokenConstants.ANY_LABEL;
import static org.neo4j.token.api.TokenConstants.ANY_RELATIONSHIP_TYPE;

class StandardAccessModeTest
{
    private static final long A = 0;
    private static final long B = 1;
    private static final int R1 = 2;
    private static final int R2 = 3;
    private static final int PROP1 = 4;
    private static final int PROP2 = 5;

    private StandardAccessMode.Builder builder;

    @BeforeEach
    void setup()
    {
        LoginContext.IdLookup lookup = mock( LoginContext.IdLookup.class );
        when( lookup.getLabelId( "A" ) ).thenReturn( (int) A );
        when( lookup.getLabelId( "B" ) ).thenReturn( (int) B );
        when( lookup.getRelTypeId( "R1" ) ).thenReturn( R1 );
        when( lookup.getRelTypeId( "R2" ) ).thenReturn( R2 );
        when( lookup.getPropertyKeyId( "PROP1" ) ).thenReturn( PROP1 );
        when( lookup.getPropertyKeyId( "PROP2" ) ).thenReturn( PROP2 );

        builder = new StandardAccessMode.Builder( true, false, Collections.emptySet(), lookup, DEFAULT_DATABASE_NAME );
    }

    @Test
    void shouldAllowNothingByDefault()
    {
        StandardAccessMode mode = builder.build();

        assertThat( mode.allowsAccess(), equalTo( false ) );

        // TRAVERSE NODES
        assertThat( mode.allowsTraverseAllLabels(), equalTo( false ) );
        assertThat( mode.allowsTraverseNode( ANY_LABEL ), equalTo( false ) );

        assertThat( mode.allowsTraverseNode( A ), equalTo( false ) );
        assertThat( mode.allowsTraverseNode( B ), equalTo( false ) );
        assertThat( mode.allowsTraverseNode( A, B ), equalTo( false ) );

        assertThat( mode.disallowsTraverseLabel( A ), equalTo( false ) );
        assertThat( mode.disallowsTraverseLabel( B ), equalTo( false ) );
        assertThat( mode.disallowsTraverseLabel( ANY_LABEL ), equalTo( false ) );

        assertThat( mode.allowsTraverseAllNodesWithLabel( A ), equalTo( false ) );
        assertThat( mode.allowsTraverseAllNodesWithLabel( B ), equalTo( false ) );
        assertThat( mode.allowsTraverseAllNodesWithLabel( ANY_LABEL ), equalTo( false ) );

        // TRAVERSE RELATIONSHIPS
        assertThat( mode.allowsTraverseAllRelTypes(), equalTo( false ) );
        assertThat( mode.allowsTraverseRelType( ANY_RELATIONSHIP_TYPE ), equalTo( false ) );

        assertThat( mode.allowsTraverseRelType( R1 ), equalTo( false ) );
        assertThat( mode.allowsTraverseRelType( R2 ), equalTo( false ) );

        // READ {PROP} ON NODES
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ), equalTo( false ) );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ), equalTo( false ) );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ), equalTo( false ) );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ), equalTo( false ) );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ), equalTo( false ) );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ), equalTo( false ) );

        // READ {PROP} ON RELATIONSHIPS
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ), equalTo( false ) );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ), equalTo( false ) );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ), equalTo( false ) );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ), equalTo( false ) );

        // SEE PROP IN TOKEN STORE
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ), equalTo( false ) );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ), equalTo( false ) );

        // WRITE
        assertThat( mode.allowsWrites(), equalTo( false ) );

        // NAME MANAGEMENT
        assertThat( mode.allowsTokenCreates( PrivilegeAction.CREATE_LABEL ), equalTo( false ) );
        assertThat( mode.allowsTokenCreates( PrivilegeAction.CREATE_RELTYPE ), equalTo( false ) );
        assertThat( mode.allowsTokenCreates( PrivilegeAction.CREATE_PROPERTYKEY ), equalTo( false ) );

        // INDEX/CONSTRAINT MANAGEMENT
        assertThat( mode.allowsSchemaWrites(), equalTo( false ) );
        assertThat( mode.allowsSchemaWrites( PrivilegeAction.CREATE_INDEX ), equalTo( false ) );
        assertThat( mode.allowsSchemaWrites( PrivilegeAction.DROP_INDEX ), equalTo( false ) );
        assertThat( mode.allowsSchemaWrites( PrivilegeAction.CREATE_CONSTRAINT ), equalTo( false ) );
        assertThat( mode.allowsSchemaWrites( PrivilegeAction.DROP_CONSTRAINT ), equalTo( false ) );
    }

    // TRAVERSE NODES

    @Test
    void shouldAllowTraverseLabel() throws Exception
    {
        // WHEN
        // GRANT TRAVERSE ON GRAPH neo4j NODES A
        var privilege = new ResourcePrivilege( GRANT, TRAVERSE, new Resource.GraphResource(), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsTraverseNode( A ), equalTo( true ) );
        assertThat( mode.allowsTraverseNode( B ), equalTo( false ) );
        assertThat( mode.allowsTraverseNode( A, B ), equalTo( true ) );

        assertThat( mode.disallowsTraverseLabel( A ), equalTo( false ) );
        assertThat( mode.disallowsTraverseLabel( B ), equalTo( false ) );

        assertThat( mode.allowsTraverseAllNodesWithLabel( A ), equalTo( true ) );
        assertThat( mode.allowsTraverseAllNodesWithLabel( B ), equalTo( false ) );

        assertThat( mode.allowsTraverseAllLabels(), equalTo( false ) );
        assertThat( mode.allowsTraverseNode( ANY_LABEL ), equalTo( false ) );
    }

    @Test
    void shouldAllowTraverseAllNodes() throws Exception
    {
        // WHEN
        // GRANT TRAVERSE ON GRAPH neo4j NODES *
        var privilege = new ResourcePrivilege( GRANT, TRAVERSE, new Resource.GraphResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsTraverseNode( A ), equalTo( true ) );
        assertThat( mode.allowsTraverseNode( B ), equalTo( true ) );
        assertThat( mode.allowsTraverseNode( A, B ), equalTo( true ) );

        assertThat( mode.disallowsTraverseLabel( A ), equalTo( false ) );
        assertThat( mode.disallowsTraverseLabel( B ), equalTo( false ) );

        assertThat( mode.allowsTraverseAllNodesWithLabel( A ), equalTo( true ) );
        assertThat( mode.allowsTraverseAllNodesWithLabel( B ), equalTo( true ) );

        assertThat( mode.allowsTraverseAllLabels(), equalTo( true ) );
        assertThat( mode.allowsTraverseNode( ANY_LABEL ), equalTo( true ) );
        assertThat( mode.disallowsTraverseLabel( ANY_LABEL ), equalTo( false ) );
    }

    @Test
    void shouldDenyTraverseNode() throws Exception
    {
        // WHEN
        // GRANT TRAVERSE ON GRAPH neo4j NODES *
        // DENY TRAVERSE ON GRAPH neo4j NODES A
        var privilege1 = new ResourcePrivilege( GRANT, TRAVERSE, new Resource.GraphResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, TRAVERSE, new Resource.GraphResource(), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsTraverseAllLabels(), equalTo( false ) );
        assertThat( mode.allowsTraverseNode( ANY_LABEL), equalTo( false ) );

        assertThat( mode.allowsTraverseNode( A ), equalTo( false ) );
        assertThat( mode.allowsTraverseNode( B ), equalTo( true ) );
        assertThat( mode.allowsTraverseNode( A, B ), equalTo( false ) );

        assertThat( mode.disallowsTraverseLabel( A ), equalTo( true ) );
        assertThat( mode.disallowsTraverseLabel( B ), equalTo( false ) );

        assertThat( mode.allowsTraverseAllNodesWithLabel( A ), equalTo( false ) );
        assertThat( mode.allowsTraverseAllNodesWithLabel( B ), equalTo( false ) );
        assertThat( mode.allowsTraverseAllNodesWithLabel( ANY_LABEL), equalTo( false ) );
    }

    // TRAVERSE RELATIONSHIPS

    @Test
    void shouldAllowTraverseAllRelTypes() throws Exception
    {
        // WHEN
        // GRANT TRAVERSE ON GRAPH neo4j RELATIONSHIPS *
        var privilege = new ResourcePrivilege( GRANT, TRAVERSE, new Resource.GraphResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsTraverseRelType( R1 ), equalTo( true ) );
        assertThat( mode.allowsTraverseRelType( R2 ), equalTo( true ) );
        assertThat( mode.allowsTraverseRelType( ANY_RELATIONSHIP_TYPE ), equalTo( true ) );
        assertThat( mode.allowsTraverseAllRelTypes(), equalTo( true ) );
    }

    @Test
    void shouldAllowTraverseRelType() throws Exception
    {
        // WHEN
        // GRANT TRAVERSE ON GRAPH neo4j RELATIONSHIPS R1
        var privilege = new ResourcePrivilege( GRANT, TRAVERSE, new Resource.GraphResource(), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsTraverseRelType( R1 ), equalTo( true ) );
        assertThat( mode.allowsTraverseRelType( R2 ), equalTo( false ) );
        assertThat( mode.allowsTraverseRelType( ANY_RELATIONSHIP_TYPE ), equalTo( false ) );
        assertThat( mode.allowsTraverseAllRelTypes(), equalTo( false ) );
    }

    @Test
    void shouldDenyTraverseRelType() throws Exception
    {
        // WHEN
        // GRANT TRAVERSE ON GRAPH neo4j RELATIONSHIPS *
        // DENY TRAVERSE ON GRAPH neo4j RELATIONSHIPS R1
        var privilege1 = new ResourcePrivilege( GRANT, TRAVERSE, new Resource.GraphResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, TRAVERSE, new Resource.GraphResource(), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsTraverseRelType( R1 ), equalTo( false ) );
        assertThat( mode.allowsTraverseRelType( R2 ), equalTo( true ) );
        assertThat( mode.allowsTraverseRelType( ANY_RELATIONSHIP_TYPE ), equalTo( false ) );
        assertThat( mode.allowsTraverseAllRelTypes(), equalTo( false ) );
    }

    // READ {PROP} ON NODES

    @Test
    void shouldAllowReadAllPropertiesAllNodes() throws Exception
    {
        // WHEN
        // GRANT READ {*} ON GRAPH neo4j NODES *
        var privilege = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ), equalTo( true ) );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ), equalTo( true ) );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ), equalTo( false ) );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ), equalTo( false ) );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ), equalTo( true ) );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ), equalTo( true ) );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ), equalTo( true ) );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ), equalTo( true ) );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ), equalTo( true ) );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ), equalTo( true ) );

        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP1 ), equalTo( true ) );
        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP2 ), equalTo( true ) );
    }

    @Test
    void shouldAllowReadPropertyAllNodes() throws Exception
    {
        // WHEN
        // GRANT READ {PROP1} ON GRAPH neo4j NODES *
        var privilege = new ResourcePrivilege( GRANT, READ, new Resource.PropertyResource( "PROP1" ), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ), equalTo( true ) );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ), equalTo( false ) );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ), equalTo( false ) );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ), equalTo( false ) );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ), equalTo( true ) );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ), equalTo( false ) );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ), equalTo( true ) );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ), equalTo( false ) );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ), equalTo( true ) );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ), equalTo( false ) );

        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP1 ), equalTo( true ) );
        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP2 ), equalTo( false ) );
    }

    @Test
    void shouldAllowReadPropertySomeNode() throws Exception
    {
        // WHEN
        // GRANT READ {PROP1} ON GRAPH neo4j NODES A
        var privilege = new ResourcePrivilege( GRANT, READ, new Resource.PropertyResource( "PROP1" ), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ), equalTo( false ) );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ), equalTo( false ) );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ), equalTo( false ) );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ), equalTo( false ) );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ), equalTo( true ) );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ), equalTo( false ) );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ), equalTo( false ) );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ), equalTo( false ) );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ), equalTo( true ) );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ), equalTo( false ) );

        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP1 ), equalTo( false ) );
        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP2 ), equalTo( false ) );
    }

    @Test
    void shouldAllowReadPropertyTraverseAllNodes() throws Exception
    {
        // WHEN
        // GRANT TRAVERSE ON GRAPH neo4j NODES *
        // GRANT READ {PROP1} ON GRAPH neo4j NODES A
        var privilege1 = new ResourcePrivilege( GRANT, TRAVERSE, new Resource.GraphResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( GRANT, READ, new Resource.PropertyResource( "PROP1" ), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ), equalTo( false ) );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ), equalTo( false ) );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ), equalTo( false ) );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ), equalTo( false ) );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ), equalTo( true ) );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ), equalTo( false ) );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ), equalTo( false ) );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ), equalTo( false ) );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ), equalTo( true ) );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ), equalTo( false ) );

        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP1 ), equalTo( false ) );
        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP2 ), equalTo( false ) );
    }

    @Test
    void shouldDenyReadPropertyAllNodes() throws Exception
    {
        // WHEN
        // GRANT READ {*} ON GRAPH neo4j NODES *
        // DENY READ {PROP1} ON GRAPH neo4j NODES *
        var privilege1 = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, READ, new Resource.PropertyResource( "PROP1" ), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ), equalTo( false ) );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ), equalTo( true ) );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ), equalTo( true ) );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ), equalTo( false ) );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ), equalTo( false ) );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ), equalTo( true ) );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ), equalTo( false ) );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ), equalTo( true ) );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ), equalTo( false ) );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ), equalTo( true ) );

        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP1 ), equalTo( false ) );
        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP2 ), equalTo( true ) );
    }

    @Test
    void shouldDenyReadPropertySomeNode() throws Exception
    {
        // WHEN
        // GRANT READ {*} ON GRAPH neo4j NODES *
        // DENY READ {PROP1} ON GRAPH neo4j NODES A
        var privilege1 = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, READ, new Resource.PropertyResource( "PROP1" ), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ), equalTo( false ) );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ), equalTo( true ) );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ), equalTo( true ) );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ), equalTo( false ) );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ), equalTo( false ) );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ), equalTo( true ) );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ), equalTo( true ) );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ), equalTo( true ) );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ), equalTo( false ) );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ), equalTo( true ) );

        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP1 ), equalTo( true ) );
        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP2 ), equalTo( true ) );
    }

    @Test
    void shouldDenyReadAllPropertiesSomeNode() throws Exception
    {
        // WHEN
        // GRANT READ {*} ON GRAPH neo4j NODES *
        // DENY READ {*} ON GRAPH neo4j NODES A
        var privilege1 = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, READ, new Resource.AllPropertiesResource(), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ), equalTo( false ) );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ), equalTo( false ) );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ), equalTo( true ) );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ), equalTo( true ) );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ), equalTo( false ) );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ), equalTo( false ) );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ), equalTo( true ) );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ), equalTo( true ) );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ), equalTo( false ) );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ), equalTo( false ) );

        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP1 ), equalTo( true ) );
        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP2 ), equalTo( true ) );
    }

    // READ {PROP} ON RELATIONSHIPS

    @Test
    void shouldAllowReadAllPropertiesAllRelationships() throws Exception
    {
        // WHEN
        // GRANT READ {*} ON GRAPH neo4j RELATIONSHIPS *
        var privilege = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ), equalTo( true ) );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ), equalTo( true ) );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ), equalTo( true ) );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ), equalTo( true ) );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ), equalTo( true ) );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ), equalTo( true ) );
    }

    @Test
    void shouldAllowReadPropertyAllRelationships() throws Exception
    {
        // WHEN
        // GRANT READ {PROP1} ON GRAPH neo4j NODES *
        var privilege = new ResourcePrivilege( GRANT, READ, new Resource.PropertyResource( "PROP1" ), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ), equalTo( true ) );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ), equalTo( false ) );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ), equalTo( true ) );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ), equalTo( false ) );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ), equalTo( true ) );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ), equalTo( false ) );
    }

    @Test
    void shouldAllowReadPropertySomeRelationship() throws Exception
    {
        // WHEN
        // GRANT READ {PROP1} ON GRAPH neo4j RELATIONSHIPS R1
        var privilege = new ResourcePrivilege( GRANT, READ, new Resource.PropertyResource( "PROP1" ), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ), equalTo( false ) );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ), equalTo( false ) );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ), equalTo( true ) );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ), equalTo( false ) );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ), equalTo( false ) );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ), equalTo( false ) );
    }

    @Test
    void shouldAllowReadPropertyTraverseAllRelationships() throws Exception
    {
        // WHEN
        // GRANT TRAVERSE ON GRAPH neo4j RELATIONSHIPS *
        // GRANT READ {PROP1} ON GRAPH neo4j RELATIONSHIPS R1
        var privilege1 = new ResourcePrivilege( GRANT, TRAVERSE, new Resource.GraphResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( GRANT, READ, new Resource.PropertyResource( "PROP1" ), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ), equalTo( false ) );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ), equalTo( false ) );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ), equalTo( true ) );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ), equalTo( false ) );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ), equalTo( false ) );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ), equalTo( false ) );
    }

    @Test
    void shouldDenyReadPropertyAllRelationships() throws Exception
    {
        // WHEN
        // GRANT READ {*} ON GRAPH neo4j RELATIONSHIPS *
        // DENY READ {PROP1} ON GRAPH neo4j RELATIONSHIPS *
        var privilege1 = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, READ, new Resource.PropertyResource( "PROP1" ), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ), equalTo( false ) );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ), equalTo( true ) );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ), equalTo( false ) );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ), equalTo( true ) );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ), equalTo( false ) );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ), equalTo( true ) );
    }

    @Test
    void shouldDenyReadPropertySomeRelationship() throws Exception
    {
        // WHEN
        // GRANT READ {*} ON GRAPH neo4j RELATIONSHIPS *
        // DENY READ {PROP1} ON GRAPH neo4j RELATIONSHIPS A
        var privilege1 = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, READ, new Resource.PropertyResource( "PROP1" ), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ), equalTo( false ) );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ), equalTo( true ) );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ), equalTo( false ) );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ), equalTo( true ) );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ), equalTo( true ) );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ), equalTo( true ) );
    }

    @Test
    void shouldDenyReadAllPropertiesSomeRelationship() throws Exception
    {
        // WHEN
        // GRANT READ {*} ON GRAPH neo4j RELATIONSHIPS *
        // DENY READ {*} ON GRAPH neo4j RELATIONSHIPS A
        var privilege1 = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, READ, new Resource.AllPropertiesResource(), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ), equalTo( false ) );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ), equalTo( false ) );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ), equalTo( false ) );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ), equalTo( false ) );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ), equalTo( true ) );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ), equalTo( true ) );
    }

    // SEE PROP IN TOKEN STORE

    @Test
    void shouldAllowSeeAllPropertyTokensNodes() throws Exception
    {
        // WHEN
        // GRANT READ {*} ON GRAPH neo4j NODES *
        var privilege = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ), equalTo( true ) );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ), equalTo( true ) );
    }

    @Test
    void shouldAllowSeeAllPropertyTokensRelationships() throws Exception
    {
        // WHEN
        // GRANT READ {*} ON GRAPH neo4j RELATIONSHIPS *
        var privilege = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ), equalTo( true ) );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ), equalTo( true ) );
    }

    @Test
    void shouldAllowSeePropertyTokenNode() throws Exception
    {
        // WHEN
        // GRANT READ {PROP1} ON GRAPH neo4j NODES A
        var privilege = new ResourcePrivilege( GRANT, READ, new Resource.PropertyResource( "PROP1" ), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ), equalTo( true ) );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ), equalTo( false ) );
    }

    @Test
    void shouldAllowSeePropertyTokenRelationships() throws Exception
    {
        // WHEN
        // GRANT READ {PROP2} ON GRAPH neo4j RELATIONSHIPS R1
        var privilege = new ResourcePrivilege( GRANT, READ, new Resource.PropertyResource( "PROP2" ), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ), equalTo( false ) );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ), equalTo( true ) );
    }

    @Test
    void shouldDenySeePropertyTokenAllNodes() throws Exception
    {
        // WHEN
        // GRANT READ {*} ON GRAPH neo4j NODES *
        // DENY READ {PROP1} ON GRAPH neo4j NODES *
        var privilege1 = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, READ, new Resource.PropertyResource( "PROP1" ), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ), equalTo( false ) );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ), equalTo( true ) );
    }

    @Test
    void shouldDenySeePropertyTokenAllRelationships() throws Exception
    {
        // WHEN
        // GRANT READ {*} ON GRAPH neo4j RELATIONSHIPS *
        // DENY READ {PROP2} ON GRAPH neo4j RELATIONSHIPS *
        var privilege1 = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, READ, new Resource.PropertyResource( "PROP2" ), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ), equalTo( true ) );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ), equalTo( false ) );
    }

    @Test
    void shouldDenySeePropertyTokenSpecificNode() throws Exception
    {
        // WHEN
        // GRANT READ {*} ON GRAPH neo4j NODES A
        // DENY READ {PROP1} ON GRAPH neo4j NODES B
        var privilege1 = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, READ, new Resource.PropertyResource( "PROP1" ), new LabelSegment( "B" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ), equalTo( true ) );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ), equalTo( true ) );
    }

    @Test
    void shouldDenySeePropertyTokenSpecificRelationship() throws Exception
    {
        // WHEN
        // GRANT READ {*} ON GRAPH neo4j RELATIONSHIPS R1
        // DENY READ {PROP1} ON GRAPH neo4j RELATIONSHIPS R2
        var privilege1 = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, READ, new Resource.PropertyResource( "PROP1" ), new RelTypeSegment( "R2" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ), equalTo( true ) );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ), equalTo( true ) );
    }

    @Test
    void shouldAllowSeePropertyTokenGrantNodesDenyRelationships() throws Exception
    {
        // WHEN
        // GRANT READ {*} ON GRAPH neo4j NODES *
        // DENY READ {*} ON GRAPH neo4j RELATIONSHIPS *
        var privilege1 = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, READ, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ), equalTo( true ) );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ), equalTo( true ) );
    }

    @Test
    void shouldAllowSeePropertyTokenGrantRelationshipsDenyNodes() throws Exception
    {
        // WHEN
        // GRANT READ {*} ON GRAPH neo4j RELATIONSHIPS *
        // DENY READ {*} ON GRAPH neo4j NODES *
        var privilege1 = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, READ, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ), equalTo( true ) );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ), equalTo( true ) );
    }

    // WRITE

    @Test
    void shouldAllowWrites() throws Exception
    {
        // WHEN
        // GRANT WRITE ON GRAPH neo4j ELEMENTS *
        var privilege1 = new ResourcePrivilege( GRANT, WRITE, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( GRANT, WRITE, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsWrites(), equalTo( true ) );
    }
}
