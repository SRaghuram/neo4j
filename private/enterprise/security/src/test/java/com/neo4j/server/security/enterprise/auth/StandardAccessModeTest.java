/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.MATCH;
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

        builder = new StandardAccessMode.Builder( true, false, Collections.emptySet(), lookup, DEFAULT_DATABASE_NAME, DEFAULT_DATABASE_NAME );
    }

    @Test
    void shouldAllowNothingByDefault()
    {
        StandardAccessMode mode = builder.build();

        assertThat( mode.allowsAccess() ).isEqualTo( false );

        // TRAVERSE NODES
        assertThat( mode.allowsTraverseAllLabels() ).isEqualTo( false );
        assertThat( mode.allowsTraverseNode( ANY_LABEL ) ).isEqualTo( false );

        assertThat( mode.allowsTraverseNode( A ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseNode( B ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseNode( A, B ) ).isEqualTo( false );

        assertThat( mode.disallowsTraverseLabel( A ) ).isEqualTo( false );
        assertThat( mode.disallowsTraverseLabel( B ) ).isEqualTo( false );
        assertThat( mode.disallowsTraverseLabel( ANY_LABEL ) ).isEqualTo( false );

        assertThat( mode.allowsTraverseAllNodesWithLabel( A ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseAllNodesWithLabel( B ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseAllNodesWithLabel( ANY_LABEL ) ).isEqualTo( false );

        // TRAVERSE RELATIONSHIPS
        assertThat( mode.allowsTraverseAllRelTypes() ).isEqualTo( false );
        assertThat( mode.allowsTraverseRelType( ANY_RELATIONSHIP_TYPE ) ).isEqualTo( false );

        assertThat( mode.allowsTraverseRelType( R1 ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseRelType( R2 ) ).isEqualTo( false );

        // READ {PROP} ON NODES
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ) ).isEqualTo( false );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ) ).isEqualTo( false );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( false );

        // READ {PROP} ON RELATIONSHIPS
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ) ).isEqualTo( false );

        // SEE PROP IN TOKEN STORE
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( false );

        // WRITE
        assertThat( mode.allowsWrites() ).isEqualTo( false );

        // NAME MANAGEMENT
        assertThat( mode.allowsTokenCreates( PrivilegeAction.CREATE_LABEL ) ).isEqualTo( false );
        assertThat( mode.allowsTokenCreates( PrivilegeAction.CREATE_RELTYPE ) ).isEqualTo( false );
        assertThat( mode.allowsTokenCreates( PrivilegeAction.CREATE_PROPERTYKEY ) ).isEqualTo( false );

        // INDEX/CONSTRAINT MANAGEMENT
        assertThat( mode.allowsSchemaWrites() ).isEqualTo( false );
        assertThat( mode.allowsSchemaWrites( PrivilegeAction.CREATE_INDEX ) ).isEqualTo( false );
        assertThat( mode.allowsSchemaWrites( PrivilegeAction.DROP_INDEX ) ).isEqualTo( false );
        assertThat( mode.allowsSchemaWrites( PrivilegeAction.CREATE_CONSTRAINT ) ).isEqualTo( false );
        assertThat( mode.allowsSchemaWrites( PrivilegeAction.DROP_CONSTRAINT ) ).isEqualTo( false );
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
        assertThat( mode.allowsTraverseNode( A ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseNode( B ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseNode( A, B ) ).isEqualTo( true );

        assertThat( mode.disallowsTraverseLabel( A ) ).isEqualTo( false );
        assertThat( mode.disallowsTraverseLabel( B ) ).isEqualTo( false );

        assertThat( mode.allowsTraverseAllNodesWithLabel( A ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseAllNodesWithLabel( B ) ).isEqualTo( false );

        assertThat( mode.allowsTraverseAllLabels() ).isEqualTo( false );
        assertThat( mode.allowsTraverseNode( ANY_LABEL ) ).isEqualTo( false );
    }

    @Test
    void shouldAllowTraverseAllNodes() throws Exception
    {
        // WHEN
        // GRANT TRAVERSE ON GRAPH neo4j NODES *
        var privilege = new ResourcePrivilege( GRANT, TRAVERSE, new Resource.GraphResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsTraverseNode( A ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseNode( B ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseNode( A, B ) ).isEqualTo( true );

        assertThat( mode.disallowsTraverseLabel( A ) ).isEqualTo( false );
        assertThat( mode.disallowsTraverseLabel( B ) ).isEqualTo( false );

        assertThat( mode.allowsTraverseAllNodesWithLabel( A ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseAllNodesWithLabel( B ) ).isEqualTo( true );

        assertThat( mode.allowsTraverseAllLabels() ).isEqualTo( true );
        assertThat( mode.allowsTraverseNode( ANY_LABEL ) ).isEqualTo( true );
        assertThat( mode.disallowsTraverseLabel( ANY_LABEL ) ).isEqualTo( false );
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
        assertThat( mode.allowsTraverseAllLabels() ).isEqualTo( false );
        assertThat( mode.allowsTraverseNode( ANY_LABEL ) ).isEqualTo( false );

        assertThat( mode.allowsTraverseNode( A ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseNode( B ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseNode( A, B ) ).isEqualTo( false );

        assertThat( mode.disallowsTraverseLabel( A ) ).isEqualTo( true );
        assertThat( mode.disallowsTraverseLabel( B ) ).isEqualTo( false );

        assertThat( mode.allowsTraverseAllNodesWithLabel( A ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseAllNodesWithLabel( B ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseAllNodesWithLabel( ANY_LABEL ) ).isEqualTo( false );
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
        assertThat( mode.allowsTraverseRelType( R1 ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseRelType( R2 ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseRelType( ANY_RELATIONSHIP_TYPE ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseAllRelTypes() ).isEqualTo( true );
    }

    @Test
    void shouldAllowTraverseRelType() throws Exception
    {
        // WHEN
        // GRANT TRAVERSE ON GRAPH neo4j RELATIONSHIPS R1
        var privilege = new ResourcePrivilege( GRANT, TRAVERSE, new Resource.GraphResource(), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsTraverseRelType( R1 ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseRelType( R2 ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseRelType( ANY_RELATIONSHIP_TYPE ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseAllRelTypes() ).isEqualTo( false );
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
        assertThat( mode.allowsTraverseRelType( R1 ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseRelType( R2 ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseRelType( ANY_RELATIONSHIP_TYPE ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseAllRelTypes() ).isEqualTo( false );
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
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ) ).isEqualTo( true );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ) ).isEqualTo( false );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( true );

        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldAllowReadPropertyAllNodes() throws Exception
    {
        // WHEN
        // GRANT READ {PROP1} ON GRAPH neo4j NODES *
        var privilege = new ResourcePrivilege( GRANT, READ, new Resource.PropertyResource( "PROP1" ), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ) ).isEqualTo( false );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ) ).isEqualTo( false );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldAllowReadPropertySomeNode() throws Exception
    {
        // WHEN
        // GRANT READ {PROP1} ON GRAPH neo4j NODES A
        var privilege = new ResourcePrivilege( GRANT, READ, new Resource.PropertyResource( "PROP1" ), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ) ).isEqualTo( false );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ) ).isEqualTo( false );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP2 ) ).isEqualTo( false );
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
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ) ).isEqualTo( false );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ) ).isEqualTo( false );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP2 ) ).isEqualTo( false );
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
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ) ).isEqualTo( true );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ) ).isEqualTo( true );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( true );

        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP2 ) ).isEqualTo( true );
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
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ) ).isEqualTo( true );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ) ).isEqualTo( true );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( true );

        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP2 ) ).isEqualTo( true );
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
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ) ).isEqualTo( false );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ) ).isEqualTo( true );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ) ).isEqualTo( true );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP2 ) ).isEqualTo( true );
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
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ) ).isEqualTo( true );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldAllowReadPropertyAllRelationships() throws Exception
    {
        // WHEN
        // GRANT READ {PROP1} ON GRAPH neo4j NODES *
        var privilege = new ResourcePrivilege( GRANT, READ, new Resource.PropertyResource( "PROP1" ), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldAllowReadPropertySomeRelationship() throws Exception
    {
        // WHEN
        // GRANT READ {PROP1} ON GRAPH neo4j RELATIONSHIPS R1
        var privilege = new ResourcePrivilege( GRANT, READ, new Resource.PropertyResource( "PROP1" ), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ) ).isEqualTo( false );
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
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ) ).isEqualTo( false );
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
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ) ).isEqualTo( true );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ) ).isEqualTo( true );
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
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ) ).isEqualTo( true );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ) ).isEqualTo( true );
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
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ) ).isEqualTo( true );
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
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldAllowSeeAllPropertyTokensRelationships() throws Exception
    {
        // WHEN
        // GRANT READ {*} ON GRAPH neo4j RELATIONSHIPS *
        var privilege = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldAllowSeePropertyTokenNode() throws Exception
    {
        // WHEN
        // GRANT READ {PROP1} ON GRAPH neo4j NODES A
        var privilege = new ResourcePrivilege( GRANT, READ, new Resource.PropertyResource( "PROP1" ), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldAllowSeePropertyTokenRelationships() throws Exception
    {
        // WHEN
        // GRANT READ {PROP2} ON GRAPH neo4j RELATIONSHIPS R1
        var privilege = new ResourcePrivilege( GRANT, READ, new Resource.PropertyResource( "PROP2" ), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( true );
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
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( true );
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
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( false );
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
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( true );
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
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( true );
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
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( true );
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
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( true );
    }

    // MATCH: TRAVERSE NODES

    @Test
    void shouldAllowTraverseLabelWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j NODES A
        var privilege = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsTraverseNode( A ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseNode( B ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseNode( A, B ) ).isEqualTo( true );

        assertThat( mode.disallowsTraverseLabel( A ) ).isEqualTo( false );
        assertThat( mode.disallowsTraverseLabel( B ) ).isEqualTo( false );

        assertThat( mode.allowsTraverseAllNodesWithLabel( A ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseAllNodesWithLabel( B ) ).isEqualTo( false );

        assertThat( mode.allowsTraverseAllLabels() ).isEqualTo( false );
        assertThat( mode.allowsTraverseNode( ANY_LABEL ) ).isEqualTo( false );
    }

    @Test
    void shouldAllowTraverseAllNodesWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j NODES *
        var privilege = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsTraverseNode( A ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseNode( B ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseNode( A, B ) ).isEqualTo( true );

        assertThat( mode.disallowsTraverseLabel( A ) ).isEqualTo( false );
        assertThat( mode.disallowsTraverseLabel( B ) ).isEqualTo( false );

        assertThat( mode.allowsTraverseAllNodesWithLabel( A ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseAllNodesWithLabel( B ) ).isEqualTo( true );

        assertThat( mode.allowsTraverseAllLabels() ).isEqualTo( true );
        assertThat( mode.allowsTraverseNode( ANY_LABEL ) ).isEqualTo( true );
        assertThat( mode.disallowsTraverseLabel( ANY_LABEL ) ).isEqualTo( false );
    }

    @Test
    void shouldDenyTraverseNodeWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j NODES *
        // DENY MATCH {*} ON GRAPH neo4j NODES A
        var privilege1 = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, MATCH, new Resource.AllPropertiesResource(), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsTraverseAllLabels() ).isEqualTo( false );
        assertThat( mode.allowsTraverseNode( ANY_LABEL ) ).isEqualTo( false );

        assertThat( mode.allowsTraverseNode( A ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseNode( B ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseNode( A, B ) ).isEqualTo( false );

        assertThat( mode.disallowsTraverseLabel( A ) ).isEqualTo( true );
        assertThat( mode.disallowsTraverseLabel( B ) ).isEqualTo( false );

        assertThat( mode.allowsTraverseAllNodesWithLabel( A ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseAllNodesWithLabel( B ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseAllNodesWithLabel( ANY_LABEL ) ).isEqualTo( false );
    }

    // MATCH: TRAVERSE RELATIONSHIPS

    @Test
    void shouldAllowTraverseAllRelTypesWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j RELATIONSHIPS *
        var privilege = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsTraverseRelType( R1 ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseRelType( R2 ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseRelType( ANY_RELATIONSHIP_TYPE ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseAllRelTypes() ).isEqualTo( true );
    }

    @Test
    void shouldAllowTraverseRelTypeWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j RELATIONSHIPS R1
        var privilege = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsTraverseRelType( R1 ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseRelType( R2 ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseRelType( ANY_RELATIONSHIP_TYPE ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseAllRelTypes() ).isEqualTo( false );
    }

    @Test
    void shouldDenyTraverseRelTypeWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j RELATIONSHIPS *
        // DENY MATCH {*} ON GRAPH neo4j RELATIONSHIPS R1
        var privilege1 = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, MATCH, new Resource.AllPropertiesResource(), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsTraverseRelType( R1 ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseRelType( R2 ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseRelType( ANY_RELATIONSHIP_TYPE ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseAllRelTypes() ).isEqualTo( false );
    }

    // MATCH {PROP} ON NODES

    @Test
    void shouldAllowReadAllPropertiesAllNodesWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j NODES *
        var privilege = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ) ).isEqualTo( true );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ) ).isEqualTo( false );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( true );

        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldAllowReadPropertyAllNodesWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {PROP1} ON GRAPH neo4j NODES *
        var privilege = new ResourcePrivilege( GRANT, MATCH, new Resource.PropertyResource( "PROP1" ), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ) ).isEqualTo( false );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ) ).isEqualTo( false );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldAllowReadPropertySomeNodeWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {PROP1} ON GRAPH neo4j NODES A
        var privilege = new ResourcePrivilege( GRANT, MATCH, new Resource.PropertyResource( "PROP1" ), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ) ).isEqualTo( false );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ) ).isEqualTo( false );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldAllowReadPropertyTraverseAllNodesWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT TRAVERSE ON GRAPH neo4j NODES *
        // GRANT MATCH {PROP1} ON GRAPH neo4j NODES A
        var privilege1 = new ResourcePrivilege( GRANT, TRAVERSE, new Resource.GraphResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( GRANT, MATCH, new Resource.PropertyResource( "PROP1" ), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ) ).isEqualTo( false );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ) ).isEqualTo( false );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldDenyReadPropertyAllNodesWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j NODES *
        // DENY MATCH {PROP1} ON GRAPH neo4j NODES *
        var privilege1 = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, MATCH, new Resource.PropertyResource( "PROP1" ), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ) ).isEqualTo( true );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ) ).isEqualTo( true );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( true );

        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldDenyReadPropertySomeNodeWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j NODES *
        // DENY MATCH {PROP1} ON GRAPH neo4j NODES A
        var privilege1 = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, MATCH, new Resource.PropertyResource( "PROP1" ), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ) ).isEqualTo( true );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ) ).isEqualTo( true );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( true );

        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldDenyReadAllPropertiesSomeNodeWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j NODES *
        // DENY MATCH {*} ON GRAPH neo4j NODES A
        var privilege1 = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, MATCH, new Resource.AllPropertiesResource(), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ) ).isEqualTo( false );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ) ).isEqualTo( true );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ) ).isEqualTo( true );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP2 ) ).isEqualTo( true );
    }

    // MATCH {PROP} ON RELATIONSHIPS

    @Test
    void shouldAllowReadAllPropertiesAllRelationshipsWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j RELATIONSHIPS *
        var privilege = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ) ).isEqualTo( true );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldAllowReadPropertyAllRelationshipsWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {PROP1} ON GRAPH neo4j NODES *
        var privilege = new ResourcePrivilege( GRANT, MATCH, new Resource.PropertyResource( "PROP1" ), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldAllowReadPropertySomeRelationshipWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {PROP1} ON GRAPH neo4j RELATIONSHIPS R1
        var privilege = new ResourcePrivilege( GRANT, MATCH, new Resource.PropertyResource( "PROP1" ), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldAllowReadPropertyTraverseAllRelationshipsWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT TRAVERSE ON GRAPH neo4j RELATIONSHIPS *
        // GRANT MATCH {PROP1} ON GRAPH neo4j RELATIONSHIPS R1
        var privilege1 = new ResourcePrivilege( GRANT, TRAVERSE, new Resource.GraphResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( GRANT, MATCH, new Resource.PropertyResource( "PROP1" ), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldDenyReadPropertyAllRelationshipsWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j RELATIONSHIPS *
        // DENY MATCH {PROP1} ON GRAPH neo4j RELATIONSHIPS *
        var privilege1 = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, MATCH, new Resource.PropertyResource( "PROP1" ), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ) ).isEqualTo( true );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldDenyReadPropertySomeRelationshipWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j RELATIONSHIPS *
        // DENY MATCH {PROP1} ON GRAPH neo4j RELATIONSHIPS A
        var privilege1 = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, MATCH, new Resource.PropertyResource( "PROP1" ), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ) ).isEqualTo( true );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldDenyReadAllPropertiesSomeRelationshipWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j RELATIONSHIPS *
        // DENY MATCH {*} ON GRAPH neo4j RELATIONSHIPS A
        var privilege1 = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, MATCH, new Resource.AllPropertiesResource(), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ) ).isEqualTo( true );
    }

    // MATCH: SEE PROP IN TOKEN STORE

    @Test
    void shouldAllowSeeAllPropertyTokensNodesWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j NODES *
        var privilege = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldAllowSeeAllPropertyTokensRelationshipsWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j RELATIONSHIPS *
        var privilege = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldAllowSeePropertyTokenNodeWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {PROP1} ON GRAPH neo4j NODES A
        var privilege = new ResourcePrivilege( GRANT, MATCH, new Resource.PropertyResource( "PROP1" ), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldAllowSeePropertyTokenRelationshipsWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {PROP2} ON GRAPH neo4j RELATIONSHIPS R1
        var privilege = new ResourcePrivilege( GRANT, MATCH, new Resource.PropertyResource( "PROP2" ), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldDenySeePropertyTokenAllNodesWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j NODES *
        // DENY MATCH {PROP1} ON GRAPH neo4j NODES *
        var privilege1 = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, MATCH, new Resource.PropertyResource( "PROP1" ), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldDenySeePropertyTokenAllRelationshipsWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j RELATIONSHIPS *
        // DENY MATCH {PROP2} ON GRAPH neo4j RELATIONSHIPS *
        var privilege1 = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, MATCH, new Resource.PropertyResource( "PROP2" ), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldDenySeePropertyTokenSpecificNodeWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j NODES A
        // DENY MATCH {PROP1} ON GRAPH neo4j NODES B
        var privilege1 = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, MATCH, new Resource.PropertyResource( "PROP1" ), new LabelSegment( "B" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldDenySeePropertyTokenSpecificRelationshipWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j RELATIONSHIPS R1
        // DENY MATCH {PROP1} ON GRAPH neo4j RELATIONSHIPS R2
        var privilege1 = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, MATCH, new Resource.PropertyResource( "PROP1" ), new RelTypeSegment( "R2" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldAllowSeePropertyTokenGrantNodesDenyRelationshipsWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j NODES *
        // DENY MATCH {*} ON GRAPH neo4j RELATIONSHIPS *
        var privilege1 = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, MATCH, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldAllowSeePropertyTokenGrantRelationshipsDenyNodesWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j RELATIONSHIPS *
        // DENY MATCH {*} ON GRAPH neo4j NODES *
        var privilege1 = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, MATCH, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( true );
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
        assertThat( mode.allowsWrites() ).isEqualTo( true );
    }
}
