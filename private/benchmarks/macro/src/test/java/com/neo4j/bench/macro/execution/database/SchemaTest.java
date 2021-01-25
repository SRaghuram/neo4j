/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.database;

import com.google.common.collect.Lists;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.macro.execution.database.Schema.IndexSchemaEntry;
import com.neo4j.bench.macro.execution.database.Schema.NodeExistsSchemaEntry;
import com.neo4j.bench.macro.execution.database.Schema.NodeKeySchemaEntry;
import com.neo4j.bench.macro.execution.database.Schema.NodeUniqueSchemaEntry;
import com.neo4j.bench.macro.execution.database.Schema.RelationshipExistsSchemaEntry;
import com.neo4j.bench.macro.execution.database.Schema.SchemaEntry;
import com.neo4j.bench.macro.workload.WorkloadConfigError;
import com.neo4j.bench.macro.workload.WorkloadConfigException;
import org.junit.jupiter.api.Test;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class SchemaTest
{
    private static final String NODE_KEY = "CONSTRAINT ON (n:Label) ASSERT (n.prop1, n.prop2) IS NODE KEY";
    private static final String NODE_UNIQUE = "CONSTRAINT ON (n:Label) ASSERT p.prop1 IS UNIQUE";
    private static final String NODE_EXISTS = "CONSTRAINT ON (n:Label) ASSERT exists(p.prop1)";
    private static final String RELATIONSHIP_EXISTS = "CONSTRAINT ON ()-[r:TYPE]-() ASSERT exists(r.prop1)";
    private static final String INDEX = "INDEX ON :Label(prop1)";
    private static final String COMPOSITE_INDEX = "INDEX ON :Label(prop1,prop2)";

    private static final String UNRECOGNIZED = "unrecognized";

    private static final Label LABEL = Label.label( "Label" );
    private static final RelationshipType TYPE = RelationshipType.withName( "TYPE" );
    private static final String PROP1 = "prop1";
    private static final String PROP2 = "prop2";

    @Test
    public void shouldParseNodeKey()
    {
        assertThat( NodeKeySchemaEntry.isNodeKey( NODE_KEY ), is( true ) );

        assertThat( NodeKeySchemaEntry.isNodeKey( NODE_UNIQUE ), is( false ) );
        assertThat( NodeKeySchemaEntry.isNodeKey( NODE_EXISTS ), is( false ) );
        assertThat( NodeKeySchemaEntry.isNodeKey( RELATIONSHIP_EXISTS ), is( false ) );
        assertThat( NodeKeySchemaEntry.isNodeKey( INDEX ), is( false ) );
        assertThat( NodeKeySchemaEntry.isNodeKey( COMPOSITE_INDEX ), is( false ) );

        SchemaEntry entry = NodeKeySchemaEntry.parse( NODE_KEY );
        assertThat( entry, instanceOf( NodeKeySchemaEntry.class ) );

        assertThat( ((NodeKeySchemaEntry) entry).label(), equalTo( LABEL ) );
        assertThat( ((NodeKeySchemaEntry) entry).properties(), equalTo( Lists.newArrayList( PROP1, PROP2 ) ) );

        assertThat( NodeKeySchemaEntry.isNodeKey( entry.description() ), is( true ) );
        assertThat( NodeKeySchemaEntry.parse( entry.description() ), instanceOf( NodeKeySchemaEntry.class ) );
    }

    @Test
    public void shouldParseNodeUnique()
    {
        assertThat( NodeUniqueSchemaEntry.isNodeUnique( NODE_UNIQUE ), is( true ) );

        assertThat( NodeUniqueSchemaEntry.isNodeUnique( NODE_KEY ), is( false ) );
        assertThat( NodeUniqueSchemaEntry.isNodeUnique( NODE_EXISTS ), is( false ) );
        assertThat( NodeUniqueSchemaEntry.isNodeUnique( RELATIONSHIP_EXISTS ), is( false ) );
        assertThat( NodeUniqueSchemaEntry.isNodeUnique( INDEX ), is( false ) );
        assertThat( NodeUniqueSchemaEntry.isNodeUnique( COMPOSITE_INDEX ), is( false ) );

        SchemaEntry entry = NodeUniqueSchemaEntry.parse( NODE_UNIQUE );
        assertThat( entry, instanceOf( NodeUniqueSchemaEntry.class ) );

        assertThat( ((NodeUniqueSchemaEntry) entry).label(), equalTo( LABEL ) );
        assertThat( ((NodeUniqueSchemaEntry) entry).property(), equalTo( PROP1 ) );

        assertThat( NodeUniqueSchemaEntry.isNodeUnique( entry.description() ), is( true ) );
        assertThat( NodeUniqueSchemaEntry.parse( entry.description() ), instanceOf( NodeUniqueSchemaEntry.class ) );
    }

    @Test
    public void shouldParseNodeExists()
    {
        assertThat( NodeExistsSchemaEntry.isNodeExists( NODE_EXISTS ), is( true ) );

        assertThat( NodeExistsSchemaEntry.isNodeExists( NODE_UNIQUE ), is( false ) );
        assertThat( NodeExistsSchemaEntry.isNodeExists( NODE_KEY ), is( false ) );
        assertThat( NodeExistsSchemaEntry.isNodeExists( RELATIONSHIP_EXISTS ), is( false ) );
        assertThat( NodeExistsSchemaEntry.isNodeExists( INDEX ), is( false ) );
        assertThat( NodeExistsSchemaEntry.isNodeExists( COMPOSITE_INDEX ), is( false ) );

        SchemaEntry entry = NodeExistsSchemaEntry.parse( NODE_EXISTS );
        assertThat( entry, instanceOf( NodeExistsSchemaEntry.class ) );

        assertThat( ((NodeExistsSchemaEntry) entry).label(), equalTo( LABEL ) );
        assertThat( ((NodeExistsSchemaEntry) entry).property(), equalTo( PROP1 ) );

        assertThat( NodeExistsSchemaEntry.isNodeExists( entry.description() ), is( true ) );
        assertThat( NodeExistsSchemaEntry.parse( entry.description() ), instanceOf( NodeExistsSchemaEntry.class ) );
    }

    @Test
    public void shouldParseRelationshipExists()
    {
        assertThat( RelationshipExistsSchemaEntry.isRelationshipExists( RELATIONSHIP_EXISTS ), is( true ) );

        assertThat( RelationshipExistsSchemaEntry.isRelationshipExists( NODE_EXISTS ), is( false ) );
        assertThat( RelationshipExistsSchemaEntry.isRelationshipExists( NODE_UNIQUE ), is( false ) );
        assertThat( RelationshipExistsSchemaEntry.isRelationshipExists( NODE_KEY ), is( false ) );
        assertThat( RelationshipExistsSchemaEntry.isRelationshipExists( INDEX ), is( false ) );
        assertThat( RelationshipExistsSchemaEntry.isRelationshipExists( COMPOSITE_INDEX ), is( false ) );

        SchemaEntry entry = RelationshipExistsSchemaEntry.parse( RELATIONSHIP_EXISTS );
        assertThat( entry, instanceOf( RelationshipExistsSchemaEntry.class ) );

        assertThat( ((RelationshipExistsSchemaEntry) entry).type(), equalTo( TYPE ) );
        assertThat( ((RelationshipExistsSchemaEntry) entry).property(), equalTo( PROP1 ) );

        assertThat( RelationshipExistsSchemaEntry.isRelationshipExists( entry.description() ), is( true ) );
        assertThat( RelationshipExistsSchemaEntry.parse( entry.description() ), instanceOf( RelationshipExistsSchemaEntry.class ) );
    }

    @Test
    public void shouldParseIndex()
    {
        assertThat( IndexSchemaEntry.isIndex( INDEX ), is( true ) );
        assertThat( IndexSchemaEntry.isIndex( COMPOSITE_INDEX ), is( true ) );

        assertThat( IndexSchemaEntry.isIndex( RELATIONSHIP_EXISTS ), is( false ) );
        assertThat( IndexSchemaEntry.isIndex( NODE_EXISTS ), is( false ) );
        assertThat( IndexSchemaEntry.isIndex( NODE_UNIQUE ), is( false ) );
        assertThat( IndexSchemaEntry.isIndex( NODE_KEY ), is( false ) );

        SchemaEntry entry = IndexSchemaEntry.parse( INDEX );
        assertThat( entry, instanceOf( IndexSchemaEntry.class ) );

        assertThat( ((IndexSchemaEntry) entry).label(), equalTo( LABEL ) );
        assertThat( ((IndexSchemaEntry) entry).properties(), equalTo( Lists.newArrayList( PROP1 ) ) );

        assertThat( IndexSchemaEntry.isIndex( entry.description() ), is( true ) );
        assertThat( IndexSchemaEntry.parse( entry.description() ), instanceOf( IndexSchemaEntry.class ) );

        entry = IndexSchemaEntry.parse( COMPOSITE_INDEX );
        assertThat( entry, instanceOf( IndexSchemaEntry.class ) );

        assertThat( ((IndexSchemaEntry) entry).label(), equalTo( LABEL ) );
        assertThat( ((IndexSchemaEntry) entry).properties(), equalTo( Lists.newArrayList( PROP1, PROP2 ) ) );

        assertThat( IndexSchemaEntry.isIndex( entry.description() ), is( true ) );
        assertThat( IndexSchemaEntry.parse( entry.description() ), instanceOf( IndexSchemaEntry.class ) );
    }

    @Test
    public void shouldParseEntry()
    {
        assertThat( assertParseEntry( NODE_KEY ), instanceOf( NodeKeySchemaEntry.class ) );
        assertThat( assertParseEntry( NODE_UNIQUE ), instanceOf( NodeUniqueSchemaEntry.class ) );
        assertThat( assertParseEntry( NODE_EXISTS ), instanceOf( NodeExistsSchemaEntry.class ) );
        assertThat( assertParseEntry( RELATIONSHIP_EXISTS ), instanceOf( RelationshipExistsSchemaEntry.class ) );
        assertThat( assertParseEntry( INDEX ), instanceOf( IndexSchemaEntry.class ) );
        assertThat( assertParseEntry( COMPOSITE_INDEX ), instanceOf( IndexSchemaEntry.class ) );

        WorkloadConfigException workloadConfigException = BenchmarkUtil.assertException( WorkloadConfigException.class,
                                                                                         () -> SchemaEntry.parse( UNRECOGNIZED ) );
        assertThat( workloadConfigException.error(), equalTo( WorkloadConfigError.INVALID_SCHEMA_ENTRY ) );
    }

    private SchemaEntry assertParseEntry( String value )
    {
        SchemaEntry entry = SchemaEntry.parse( value );
        assertThat( entry, equalTo( SchemaEntry.parse( entry.description() ) ) );
        return entry;
    }
}
