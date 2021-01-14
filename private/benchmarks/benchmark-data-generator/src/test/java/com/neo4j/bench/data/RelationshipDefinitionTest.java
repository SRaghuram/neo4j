/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.data;

import org.junit.jupiter.api.Test;

import org.neo4j.graphdb.RelationshipType;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNot.not;

public class RelationshipDefinitionTest
{
    @Test
    public void shouldParseCorrectlyForSingleDefinitions()
    {
        String definitionsString = "(R1:1)";
        RelationshipDefinition[] actualDefinitions = RelationshipDefinition.from( definitionsString );
        RelationshipDefinition[] correctDefinitions = new RelationshipDefinition[]{
                new RelationshipDefinition( RelationshipType.withName( "R1" ), 1 )
        };
        RelationshipDefinition[] incorrectDefinitions1 = new RelationshipDefinition[]{
                new RelationshipDefinition( RelationshipType.withName( "R1" ), 2 )
        };
        RelationshipDefinition[] incorrectDefinitions2 = new RelationshipDefinition[]{
                new RelationshipDefinition( RelationshipType.withName( "R" ), 1 )
        };
        assertThat( actualDefinitions, equalTo( correctDefinitions ) );
        assertThat( actualDefinitions, not( equalTo( incorrectDefinitions1 ) ) );
        assertThat( actualDefinitions, not( equalTo( incorrectDefinitions2 ) ) );
    }

    @Test
    public void shouldParseCorrectlyForMultipleDefinitions()
    {
        String definitionsString = "(R1:1),(R2:2),(R3:3),(R400:400),(R5000:5000)";
        RelationshipDefinition[] actualDefinitions = RelationshipDefinition.from( definitionsString );
        RelationshipDefinition[] correctDefinitions = new RelationshipDefinition[]{
                new RelationshipDefinition( RelationshipType.withName( "R1" ), 1 ),
                new RelationshipDefinition( RelationshipType.withName( "R2" ), 2 ),
                new RelationshipDefinition( RelationshipType.withName( "R3" ), 3 ),
                new RelationshipDefinition( RelationshipType.withName( "R400" ), 400 ),
                new RelationshipDefinition( RelationshipType.withName( "R5000" ), 5000 )
        };
        RelationshipDefinition[] incorrectDefinitions1 = new RelationshipDefinition[]{
                new RelationshipDefinition( RelationshipType.withName( "R1" ), 2 ),
                new RelationshipDefinition( RelationshipType.withName( "R2" ), 2 ),
                new RelationshipDefinition( RelationshipType.withName( "R3" ), 3 ),
                new RelationshipDefinition( RelationshipType.withName( "R400" ), 400 ),
                new RelationshipDefinition( RelationshipType.withName( "R5000" ), 5000 )
        };
        RelationshipDefinition[] incorrectDefinitions2 = new RelationshipDefinition[]{
                new RelationshipDefinition( RelationshipType.withName( "R" ), 1 ),
                new RelationshipDefinition( RelationshipType.withName( "R2" ), 2 ),
                new RelationshipDefinition( RelationshipType.withName( "R3" ), 3 ),
                new RelationshipDefinition( RelationshipType.withName( "R400" ), 400 ),
                new RelationshipDefinition( RelationshipType.withName( "R5000" ), 5000 )
        };
        assertThat( actualDefinitions, equalTo( correctDefinitions ) );
        assertThat( actualDefinitions, not( equalTo( incorrectDefinitions1 ) ) );
        assertThat( actualDefinitions, not( equalTo( incorrectDefinitions2 ) ) );
    }
}
