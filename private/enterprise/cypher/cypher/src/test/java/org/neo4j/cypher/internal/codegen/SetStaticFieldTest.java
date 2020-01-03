/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.codegen;

import org.junit.jupiter.api.Test;

import org.neo4j.cypher.internal.runtime.compiled.codegen.setStaticField;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SetStaticFieldTest
{
    @Test
    void shouldAssignFields()
    {
        // when
        setStaticField.apply( Apa.class, "X", "HELLO WORLD!" );

        // then
        assertEquals( "HELLO WORLD!", Apa.X );
    }

    public static class Apa
    {
        public static String X;
    }
}
