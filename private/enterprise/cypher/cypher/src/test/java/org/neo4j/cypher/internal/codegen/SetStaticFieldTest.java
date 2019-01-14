/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.codegen;

import org.junit.Test;

import org.neo4j.cypher.internal.runtime.compiled.codegen.setStaticField;

import static org.junit.Assert.assertEquals;

public class SetStaticFieldTest
{
    @Test
    public void shouldAssignFields()
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
