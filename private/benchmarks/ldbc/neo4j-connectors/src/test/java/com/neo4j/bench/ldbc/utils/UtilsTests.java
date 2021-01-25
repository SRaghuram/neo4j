/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.utils;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class UtilsTests
{
    @Test
    public void shouldCopyAndAppendElementToNewArrayWhenOldArrayNotNull() throws IOException
    {
        String[] oldArray = {"1", "2", "3"};
        String newElement = "4";
        String[] newArray = Utils.copyArrayAndAddElement( oldArray, newElement );
        assertThat( newArray, equalTo( new String[] {"1", "2", "3", "4"} ) );
    }

    @Test
    public void shouldCopyAndAppendElementToNewArrayWhenOldArrayNull() throws IOException
    {
        String[] oldArray = null;
        String newElement = "4";
        String[] newArray = Utils.copyArrayAndAddElement( oldArray, newElement );
        assertThat( newArray, equalTo( new String[] {"4"} ) );
    }
}
