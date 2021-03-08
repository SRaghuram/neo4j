/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.database;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AllSupportedVersionsTest
{

    @Test
    public void findPrevSupportedVersion()
    {
        String prevVersion = AllSupportedVersions.prevVersion( "4.0" );
        assertEquals( "3.5", prevVersion );
    }

    @Test
    public void notSupportedVersion()
    {
        assertThrows( IllegalArgumentException.class,
                                 () -> AllSupportedVersions.prevVersion( "5.0" ),
                                 "unsupported or not found previous version of 5.0, " +
                                 "please update com.neo4j.bench.common.database.AllSupportedVersions class" );
    }
}
