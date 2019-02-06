/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro;

import org.junit.Test;

public class ListCommandIT
{
    @Test
    public void shouldPrintAvailableGroups() throws Exception
    {
        Main.main( new String[]{"ls"} );
    }

    @Test
    public void shouldPrintAvailableGroupsAndBenchmarks() throws Exception
    {
        Main.main( new String[]{"ls", "-v"} );
    }
}
