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
