package com.neo4j.bench.jmh.api;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class RunIT
{
    @Test
    public void shouldDoStuff()
    {
        assertThat( true, equalTo( false ) );
    }
}
