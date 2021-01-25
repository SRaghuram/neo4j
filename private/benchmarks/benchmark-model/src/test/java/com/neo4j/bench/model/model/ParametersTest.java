/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import static com.neo4j.bench.model.model.Parameters.CLIENT;
import static com.neo4j.bench.model.model.Parameters.NONE;
import static com.neo4j.bench.model.model.Parameters.SERVER;
import static com.neo4j.bench.model.model.Parameters.fromMap;
import static com.neo4j.bench.model.model.Parameters.parse;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class ParametersTest
{
    @Test
    public void shouldDoEquality()
    {
        assertThat( NONE, equalTo( fromMap( emptyMap() ) ) );
        assertThat( CLIENT, equalTo( fromMap( singletonMap( "process", "client" ) ) ) );
        assertThat( SERVER, equalTo( fromMap( singletonMap( "process", "server" ) ) ) );
        assertThat( fromMap( singletonMap( "k", "v1" ) ), equalTo( fromMap( singletonMap( "k", "v1" ) ) ) );
        assertThat( fromMap( singletonMap( "k", "v1" ) ), Matchers.not( equalTo( fromMap( singletonMap( "k", "v2" ) ) ) ) );
    }

    @Test
    public void shouldSerializeAndDeserialize()
    {
        doShouldSerializeAndDeserialize( NONE );
        doShouldSerializeAndDeserialize( CLIENT );
        doShouldSerializeAndDeserialize( SERVER );
        doShouldSerializeAndDeserialize( fromMap( singletonMap( "k", "v1" ) ) );
    }

    private void doShouldSerializeAndDeserialize( Parameters parameters )
    {
        assertThat( parameters, equalTo( parse( parameters.toString() ) ) );
    }
}
