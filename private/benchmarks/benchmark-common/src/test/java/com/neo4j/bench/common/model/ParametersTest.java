/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.model;

import com.neo4j.bench.model.model.Parameters;
import org.junit.jupiter.api.Test;

import static com.neo4j.bench.model.model.Parameters.fromMap;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

class ParametersTest
{
    @Test
    void shouldDoEquality()
    {
        assertThat( Parameters.NONE, equalTo( fromMap( emptyMap() ) ) );
        assertThat( Parameters.CLIENT, equalTo( fromMap( singletonMap( "process", "client" ) ) ) );
        assertThat( Parameters.SERVER, equalTo( fromMap( singletonMap( "process", "server" ) ) ) );
        assertThat( fromMap( singletonMap( "k", "v1" ) ), equalTo( fromMap( singletonMap( "k", "v1" ) ) ) );
        assertThat( fromMap( singletonMap( "k", "v1" ) ), not( equalTo( fromMap( singletonMap( "k", "v2" ) ) ) ) );
    }

    @Test
    void shouldSerializeAndDeserialize()
    {
        doShouldSerializeAndDeserialize( Parameters.NONE );
        doShouldSerializeAndDeserialize( Parameters.CLIENT );
        doShouldSerializeAndDeserialize( Parameters.SERVER );
        doShouldSerializeAndDeserialize( fromMap( singletonMap( "k", "v1" ) ) );
    }

    private void doShouldSerializeAndDeserialize( Parameters parameters )
    {
        assertThat( parameters, equalTo( Parameters.parse( parameters.toString() ) ) );
    }
}
