/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.neo4j.string.UTF8;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

class RoleSerializationTest
{
    private SortedSet<String> steveBob;
    private SortedSet<String> kellyMarie;

    @BeforeEach
    void setUp()
    {
        steveBob = new TreeSet<>();
        steveBob.add( "Steve" );
        steveBob.add( "Bob" );

        kellyMarie = new TreeSet<>();
        kellyMarie.add( "Kelly" );
        kellyMarie.add( "Marie" );
    }

    @Test
    void shouldSerializeAndDeserialize() throws Exception
    {
        // Given
        RoleSerialization serialization = new RoleSerialization();

        List<RoleRecord> roles = asList(
                new RoleRecord( "admin", steveBob ),
                new RoleRecord( "publisher", kellyMarie ) );

        // When
        byte[] serialized = serialization.serialize( roles );

        // Then
        assertThat( serialization.deserializeRecords( serialized ) ).isEqualTo( roles );
    }

    /**
     * This is a future-proofing test. If you come here because you've made changes to the serialization format,
     * this is your reminder to make sure to build this is in a backwards compatible way.
     */
    @Test
    void shouldReadV1SerializationFormat() throws Exception
    {
        // Given
        RoleSerialization serialization = new RoleSerialization();

        // When
        List<RoleRecord> deserialized = serialization.deserializeRecords(
                UTF8.encode( "admin:Bob,Steve\n" + "publisher:Kelly,Marie\n" ) );

        // Then
        assertThat( deserialized ).isEqualTo( asList( new RoleRecord( "admin", steveBob ), new RoleRecord( "publisher", kellyMarie ) ) );
    }
}
