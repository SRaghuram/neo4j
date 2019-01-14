/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.security.enterprise.auth;

import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.neo4j.string.UTF8;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class RoleSerializationTest
{
    private SortedSet<String> steveBob;
    private SortedSet<String> kellyMarie;

    @Before
    public void setUp()
    {
        steveBob = new TreeSet<>();
        steveBob.add( "Steve" );
        steveBob.add( "Bob" );

        kellyMarie = new TreeSet<>();
        kellyMarie.add( "Kelly" );
        kellyMarie.add( "Marie" );
    }

    @Test
    public void shouldSerializeAndDeserialize() throws Exception
    {
        // Given
        RoleSerialization serialization = new RoleSerialization();

        List<RoleRecord> roles = asList(
                new RoleRecord( "admin", steveBob ),
                new RoleRecord( "publisher", kellyMarie ) );

        // When
        byte[] serialized = serialization.serialize( roles );

        // Then
        assertThat( serialization.deserializeRecords( serialized ), equalTo( roles ) );
    }

    /**
     * This is a future-proofing test. If you come here because you've made changes to the serialization format,
     * this is your reminder to make sure to build this is in a backwards compatible way.
     */
    @Test
    public void shouldReadV1SerializationFormat() throws Exception
    {
        // Given
        RoleSerialization serialization = new RoleSerialization();

        // When
        List<RoleRecord> deserialized = serialization.deserializeRecords(
                UTF8.encode( "admin:Bob,Steve\n" + "publisher:Kelly,Marie\n" ) );

        // Then
        assertThat( deserialized, equalTo( asList(
                new RoleRecord( "admin", steveBob ),
                new RoleRecord( "publisher", kellyMarie ) ) ) );
    }
}
