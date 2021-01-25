/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.application;

import com.neo4j.configuration.ApplicationProtocolVersion;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.comparesEqualTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ApplicationProtocolVersionTest
{
    @Test
    void shouldNotSupportNegativeMajorAndMinor()
    {
        assertThrows( IllegalArgumentException.class, () -> new ApplicationProtocolVersion( -1, 1 ) );
        assertThrows( IllegalArgumentException.class, () -> new ApplicationProtocolVersion( 1, -1 ) );
        assertThrows( IllegalArgumentException.class, () -> new ApplicationProtocolVersion( -1, -1 ) );
    }

    @Test
    void shouldContainMajorAndMinor()
    {
        var version = new ApplicationProtocolVersion( 37, 42 );

        assertEquals( 37, version.major() );
        assertEquals( 42, version.minor() );
    }

    @Test
    void shouldParseValidStrings()
    {
        assertEquals( new ApplicationProtocolVersion( 1, 5 ), ApplicationProtocolVersion.parse( "1.5" ) );
        assertEquals( new ApplicationProtocolVersion( 0, 99 ), ApplicationProtocolVersion.parse( "0.99" ) );
        assertEquals( new ApplicationProtocolVersion( 3, 42 ), ApplicationProtocolVersion.parse( "3.42" ) );
        assertEquals( new ApplicationProtocolVersion( 99, 999 ), ApplicationProtocolVersion.parse( "99.999" ) );
    }

    @Test
    void shouldFailToParseInvalidStrings()
    {
        assertThrows( IllegalArgumentException.class, () -> ApplicationProtocolVersion.parse( "" ) );
        assertThrows( IllegalArgumentException.class, () -> ApplicationProtocolVersion.parse( "1." ) );
        assertThrows( IllegalArgumentException.class, () -> ApplicationProtocolVersion.parse( "1.2.3" ) );
        assertThrows( IllegalArgumentException.class, () -> ApplicationProtocolVersion.parse( "a" ) );
        assertThrows( IllegalArgumentException.class, () -> ApplicationProtocolVersion.parse( "a.b" ) );
        assertThrows( IllegalArgumentException.class, () -> ApplicationProtocolVersion.parse( "1.a" ) );
        assertThrows( IllegalArgumentException.class, () -> ApplicationProtocolVersion.parse( "1.0a" ) );

        assertThrows( IllegalArgumentException.class, () -> ApplicationProtocolVersion.parse( "1.-1" ) );
        assertThrows( IllegalArgumentException.class, () -> ApplicationProtocolVersion.parse( "-1.9" ) );
        assertThrows( IllegalArgumentException.class, () -> ApplicationProtocolVersion.parse( "-1.-9" ) );
    }

    @Test
    void shouldEncodeAndDecode()
    {
        var buffer = Unpooled.buffer();
        try
        {
            var originalVersion = new ApplicationProtocolVersion( 13, 45 );

            originalVersion.encode( buffer );
            var decodedVersion = ApplicationProtocolVersion.decode( buffer );

            assertEquals( originalVersion, decodedVersion );
        }
        finally
        {
            buffer.release();
        }
    }

    @Test
    void shouldCompareVersions()
    {
        var version1 = new ApplicationProtocolVersion( 1, 15 );
        var version2 = new ApplicationProtocolVersion( 0, 99 );
        var version3 = new ApplicationProtocolVersion( 2, 0 );

        assertThat( version1, comparesEqualTo( version1 ) );
        assertThat( version1, greaterThan( version2 ) );
        assertThat( version1, lessThan( version3 ) );

        assertThat( version2, comparesEqualTo( version2 ) );
        assertThat( version2, lessThan( version1 ) );
        assertThat( version2, lessThan( version3 ) );

        assertThat( version3, comparesEqualTo( version3 ) );
        assertThat( version3, greaterThan( version1 ) );
        assertThat( version3, greaterThan( version2 ) );
    }
}
