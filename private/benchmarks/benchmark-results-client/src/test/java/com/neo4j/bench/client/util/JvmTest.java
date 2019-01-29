package com.neo4j.bench.client.util;

import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static com.neo4j.bench.client.util.JvmVersion.parseMajorVersion;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class JvmTest
{
    @Test
    public void shouldGetJvmVersionForAbsoluteJvmPath()
    {
        Path jdkPath = Paths.get( System.getenv( "JAVA_HOME" ) );
        Jvm jvm = Jvm.fromJdkPath( jdkPath );
        JvmVersion jvmVersion = jvm.version();
        assertThat( "release version should be the same as java.version property",
                jvmVersion.majorVersion(),
                is( parseMajorVersion( System.getProperty( "java.version" ) ) ) );
    }

}
