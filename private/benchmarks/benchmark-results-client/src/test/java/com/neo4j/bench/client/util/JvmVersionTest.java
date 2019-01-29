package com.neo4j.bench.client.util;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Optional;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class JvmVersionTest
{
    @Test
    public void shouldParseJdk_8_ReleaseFile()
    {
        JvmVersion jvmVersion = JvmVersion.getVersionFromRelease( Resources.toPath( "/jdks/jdk8" ) ).get();
        assertThat( jvmVersion.majorVersion(), is( 8 ) );
        assertThat( jvmVersion.implementor(), is(Optional.empty()) );
    }

    @Test
    public void shouldParseJdk_11_ReleaseFile()
    {
        JvmVersion jvmVersion = JvmVersion.getVersionFromRelease( Resources.toPath( "/jdks/jdk11" ) ).get();
        assertThat( jvmVersion.majorVersion(), is( 11 ) );
        assertThat( jvmVersion.implementor(), is( Optional.of("Oracle Corporation" ) ) );
    }

    @Test
    public void shouldParseJdk_11_0_1_ReleaseFile()
    {
        JvmVersion jvmVersion = JvmVersion.getVersionFromRelease( Resources.toPath( "/jdks/jdk11.0.1" ) ).get();
        assertThat( jvmVersion.majorVersion(), is( 11 ) );
        assertThat( jvmVersion.implementor().get(), is( "Oracle Corporation" ) );
    }

    @Test
    public void shouldParseVersionFromRuntime() throws IOException, InterruptedException
    {
        String javaHome = System.getenv( "JAVA_HOME" );
        int majorVersion = JvmVersion.parseMajorVersion( System.getProperty( "java.version" ) );
        JvmVersion jvmVersion = JvmVersion.getVersionFromRuntime( Jvm.fromJdkPath( Paths.get( javaHome ) ) );
        assertThat( jvmVersion.majorVersion(), is( majorVersion ) );
    }

}
