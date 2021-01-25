/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.util;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Paths;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class JvmVersionTest
{

    @Test
    public void shouldParseVersionFromRuntime() throws IOException, InterruptedException
    {
        String javaHome = System.getenv( "JAVA_HOME" );
        int majorVersion = JvmVersion.parseMajorVersion( System.getProperty( "java.version" ) );
        JvmVersion jvmVersion = JvmVersion.getVersion( Jvm.fromJdkPath( Paths.get( javaHome ) ) );
        assertThat( jvmVersion.majorVersion(), is( majorVersion ) );
        assertThat( jvmVersion.runtimeName(), not( isEmptyOrNullString() ) );
    }
}
