/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.process;

import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.model.process.JvmArgs;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class JpsPidTest
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    public static class JustForMain
    {
        public static void main( String[] args ) throws InterruptedException
        {
            Duration sleepMs = Duration.ofSeconds( 5 );
            Thread.sleep( sleepMs.toMillis() );
            System.out.println( "Hello Work" );
        }
    }

    @Test
    public void shouldFailWhenNotUsingAnyPidStrategy() throws RuntimeException
    {
        Jvm jvm = Jvm.defaultJvm();
        JvmProcessArgs jvmProcessArgs = JvmProcessArgs.argsForJvmProcess( Collections.emptyList(),
                                                                          jvm,
                                                                          JvmArgs.empty(),
                                                                          Collections.emptyList(),
                                                                          JustForMain.class );
        BenchmarkUtil.assertException( RuntimeException.class,
                                       () -> JvmProcess.start( jvmProcessArgs, ProcessBuilder.Redirect.INHERIT, ProcessBuilder.Redirect.INHERIT,
                                                               Collections.emptyList() ) );
    }

    @Test
    public void shouldFindPidWithJPSAndPgrepAndPS() throws Exception
    {
        Jvm jvm = Jvm.defaultJvm();
        JvmArgs jvmArgs = JvmArgs.from(  "-Xmx4g" );
        JvmProcessArgs jvmProcessArgs = JvmProcessArgs.argsForJvmProcess( Collections.emptyList(),
                                                                          jvm,
                                                                          jvmArgs,
                                                                          Collections.emptyList(),
                                                                          JustForMain.class );

        JvmProcess jvmProcess = JvmProcess.start( jvmProcessArgs, ProcessBuilder.Redirect.INHERIT, ProcessBuilder.Redirect.INHERIT,
                                                  Arrays.asList( new JpsPid(), new PgrepAndPsPid() ) );
        JpsPid jpsPid = new JpsPid();
        PgrepAndPsPid pgrepAndPsPid = new PgrepAndPsPid();

        jpsPid.tryFindFor( jvm, Instant.now(), Duration.of( 5, ChronoUnit.MINUTES ), jvmProcessArgs.processName() );
        pgrepAndPsPid.tryFindFor( jvm, Instant.now(), Duration.of( 5, ChronoUnit.MINUTES ), jvmProcessArgs.processName() );
        assertThat( jpsPid.pid().get(), equalTo( jvmProcess.pid().get() ) );
        assertThat( pgrepAndPsPid.pid().get(), equalTo( jvmProcess.pid().get() ) );
        jvmProcess.waitFor();
    }
}
