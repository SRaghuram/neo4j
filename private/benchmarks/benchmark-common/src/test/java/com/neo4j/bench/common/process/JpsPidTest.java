/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.process;

import com.google.common.collect.Lists;
import com.neo4j.bench.common.util.Jvm;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
    public void shouldFindPidWithJPS() throws Exception
    {
        Jvm jvm = Jvm.defaultJvm();
        List<String> jvmArgs = Lists.newArrayList( "-Xmx4g" );
        ArrayList<String> toolCommandArgs = Lists.newArrayList( "help" );
        JvmProcessArgs jvmProcessArgs = JvmProcessArgs.argsForJvmProcess( Collections.emptyList(),
                                                                          jvm,
                                                                          jvmArgs,
                                                                          toolCommandArgs,
                                                                          JustForMain.class );

        JvmProcess jvmProcess = JvmProcess.start( jvmProcessArgs, ProcessBuilder.Redirect.INHERIT, ProcessBuilder.Redirect.INHERIT );
        JpsPid jpsPid = JpsPid.tryFindFor( jvm, Instant.now(), Duration.of( 5, ChronoUnit.MINUTES ), jvmProcessArgs.processName() );
        assertThat( jpsPid.pid().get(), equalTo( jvmProcess.pid().get() ) );
        jvmProcess.waitFor();
    }

    @Test
    public void shouldFindPidWithPgrepAndPS() throws Exception
    {
        Jvm jvm = Jvm.defaultJvm();
        List<String> jvmArgs = Lists.newArrayList( "-Xmx4g" );
        ArrayList<String> toolCommandArgs = Lists.newArrayList( "help" );
        JvmProcessArgs jvmProcessArgs = JvmProcessArgs.argsForJvmProcess( Collections.emptyList(),
                                                                          jvm,
                                                                          jvmArgs,
                                                                          toolCommandArgs,
                                                                          JustForMain.class );

        JvmProcess jvmProcess = JvmProcess.start( jvmProcessArgs, ProcessBuilder.Redirect.INHERIT, ProcessBuilder.Redirect.INHERIT );
        JpsPid jpsPid = JpsPid.tryPgrep( jvmProcessArgs.processName() );
        assertThat( jpsPid.pid().get(), equalTo( jvmProcess.pid().get() ) );
        jvmProcess.waitFor();
    }
}
