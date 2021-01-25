/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.process;

import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.model.process.JvmArgs;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class JpsPidTest
{
    private static final Logger LOG = LoggerFactory.getLogger( JpsPidTest.class );

    public static class JustForMain
    {
        public static void main( String[] args ) throws InterruptedException
        {
            Duration sleepMs = Duration.ofSeconds( 5 );
            Thread.sleep( sleepMs.toMillis() );
            LOG.debug( "Hello Work" );
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
    public void shouldFindPidWithJPSAndPgrepAndPS()
    {
        Jvm jvm = Jvm.defaultJvm();
        JvmArgs jvmArgs = JvmArgs.from( "-Xmx4g" );
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
