/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.stresstests.transaction.log;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.concurrent.Callable;

import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.transaction.log.stresstest.TransactionAppenderStressTest.Builder;
import org.neo4j.kernel.impl.transaction.log.stresstest.TransactionAppenderStressTest.TransactionIdChecker;

import static com.neo4j.helper.StressTestingHelper.ensureExistsAndEmpty;
import static com.neo4j.helper.StressTestingHelper.fromEnv;
import static java.lang.Integer.parseInt;
import static java.lang.System.getProperty;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.function.Suppliers.untilTimeExpired;

/**
 * Notice the class name: this is _not_ going to be run as part of the main build.
 */
class TransactionAppenderStressTesting
{
    private static final String DEFAULT_DURATION_IN_MINUTES = "5";
    private static final String DEFAULT_WORKING_DIR = Path.of( getProperty( "java.io.tmpdir" ), "working" ).toString();
    private static final String DEFAULT_NUM_THREADS = "10";

    @Test
    void shouldBehaveCorrectlyUnderStress() throws Throwable
    {
        int durationInMinutes = parseInt( fromEnv( "TX_APPENDER_STRESS_DURATION", DEFAULT_DURATION_IN_MINUTES ) );
        Path workingDirectory = Path.of( fromEnv( "TX_APPENDER_WORKING_DIRECTORY", DEFAULT_WORKING_DIR ) );
        int threads = parseInt( fromEnv( "TX_APPENDER_NUM_THREADS", DEFAULT_NUM_THREADS ) );

        Callable<Long> runner = new Builder()
                .with( untilTimeExpired( durationInMinutes, MINUTES ) )
                .withWorkingDirectory( DatabaseLayout.ofFlat( ensureExistsAndEmpty( workingDirectory ) ) )
                .withNumThreads( threads )
                .build();

        long appendedTxs = runner.call();

        assertEquals( new TransactionIdChecker( workingDirectory ).parseAllTxLogs(), appendedTxs );

        // let's cleanup disk space when everything went well
        FileUtils.deleteDirectory( workingDirectory );
    }
}
