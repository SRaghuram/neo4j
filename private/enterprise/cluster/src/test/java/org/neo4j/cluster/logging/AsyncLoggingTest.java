/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.logging;

import org.junit.Test;

import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.async.AsyncLogProvider;

import static org.hamcrest.CoreMatchers.endsWith;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class AsyncLoggingTest
{
    @Test
    public void shouldLogMessages()
    {
        // given
        AssertableLogProvider logs = new AssertableLogProvider();
        AsyncLogging logging = new AsyncLogging( logs.getLog( "meta" ) );

        // when
        logging.start();
        try
        {
            new AsyncLogProvider( logging.eventSender(), logs ).getLog( "test" ).info( "hello" );
        }
        finally
        {
            logging.stop();
        }
        // then
        logs.assertExactly( inLog( "test" ).info( endsWith( "hello" ) ) );
    }

    @Test
    public void shouldLogWhenLoggingThreadStarts()
    {
        // given
        AssertableLogProvider logs = new AssertableLogProvider();
        AsyncLogging logging = new AsyncLogging( logs.getLog( "meta" ) );

        // when
        new AsyncLogProvider( logging.eventSender(), logs ).getLog( "test" ).info( "hello" );

        // then
        logs.assertNoLoggingOccurred();

        // when
        logging.start();
        logging.stop();

        // then
        logs.assertExactly( inLog( "test" ).info( endsWith( "hello" ) ) );
    }
}
