/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.multidatabase.stresstest;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.multidatabase.stresstest.commands.CreateManagerCommand;
import org.neo4j.multidatabase.stresstest.commands.DatabaseManagerCommand;
import org.neo4j.multidatabase.stresstest.commands.DropManagerCommand;
import org.neo4j.multidatabase.stresstest.commands.ExecuteTransactionCommand;
import org.neo4j.multidatabase.stresstest.commands.StopStartManagerCommand;

import static java.lang.String.format;
import static org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertNull;

class CommandExecutor implements Runnable
{
    private static final AtomicInteger dbCounter = new AtomicInteger();
    private final DatabaseManager databaseManager;
    private final CountDownLatch executionLatch;
    private final long finishTimeMillis;
    private final ThreadLocalRandom random;
    private final AtomicInteger commandCounter = new AtomicInteger();
    private volatile Exception executionException;
    private int createCommands;
    private int executionCommands;
    private int stopStartCommands;
    private int dropCommands;

    CommandExecutor( DatabaseManager databaseManager, CountDownLatch executionLatch, long finishTimeMillis )
    {
        this.databaseManager = databaseManager;
        this.executionLatch = executionLatch;
        this.finishTimeMillis = finishTimeMillis;
        this.random = ThreadLocalRandom.current();
    }

    @Override
    public void run()
    {
        while ( finishTimeMillis > System.currentTimeMillis() )
        {
            try
            {
                List<String> databases = databaseManager.listDatabases();
                DatabaseManagerCommand command;

                if ( databases.isEmpty() )
                {
                    command = new CreateManagerCommand( databaseManager, createDatabaseName() );
                    createCommands++;
                }
                else
                {
                    String database = getRandomDatabaseName( databases );
                    int operation = random.nextInt( 100 );
                    if ( operation < 80 )
                    {
                        command = new ExecuteTransactionCommand( databaseManager, database );
                        executionCommands++;
                    }
                    else if ( operation < 90 )
                    {
                        command = new StopStartManagerCommand( databaseManager, database );
                        stopStartCommands++;
                    }
                    else if ( operation < 95 )
                    {
                        command = new CreateManagerCommand( databaseManager, createDatabaseName() );
                        createCommands++;
                    }
                    else
                    {
                        command = new DropManagerCommand( databaseManager, database );
                        dropCommands++;
                    }
                }
                command.execute();
                commandCounter.incrementAndGet();
            }
            catch ( TransientTransactionFailureException | TransactionFailureException | IllegalStateException e )
            {
                // ignore
            }
            catch ( Exception e )
            {
                if ( executionException == null )
                {
                    executionException = e;
                }
            }
        }
        executionLatch.countDown();
    }

    private String createDatabaseName()
    {
        return "database" + dbCounter.getAndIncrement();
    }

    private String getRandomDatabaseName( List<String> databases )
    {
        int knownDatabases = databases.size();
        return databases.get( random.nextInt( knownDatabases ) );
    }

    void checkExecutionResults()
    {
        assertThat( commandCounter.get(), greaterThan( 0 ) );
        System.out.println("======================================================");
        System.out.println( format( "Commands distribution: created databases: %d,%n " +
                        "stop-start database: %d,%n dropped databases: %d,%n execute transactions: %d.",
                        createCommands, stopStartCommands, dropCommands, executionCommands ) );
        System.out.println("======================================================");
        assertNull( executionException, () -> getStackTrace( executionException ) );
    }
}
