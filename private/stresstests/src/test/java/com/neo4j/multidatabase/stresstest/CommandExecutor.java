/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.multidatabase.stresstest;

import com.neo4j.multidatabase.stresstest.commands.CreateManagerCommand;
import com.neo4j.multidatabase.stresstest.commands.DatabaseManagerCommand;
import com.neo4j.multidatabase.stresstest.commands.DropManagerCommand;
import com.neo4j.multidatabase.stresstest.commands.ExecuteTransactionCommand;
import com.neo4j.multidatabase.stresstest.commands.StopStartManagerCommand;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.TransientTransactionFailureException;

import static java.lang.String.format;
import static org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

class CommandExecutor implements Runnable
{
    private static final AtomicInteger dbCounter = new AtomicInteger();
    private final DatabaseManagementService dbms;
    private final CountDownLatch executionLatch;
    private final long finishTimeMillis;
    private final ThreadLocalRandom random;
    private final AtomicInteger commandCounter = new AtomicInteger();
    private volatile Exception executionException;
    private int createCommands;
    private int executionCommands;
    private int stopStartCommands;
    private int dropCommands;

    CommandExecutor( DatabaseManagementService dbms, CountDownLatch executionLatch, long finishTimeMillis )
    {
        this.dbms = dbms;
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
                var databases = dbms.listDatabases().stream()
                        .filter( dbName -> !Objects.equals( dbName, SYSTEM_DATABASE_NAME ) )
                        .collect( Collectors.toList() );

                DatabaseManagerCommand command;

                if ( databases.isEmpty() )
                {
                    command = new CreateManagerCommand( dbms, createDatabaseName() );
                    createCommands++;
                }
                else
                {
                    String databaseName = getRandomDatabaseName( databases );
                    int operation = random.nextInt( 100 );
                    if ( operation < 80 )
                    {
                        command = new ExecuteTransactionCommand( dbms, databaseName );
                        executionCommands++;
                    }
                    else if ( operation < 90 )
                    {
                        command = new StopStartManagerCommand( dbms, databaseName );
                        stopStartCommands++;
                    }
                    else if ( operation < 95 )
                    {
                        command = new CreateManagerCommand( dbms, createDatabaseName() );
                        createCommands++;
                    }
                    else
                    {
                        command = new DropManagerCommand( dbms, databaseName );
                        dropCommands++;
                    }
                }
                command.execute();
                commandCounter.incrementAndGet();
            }
            catch ( TransientTransactionFailureException |
                    TransactionFailureException |
                    DatabaseShutdownException |
                    DatabaseNotFoundException e )
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

    private static String createDatabaseName()
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
