/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserAggregationResult;
import org.neo4j.procedure.UserAggregationUpdate;
import org.neo4j.procedure.UserFunction;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.jar.JarBuilder;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.plugin_dir;
import static org.neo4j.configuration.GraphDatabaseSettings.procedure_unrestricted;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

@SuppressWarnings( "ALL" )
@TestDirectoryExtension
public class ConcurrentProcedureIT
{
    @Inject
    private TestDirectory plugins;
    private GraphDatabaseService db;
    private DatabaseManagementService managementService;
    private static final int THREADS = 10;

    @BeforeEach
    void setUp() throws IOException
    {
        new JarBuilder().createJarFor( plugins.createFile( "myProceduresWithKernelTransaction.jar" ),
                                       ClassWithProceduresUsingKernelTransaction.class );
        managementService = new TestEnterpriseDatabaseManagementServiceBuilder()
                .impermanent()
                .setConfig( plugin_dir, plugins.absolutePath() )
                .setConfig( procedure_unrestricted,
                            List.of( "com.neo4j.procedure.metaDataIdProcedure",
                                     "com.neo4j.procedure.metaDataIdFunction",
                                     "com.neo4j.procedure.metaDataIdAggregation" ) )
                .build();
        db = managementService.database( DEFAULT_DATABASE_NAME );
    }

    @AfterEach
    void tearDown()
    {
        if ( this.db != null )
        {
            this.managementService.shutdown();
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"INTERPRETED", "SLOTTED", "PIPELINED"} )
    void proceduresShouldGetCorrectKernelTransaction( String runtime ) throws InterruptedException, ExecutionException
    {
        run( tx ->
                     tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.metaDataIdProcedure", runtime ) ).<Long>columnAs( "value" ).next() );
    }

    @ParameterizedTest
    @ValueSource( strings = {"INTERPRETED", "SLOTTED", "PIPELINED"} )
    void userFunctionShouldGetCorrectKernelTransaction( String runtime ) throws InterruptedException, ExecutionException
    {
        run( tx ->
                     tx.execute( format( "CYPHER runtime=%s RETURN com.neo4j.procedure.metaDataIdFunction() AS value", runtime ) ).<Long>columnAs( "value" )
                             .next() );
    }

    @ParameterizedTest
    @ValueSource( strings = {"INTERPRETED", "SLOTTED", "PIPELINED"} )
    void userAggregationFunctionShouldGetCorrectKernelTransaction( String runtime ) throws InterruptedException, ExecutionException
    {
        run( tx ->
                     tx.execute( format( "CYPHER runtime=%s RETURN com.neo4j.procedure.metaDataIdAggregation() AS value", runtime ) ).<Long>columnAs( "value" )
                             .next() );
    }

    private void run( Function<Transaction,Long> value ) throws InterruptedException, ExecutionException
    {
        ExecutorService service = Executors.newFixedThreadPool( THREADS );
        try
        {
            CompletionService<Long> completionService = new ExecutorCompletionService<>( service );
            for ( int i = 0; i < THREADS; i++ )
            {
                completionService.submit( new Worker( value ) );
            }

            MutableLongSet set = new LongHashSet();
            for ( int i = 0; i < THREADS; i++ )
            {
                Long element = completionService.take().get();
                if ( !set.add( element ) )
                {
                    fail( element + " has already been seen in " + set );
                }
            }
        }
        finally
        {
            service.shutdown();
        }
    }

    private final AtomicInteger counter = new AtomicInteger( 0 );

    class Worker implements Callable<Long>
    {
        private final Function<Transaction,Long> function;

        Worker( Function<Transaction,Long> function )
        {
            this.function = function;
        }

        @Override
        public Long call()
        {
            try ( Transaction tx = db.beginTx() )
            {
                KernelTransaction kernelTransaction = ((InternalTransaction) tx).kernelTransaction();
                int uniqueId = counter.getAndIncrement();
                kernelTransaction.setMetaData( map( "id", uniqueId ) );
                long value = function.apply( tx );
                assertEquals( value, uniqueId );
                return value;
            }
        }
    }

    @SuppressWarnings( "unused" )
    public static class Output
    {
        public long value;

        public Output( long value )
        {
            this.value = value;
        }
    }

    @SuppressWarnings( {"unused"} )
    public static class ClassWithProceduresUsingKernelTransaction
    {
        @Context
        public KernelTransaction ktx;

        @Procedure
        public Stream<Output> metaDataIdProcedure()
        {
            return Stream.of( new Output( (int) ktx.getMetaData().get( "id" ) ) );
        }

        @UserFunction
        public long metaDataIdFunction()
        {
            return (int) ktx.getMetaData().get( "id" );
        }

        @UserAggregationFunction
        public MetaDataAggregator metaDataIdAggregation()
        {
            return new MetaDataAggregator( ktx );
        }
    }

    @SuppressWarnings( {"EmptyMethod", "unused"} )
    public static class MetaDataAggregator
    {
        private final KernelTransaction ktx;

        public MetaDataAggregator( KernelTransaction ktx )
        {
            this.ktx = ktx;
        }

        @UserAggregationUpdate
        public void update()
        {
            //this is a little silly and not really an aggregation
        }

        @UserAggregationResult
        public long result()
        {
            return (int) ktx.getMetaData().get( "id" );
        }
    }
}
