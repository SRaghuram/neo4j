/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static com.neo4j.bench.model.model.BranchAndVersion.validate;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BranchAndVersionTest
{
    @Test
    public void shouldPassValidateWithExpectedParameters()
    {
        validate( Repository.MICRO_BENCH, Repository.MICRO_BENCH.defaultOwner(), "3.4" );
        validate( Repository.LDBC_BENCH, Repository.LDBC_BENCH.defaultOwner(), "3.4" );
        validate( Repository.MACRO_BENCH, Repository.MACRO_BENCH.defaultOwner(), "3.4" );
        validate( Repository.MICRO_BENCH, Repository.MICRO_BENCH.defaultOwner(), "4.2.0-drop07" );
        validate( Repository.LDBC_BENCH, Repository.LDBC_BENCH.defaultOwner(), "4.2.0-drop07" );
        validate( Repository.MACRO_BENCH, Repository.MACRO_BENCH.defaultOwner(), "4.2.0-drop07" );
        validate( Repository.ALGOS, Repository.ALGOS.defaultOwner(), "master" );
        validate( Repository.ALGOS_JMH, Repository.ALGOS_JMH.defaultOwner(), "master" );
        validate( Repository.QUALITY_TASK, Repository.QUALITY_TASK.defaultOwner(), "master" );
    }

    @Test
    public void shouldFailValidateWithUnexpectedParameters()
    {
        assertException( RuntimeException.class,
                                       () -> validate( Repository.MICRO_BENCH, Repository.MICRO_BENCH.defaultOwner(), "3.4-pie" ) );

        assertException( RuntimeException.class,
                                       () -> validate( Repository.LDBC_BENCH, Repository.LDBC_BENCH.defaultOwner(), "3.4-pie" ) );

        assertException( RuntimeException.class,
                                       () -> validate( Repository.LDBC_BENCH, "Robert", "3.4" ) );

        assertException( RuntimeException.class,
                                       () -> validate( Repository.ALGOS, Repository.ALGOS.defaultOwner(), "3.4.5" ) );

        assertException( RuntimeException.class,
                                       () -> validate( Repository.ALGOS_JMH, Repository.ALGOS_JMH.defaultOwner(), "3.4.5" ) );

        assertException( RuntimeException.class,
                                       () -> validate( Repository.QUALITY_TASK, Repository.QUALITY_TASK.defaultOwner(), "1.0" ) );
    }

    @Test
    public void checksPersonalBranch()
    {
        Arrays.stream( Repository.values() )
              .forEach( repository ->
                        {
                            assertFalse( BranchAndVersion.isPersonalBranch( repository, repository.defaultOwner() ) );
                            assertFalse( BranchAndVersion.isPersonalBranch( repository, repository.defaultOwner().toLowerCase() ) );
                            assertFalse( BranchAndVersion.isPersonalBranch( repository, repository.defaultOwner().toUpperCase() ) );
                        } );

        Arrays.stream( Repository.values() )
              .forEach( repository ->
                        {
                            assertTrue( BranchAndVersion.isPersonalBranch( repository, "cat" ) );
                            assertTrue( BranchAndVersion.isPersonalBranch( repository, "Dog" ) );
                            assertTrue( BranchAndVersion.isPersonalBranch( repository, "T-REX" ) );
                        } );
    }

    @Test
    public void checksVersionFormat()
    {
        Arrays.stream( Repository.values() )
              .filter( r -> r != Repository.QUALITY_TASK ) // Ignore Quality repos, as they use a different version scheme
              .forEach( repository ->
                        {
                            assertTrue( repository.isValidVersion( "1.2.3" ) );
                            assertTrue( repository.isValidVersion( "11.2.3" ) );
                            assertTrue( repository.isValidVersion( "1.22.3" ) );
                            assertTrue( repository.isValidVersion( "1.2.33" ) );
                            assertTrue( repository.isValidVersion( "11.22.33" ) );

                            assertFalse( repository.isValidVersion( "1" ) );
                            assertFalse( repository.isValidVersion( "1.2" ) );

                            assertFalse( repository.isValidVersion( "1.2.3.4" ) );

                            assertFalse( repository.isValidVersion( "111.2.3" ) );
                            assertFalse( repository.isValidVersion( "1.222.3" ) );
                            assertFalse( repository.isValidVersion( "1.2.333" ) );
                            assertFalse( repository.isValidVersion( "123" ) );

                            repository.assertValidVersion( "1.2.3" );
                            repository.assertValidVersion( "11.2.3" );
                            repository.assertValidVersion( "1.22.3" );
                            repository.assertValidVersion( "1.2.33" );
                            repository.assertValidVersion( "11.22.33" );

                            assertException( RuntimeException.class,
                                                           () -> repository.assertValidVersion( "1" ) );

                            assertException( RuntimeException.class,
                                                           () -> repository.assertValidVersion( "1.2" ) );

                            assertException( RuntimeException.class,
                                                           () -> repository.assertValidVersion( "1.2.3.4" ) );

                            assertException( RuntimeException.class,
                                                           () -> repository.assertValidVersion( "111.2.3" ) );

                            assertException( RuntimeException.class,
                                                           () -> repository.assertValidVersion( "1.222.3" ) );

                            assertException( RuntimeException.class,
                                                           () -> repository.assertValidVersion( "1.2.333" ) );

                            assertException( RuntimeException.class,
                                                           () -> repository.assertValidVersion( "123" ) );
                        } );
    }

    @Test
    public void checksBranchEqualsSeries() throws IllegalAccessException
    {
        BranchAndVersion.assertBranchEqualsSeries( "1.2.3", "1.2" );
        BranchAndVersion.assertBranchEqualsSeries( "11.22.33", "11.22" );

        BranchAndVersion.assertBranchEqualsSeries( "1.2.3-drop08.0", "1.2.3-drop08" );
        BranchAndVersion.assertBranchEqualsSeries( "1.2.3-drop08.0", "1.2" );

        assertTrue( BranchAndVersion.isValidDropBranch( "1.2.3-drop08.0", "1.2.3-drop08" ) );

        assertFalse( BranchAndVersion.isValidDropBranch( "1.2.3-drop08.0", "1.2" ) );

        assertException( RuntimeException.class,
                         () -> BranchAndVersion.assertBranchEqualsSeries( "1.2.3", "2.3" ) );
    }

    public void checkIsValidSeriesBranch() throws IllegalAccessException
    {
        assertTrue( BranchAndVersion.isValidSeriesBranch( "1.2.3", "1.2" ) );
        assertTrue( BranchAndVersion.isValidSeriesBranch( "11.22.33", "11.22" ) );
        assertFalse( BranchAndVersion.isValidSeriesBranch( "1.2.3", "2.3" ) );
        assertTrue( BranchAndVersion.isValidSeriesBranch( "1.2.3-drop08.0", "1.2" ) );
        assertFalse( BranchAndVersion.isValidSeriesBranch( "1.2.3-drop08.0", "1.2.3-drop08" ) );
    }

    public static <EXCEPTION extends Throwable> EXCEPTION assertException( Class<EXCEPTION> exception, ThrowingRunnable fun )
    {
        try
        {
            fun.run();
        }
        catch ( Throwable e )
        {
            if ( e.getClass().equals( exception ) )
            {
                return (EXCEPTION) e;
            }
            else
            {
                throw new RuntimeException( format( "Expected exception of type %s but was %s", exception.getName(), e.getClass().getName() ), e );
            }
        }
        throw new RuntimeException( "Expected exception to be thrown: " + exception.getName() );
    }

    public interface ThrowingRunnable
    {
        void run() throws Exception;
    }
}
