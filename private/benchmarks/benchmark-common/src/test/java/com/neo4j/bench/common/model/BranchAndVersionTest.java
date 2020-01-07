/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.model;

import com.neo4j.bench.common.util.BenchmarkUtil;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BranchAndVersionTest
{
    @Test
    public void shouldPassValidateWithExpectedParameters()
    {
        BranchAndVersion.validate( Repository.MICRO_BENCH, Repository.MICRO_BENCH.defaultOwner(), "3.4" );
        BranchAndVersion.validate( Repository.LDBC_BENCH, Repository.LDBC_BENCH.defaultOwner(), "3.4" );
        BranchAndVersion.validate( Repository.MACRO_BENCH, Repository.MACRO_BENCH.defaultOwner(), "3.4" );
        BranchAndVersion.validate( Repository.ALGOS, Repository.ALGOS.defaultOwner(), "master" );
        BranchAndVersion.validate( Repository.ALGOS_JMH, Repository.ALGOS_JMH.defaultOwner(), "master" );
    }

    @Test
    public void shouldFailValidateWithUnexpectedParameters()
    {
        BenchmarkUtil.assertException( RuntimeException.class,
                                       () -> BranchAndVersion.validate( Repository.MICRO_BENCH, Repository.MICRO_BENCH.defaultOwner(), "3.4-pie" ) );

        BenchmarkUtil.assertException( RuntimeException.class,
                                       () -> BranchAndVersion.validate( Repository.LDBC_BENCH, Repository.LDBC_BENCH.defaultOwner(), "3.4-pie" ) );

        BenchmarkUtil.assertException( RuntimeException.class,
                                       () -> BranchAndVersion.validate( Repository.LDBC_BENCH, "Robert", "3.4" ) );

        BenchmarkUtil.assertException( RuntimeException.class,
                                       () -> BranchAndVersion.validate( Repository.ALGOS, Repository.ALGOS.defaultOwner(), "3.4.5.5" ) );

        BenchmarkUtil.assertException( RuntimeException.class,
                                       () -> BranchAndVersion.validate( Repository.ALGOS_JMH, Repository.ALGOS_JMH.defaultOwner(), "3.4.5.2" ) );
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
              .forEach( repository ->
                        {
                            assertTrue( repository.isValidVersion( "1.2.3" ) );
                            assertTrue( repository.isValidVersion( "11.2.3" ) );
                            assertTrue( repository.isValidVersion( "1.22.3" ) );
                            assertTrue( repository.isValidVersion( "1.2.33" ) );
                            assertTrue( repository.isValidVersion( "11.22.33" ) );

                            assertFalse( repository.isValidVersion( "1" ) );
                            assertFalse( repository.isValidVersion( "1.2" ) );
                            if ( repository != Repository.ALGOS && repository != Repository.ALGOS_JMH )
                            {
                                // ALGO repos have different version scheme and allow a fourth version number
                                assertFalse( repository.isValidVersion( "1.2.3.4" ) );
                            }
                            else
                            {
                                assertTrue( repository.isValidVersion( "1.2.3.4" ) );
                            }

                            assertFalse( repository.isValidVersion( "111.2.3" ) );
                            assertFalse( repository.isValidVersion( "1.222.3" ) );
                            assertFalse( repository.isValidVersion( "1.2.333" ) );
                            assertFalse( repository.isValidVersion( "123" ) );

                            repository.assertValidVersion( "1.2.3" );
                            repository.assertValidVersion( "11.2.3" );
                            repository.assertValidVersion( "1.22.3" );
                            repository.assertValidVersion( "1.2.33" );
                            repository.assertValidVersion( "11.22.33" );

                            BenchmarkUtil.assertException( RuntimeException.class,
                                                           () -> repository.assertValidVersion( "1" ) );

                            BenchmarkUtil.assertException( RuntimeException.class,
                                                           () -> repository.assertValidVersion( "1.2" ) );

                            if ( repository != Repository.ALGOS && repository != Repository.ALGOS_JMH )
                            {
                                // ALGO repos have different version scheme and allow a fourth version number
                                BenchmarkUtil.assertException( RuntimeException.class,
                                                               () -> repository.assertValidVersion( "1.2.3.4" ) );
                            }
                            else
                            {
                                repository.assertValidVersion( "1.2.3.4" );
                            }

                            BenchmarkUtil.assertException( RuntimeException.class,
                                                           () -> repository.assertValidVersion( "111.2.3" ) );

                            BenchmarkUtil.assertException( RuntimeException.class,
                                                           () -> repository.assertValidVersion( "1.222.3" ) );

                            BenchmarkUtil.assertException( RuntimeException.class,
                                                           () -> repository.assertValidVersion( "1.2.333" ) );

                            BenchmarkUtil.assertException( RuntimeException.class,
                                                           () -> repository.assertValidVersion( "123" ) );
                        } );
    }

    @Test
    public void checksBranchEqualsSeries()
    {
        assertTrue( BranchAndVersion.branchEqualsSeries( "1.2.3", "1.2" ) );
        assertTrue( BranchAndVersion.branchEqualsSeries( "11.22.33", "11.22" ) );

        assertFalse( BranchAndVersion.branchEqualsSeries( "1.2.3", "2.3" ) );

        BranchAndVersion.assertBranchEqualsSeries( "1.2.3", "1.2" );
        BranchAndVersion.assertBranchEqualsSeries( "11.22.33", "11.22" );

        BenchmarkUtil.assertException( RuntimeException.class,
                                       () -> BranchAndVersion.assertBranchEqualsSeries( "1.2.3", "2.3" ) );
    }

    @Test
    public void checksToSanitizedVersion()
    {
        Arrays.stream( Repository.values() )
              .forEach( repository ->
                        {
                            assertThat( BranchAndVersion.toSanitizeVersion( repository, "3.1.0" ), equalTo( "3.1.0" ) );
                            assertThat( BranchAndVersion.toSanitizeVersion( repository, "3.1.0-SNAPSHOT" ), equalTo( "3.1.0" ) );

                            BenchmarkUtil.assertException( RuntimeException.class,
                                                           () -> assertThat( BranchAndVersion.toSanitizeVersion( repository, "cake-SNAPSHOT" ),
                                                                             equalTo( "3.1.0" ) ) );
                        } );
    }
}
