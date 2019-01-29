package com.neo4j.bench.client.model;

import com.neo4j.bench.client.util.BenchmarkUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class BranchAndVersionTest
{
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldPassValidateWithExpectedParameters()
    {
        BranchAndVersion.validate( Repository.MICRO_BENCH, Repository.MICRO_BENCH.defaultOwner(), "3.4" );
        BranchAndVersion.validate( Repository.LDBC_BENCH, Repository.LDBC_BENCH.defaultOwner(), "3.4" );
        BranchAndVersion.validate( Repository.RONJA_BENCH, Repository.RONJA_BENCH.defaultOwner(), "3.4" );
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

                            BenchmarkUtil.assertException( RuntimeException.class,
                                                           () -> repository.assertValidVersion( "1" ) );

                            BenchmarkUtil.assertException( RuntimeException.class,
                                                           () -> repository.assertValidVersion( "1.2" ) );

                            BenchmarkUtil.assertException( RuntimeException.class,
                                                           () -> repository.assertValidVersion( "1.2.3.4" ) );

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
