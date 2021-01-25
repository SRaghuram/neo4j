/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.security;

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.util.Map;

import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

import static com.neo4j.bench.micro.Main.run;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

@BenchmarkEnabled( true )
@OutputTimeUnit( MICROSECONDS )
public class Authorization extends AbstractSecurityBenchmark
{
    @ParamValues(
            allowed = {"0", "1", "2", "4", "8", "64", "256", "1024"},
            base = {"0", "1024"} )
    @Param( {} )
    public int otherRoles;

    @ParamValues(
            allowed = {"1", "2", "4", "8", "64"},
            base = {"1", "4"} )
    @Param( {} )
    public int roleForUser;

    @ParamValues(
            allowed = {"1", "2", "4", "8", "64"},
            base = {"8", "64"} )
    @Param( {} )
    public int permissionsPerRole;

    @Override
    public String description()
    {
        return "Tests performance of authorization";
    }

    @Override
    public boolean isThreadSafe()
    {
        return false;
    }

    @Override
    protected void afterDatabaseStart( DataGeneratorConfig config )
    {
        try ( Transaction tx = db().beginTx() )
        {
            tx.execute( "CALL db.createProperty('prop')" );
            for ( int i = 0; i < permissionsPerRole; i++ )
            {
                tx.execute( "CALL db.createLabel($label)", Map.of( "label", "L" + i ) );
            }
            tx.commit();
        }

        try ( Transaction tx = systemDb().beginTx() )
        {
            tx.execute( "CREATE USER user IF NOT EXISTS SET PASSWORD 'password' CHANGE NOT REQUIRED" );

            for ( int i = 0; i < roleForUser; i++ )
            {
                String role = "role" + i;
                tx.execute( String.format( "CREATE ROLE %s IF NOT EXISTS", role ) );
                tx.execute( String.format( "GRANT ROLE %s TO user", role ) );
                tx.execute( String.format( "GRANT ACCESS ON DATABASE * TO %s", role ) );
                for ( int j = 0; j < permissionsPerRole; j++ )
                {
                    String label = "L" + j;
                    tx.execute( String.format( "GRANT MATCH {prop} ON GRAPH * NODE %s TO %s", label, role ) );
                }
            }
            for ( int i = 0; i < otherRoles; i++ )
            {
                String role = "extraRole" + i;
                tx.execute( String.format( "CREATE ROLE %s IF NOT EXISTS", role ) );
                tx.execute( String.format( "GRANT ACCESS ON DATABASE * TO %s", role ) );
                for ( int j = 0; j < permissionsPerRole; j++ )
                {
                    String label = "L" + j;
                    tx.execute( String.format( "GRANT MATCH {prop} ON GRAPH * NODE %s TO %s", label, role ) );
                }
            }
            tx.commit();
        }
    }

    @State( Scope.Thread )
    public static class UserState
    {
        LoginContext user;
        GraphDatabaseFacade facade;
        EnterpriseAuthManager authManager;
        Transaction tx;

        @Setup( Level.Invocation )
        public void setUp( Authorization benchmark ) throws Exception
        {
            facade = (GraphDatabaseFacade) benchmark.db();
            authManager = facade.getDependencyResolver().resolveDependency( EnterpriseAuthManager.class );
            user = authManager.login( AuthToken.newBasicAuthToken( "user", "password" ) );
        }

        @TearDown( Level.Invocation )
        public void tearDown()
        {
            tx.rollback();
            authManager.clearAuthCache();
        }
    }

    @Benchmark
    @BenchmarkMode( {Mode.AverageTime} )
    public void authorize( UserState state )
    {
        state.tx = state.facade.beginTransaction( KernelTransaction.Type.EXPLICIT, state.user );
    }

    public static void main( String... methods )
    {
        run( Authorization.class, methods );
    }
}
