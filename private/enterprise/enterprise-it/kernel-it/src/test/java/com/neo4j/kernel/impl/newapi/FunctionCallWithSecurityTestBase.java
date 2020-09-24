/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.newapi;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.procedure.CallableUserFunction;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.impl.newapi.KernelAPIWriteTestBase;
import org.neo4j.kernel.impl.newapi.KernelAPIWriteTestSupport;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTString;
import static org.neo4j.internal.kernel.api.procs.UserFunctionSignature.functionName;
import static org.neo4j.internal.kernel.api.procs.UserFunctionSignature.functionSignature;

public abstract class FunctionCallWithSecurityTestBase<G extends KernelAPIWriteTestSupport> extends KernelAPIWriteTestBase<G>
{
    private final UserFunctionSignature testFunc1Signature = functionSignature( "test", "func1" )
            .in( "name", NTString )
            .out( NTString ).build();

    private final UserFunctionSignature testFunc2Signature = functionSignature( "test", "func2" )
            .in( "name", NTString )
            .out( NTString ).build();

    private final UserFunctionSignature exampleFunc1Signature = functionSignature( "example", "func1" )
            .in( "name", NTString )
            .out( NTString ).build();

    private final UserFunctionSignature exampleFunc2Signature = functionSignature( "example", "func2" )
            .in( "name", NTString )
            .out( NTString ).build();

    private final CallableUserFunction testFunc1 = function( testFunc1Signature );
    private final CallableUserFunction testFunc2 = function( testFunc2Signature );
    private final CallableUserFunction exampleFunc1 = function( exampleFunc1Signature );
    private final CallableUserFunction exampleFunc2 = function( exampleFunc2Signature );

    private static AuthManager authManager;
    private final AnyValue[] functionArgument = {Values.stringValue( "foo" )};
    private final TextValue expectedResult = Values.stringValue( "foo" );
    private final String FAIL_EXECUTE_FUNC = "Executing user defined function is not allowed for user";

    @Override
    public void createSystemGraph( GraphDatabaseService graphDb )
    {
        try
        {
            testSupport.kernelToTest().registerUserFunction( testFunc1 );
            testSupport.kernelToTest().registerUserFunction( testFunc2 );
            testSupport.kernelToTest().registerUserFunction( exampleFunc1 );
            testSupport.kernelToTest().registerUserFunction( exampleFunc2 );
        }
        catch ( ProcedureException e )
        {
            throw new RuntimeException( e );
        }

        try ( Transaction tx = graphDb.beginTx() )
        {
            tx.execute( "REVOKE EXECUTE FUNCTION * ON DBMS FROM PUBLIC" );
            tx.execute( "CREATE USER testUser SET PASSWORD 'abc123' CHANGE NOT REQUIRED" );
            tx.execute( "CREATE ROLE custom" );
            tx.execute( "GRANT ROLE custom TO testUser" );
            tx.execute( "GRANT EXECUTE FUNCTION test.func1 ON DBMS TO custom" );
            tx.execute( "GRANT EXECUTE BOOSTED FUNCTION example.* ON DBMS TO custom" );
            tx.execute( "DENY EXECUTE FUNCTION example.func1 ON DBMS TO custom" );
            tx.commit();
        }

        authManager = ((GraphDatabaseAPI) graphDb).getDependencyResolver().resolveDependency( AuthManager.class );
    }

    @Test
    void shouldExecuteExplicitlyAllowedFunction() throws Exception
    {
        LoginContext loginContext = getTestUserLoginContext();
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            QualifiedName functionName = functionName( "test", "func1" );
            var functionId = tx.procedures().functionGet( functionName ).id();
            assertThat( tx.procedures().functionCall( functionId, functionArgument ) ).isEqualTo( expectedResult );
        }
    }

    @Test
    void shouldFailExecuteFunctionWithoutGrant() throws Exception
    {
        LoginContext loginContext = getTestUserLoginContext();
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            QualifiedName functionName = functionName( "test", "func2" );
            var functionId = tx.procedures().functionGet( functionName ).id();
            assertThatThrownBy( () -> tx.procedures().functionCall( functionId, functionArgument ) ).hasMessageContaining( FAIL_EXECUTE_FUNC );
        }
    }

    @Test
    void shouldExecuteGlobbedPrivilege() throws Exception
    {
        LoginContext loginContext = getTestUserLoginContext();
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            QualifiedName functionName = functionName( "example", "func2" );
            var functionId = tx.procedures().functionGet( functionName ).id();
            assertThat( tx.procedures().functionCall( functionId, functionArgument ) ).isEqualTo( expectedResult );
        }
    }

    @Test
    void shouldFailExecuteDeniedFunction() throws Exception
    {
        LoginContext loginContext = getTestUserLoginContext();
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            QualifiedName functionName = functionName( "example", "func1" );
            var functionId = tx.procedures().functionGet( functionName ).id();
            assertThatThrownBy( () -> tx.procedures().functionCall( functionId, functionArgument ) ).hasMessageContaining( FAIL_EXECUTE_FUNC );
        }
    }

    private LoginContext getTestUserLoginContext() throws InvalidAuthTokenException
    {
        return authManager.login( Map.of( "principal", "testUser", "credentials", "abc123".getBytes( StandardCharsets.UTF_8 ), "scheme", "basic" ) );
    }

    private static CallableUserFunction function( final UserFunctionSignature signature )
    {
        return new CallableUserFunction.BasicUserFunction( signature )
        {
            @Override
            public AnyValue apply( Context ctx, AnyValue[] input )
            {
                return input[0];
            }

            @Override
            public boolean threadSafe()
            {
                return false;
            }
        };
    }
}
