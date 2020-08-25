/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.newapi;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.neo4j.collection.RawIterator;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.impl.newapi.KernelAPIWriteTestBase;
import org.neo4j.kernel.impl.newapi.KernelAPIWriteTestSupport;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.values.AnyValue;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTString;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureName;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature;

public abstract class ProcedureCallWithSecurityTestBase<G extends KernelAPIWriteTestSupport> extends KernelAPIWriteTestBase<G>
{
    private final ProcedureSignature testProc1Signature = procedureSignature( "test", "proc1" )
            .in( "name", NTString )
            .out( "name", NTString ).build();

    private final ProcedureSignature testProc2Signature = procedureSignature( "test", "proc2" )
            .in( "name", NTString )
            .out( "name", NTString ).build();

    private final ProcedureSignature exampleProc1Signature = procedureSignature( "example", "proc1" )
            .in( "name", NTString )
            .out( "name", NTString ).build();

    private final ProcedureSignature exampleProc2Signature = procedureSignature( "example", "proc2" )
            .in( "name", NTString )
            .out( "name", NTString ).build();

    private final CallableProcedure testProc1 = procedure( testProc1Signature );
    private final CallableProcedure testProc2 = procedure( testProc2Signature );
    private final CallableProcedure exampleProc1 = procedure( exampleProc1Signature );
    private final CallableProcedure exampleProc2 = procedure( exampleProc2Signature );

    private static AuthManager authManager;

    private final String FAIL_EXECUTE_PROC = "Executing procedure is not allowed for user";

    @Override
    public void createSystemGraph( GraphDatabaseService graphDb )
    {
        try
        {
            testSupport.kernelToTest().registerProcedure( testProc1 );
            testSupport.kernelToTest().registerProcedure( testProc2 );
            testSupport.kernelToTest().registerProcedure( exampleProc1 );
            testSupport.kernelToTest().registerProcedure( exampleProc2 );
        }
        catch ( ProcedureException e )
        {
            throw new RuntimeException( e );
        }

        try ( Transaction tx = graphDb.beginTx() )
        {
            tx.execute( "REVOKE EXECUTE PROCEDURE * ON DBMS FROM PUBLIC" );
            tx.execute( "CREATE USER testUser SET PASSWORD 'abc123' CHANGE NOT REQUIRED" );
            tx.execute( "CREATE ROLE custom" );
            tx.execute( "GRANT ROLE custom TO testUser" );
            tx.execute( "GRANT EXECUTE PROCEDURE test.proc1 ON DBMS TO custom" );
            tx.execute( "GRANT EXECUTE PROCEDURE example.* ON DBMS TO custom" );
            tx.execute( "DENY EXECUTE PROCEDURE example.proc1 ON DBMS TO custom" );
            tx.commit();
        }

        authManager = ((GraphDatabaseAPI) graphDb).getDependencyResolver().resolveDependency( AuthManager.class );
    }

    @Test
    void shouldExecuteExplicitlyAllowedReadProcedure() throws Exception
    {
        LoginContext loginContext = getTestUserLoginContext();
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            QualifiedName procedureName = procedureName( "test", "proc1" );
            var procedureId = tx.procedures().procedureGet( procedureName ).id();
            tx.procedures().procedureCallRead( procedureId, new AnyValue[]{}, ProcedureCallContext.EMPTY );
        }
    }

    @Test
    void shouldExecuteExplicitlyAllowedWriteProcedure() throws Exception
    {
        LoginContext loginContext = getTestUserLoginContext();
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            QualifiedName procedureName = procedureName( "test", "proc1" );
            var procedureId = tx.procedures().procedureGet( procedureName ).id();
            tx.procedures().procedureCallWrite( procedureId, new AnyValue[]{}, ProcedureCallContext.EMPTY );
        }
    }

    @Test
    void shouldFailExecuteReadProcedureWithoutGrant() throws Exception
    {
        LoginContext loginContext = getTestUserLoginContext();
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            QualifiedName procedureName = procedureName( "test", "proc2" );
            var procedureId = tx.procedures().procedureGet( procedureName ).id();
            assertThatThrownBy(() -> tx.procedures().procedureCallRead( procedureId, new AnyValue[]{}, ProcedureCallContext.EMPTY ))
                    .hasMessageContaining( FAIL_EXECUTE_PROC );
        }
    }

    @Test
    void shouldFailExecuteWriteProcedureWithoutGrant() throws Exception
    {
        LoginContext loginContext = getTestUserLoginContext();
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            QualifiedName procedureName = procedureName( "test", "proc2" );
            var procedureId = tx.procedures().procedureGet( procedureName ).id();
            assertThatThrownBy(() -> tx.procedures().procedureCallWrite( procedureId, new AnyValue[]{}, ProcedureCallContext.EMPTY ))
                    .hasMessageContaining( FAIL_EXECUTE_PROC );
        }
    }

    @Test
    void shouldExecuteGlobbedPrivilege() throws Exception
    {
        LoginContext loginContext = getTestUserLoginContext();
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            QualifiedName procedureName = procedureName( "example", "proc2" );
            var procedureId = tx.procedures().procedureGet( procedureName ).id();
            tx.procedures().procedureCallRead( procedureId, new AnyValue[]{}, ProcedureCallContext.EMPTY );
        }
    }

    @Test
    void shouldFailExecuteDeniedProcedure() throws Exception
    {
        LoginContext loginContext = getTestUserLoginContext();
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            QualifiedName procedureName = procedureName( "example", "proc1" );
            var procedureId = tx.procedures().procedureGet( procedureName ).id();
            assertThatThrownBy(() -> tx.procedures().procedureCallRead( procedureId, new AnyValue[]{}, ProcedureCallContext.EMPTY ))
                    .hasMessageContaining( FAIL_EXECUTE_PROC );
        }
    }

    private LoginContext getLoginContext( String username ) throws InvalidAuthTokenException
    {
        return authManager.login( Map.of( "principal", username, "credentials", "abc123".getBytes( StandardCharsets.UTF_8 ), "scheme", "basic" ) );
    }

    private LoginContext getTestUserLoginContext() throws InvalidAuthTokenException
    {
        return getLoginContext( "testUser" );
    }

    private static CallableProcedure procedure( final ProcedureSignature signature )
    {
        return new CallableProcedure.BasicProcedure( signature )
        {
            @Override
            public RawIterator<AnyValue[],ProcedureException> apply( Context ctx, AnyValue[] input, ResourceTracker resourceTracker )
            {
                return RawIterator.<AnyValue[], ProcedureException>of( input );
            }
        };
    }
}
