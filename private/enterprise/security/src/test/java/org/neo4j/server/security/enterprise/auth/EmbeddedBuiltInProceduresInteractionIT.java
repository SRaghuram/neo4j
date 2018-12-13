/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import com.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.ToIntFunction;

import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.DoubleLatch;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

public class EmbeddedBuiltInProceduresInteractionIT extends BuiltInProceduresInteractionTestBase<EnterpriseLoginContext>
{

    @Override
    protected Object valueOf( Object obj )
    {
        if ( obj instanceof Integer )
        {
            return ((Integer) obj).longValue();
        }
        else
        {
            return obj;
        }
    }

    @Override
    protected NeoInteractionLevel<EnterpriseLoginContext> setUpNeoServer( Map<String, String> config ) throws Throwable
    {
        return new EmbeddedInteraction( config );
    }

    @Test
    void shouldNotListAnyQueriesIfNotAuthenticated()
    {
        EnterpriseLoginContext unAuthSubject = createFakeAnonymousEnterpriseLoginContext();
        GraphDatabaseFacade graph = neo.getLocalGraph();

        try ( InternalTransaction tx = graph
                .beginTransaction( KernelTransaction.Type.explicit, unAuthSubject ) )
        {
            Result result = graph.execute( tx, "CALL dbms.listQueries", EMPTY_MAP );
            assertFalse( result.hasNext() );
            tx.success();
        }
    }

    @Test
    void shouldNotKillQueryIfNotAuthenticated() throws Throwable
    {
        EnterpriseLoginContext unAuthSubject = createFakeAnonymousEnterpriseLoginContext();

        GraphDatabaseFacade graph = neo.getLocalGraph();
        DoubleLatch latch = new DoubleLatch( 2 );
        ThreadedTransaction<EnterpriseLoginContext> read = new ThreadedTransaction<>( neo, latch );
        String query = read.execute( threading, readSubject, "UNWIND [1,2,3] AS x RETURN x" );

        latch.startAndWaitForAllToStart();

        String id = extractQueryId( query );

        try ( InternalTransaction tx = graph.beginTransaction( KernelTransaction.Type.explicit, unAuthSubject ) )
        {
            graph.execute( tx, "CALL dbms.killQuery('" + id + "')", EMPTY_MAP );
            throw new AssertionError( "Expected exception to be thrown" );
        }
        catch ( QueryExecutionException e )
        {
            assertThat( e.getMessage(), containsString( PERMISSION_DENIED ) );
        }

        latch.finishAndWaitForAllToFinish();
        read.closeAndAssertSuccess();
    }

    private EnterpriseLoginContext createFakeAnonymousEnterpriseLoginContext()
    {
        return new EnterpriseLoginContext()
        {
            @Override
            public EnterpriseSecurityContext authorize( ToIntFunction<String> propertyIdLookup, String dbName )
            {
                return new EnterpriseSecurityContext( subject(), inner.mode(), Collections.emptySet(), false );
            }

            @Override
            public Set<String> roles()
            {
                return Collections.emptySet();
            }

            SecurityContext inner = AnonymousContext.none().authorize( s -> -1, GraphDatabaseSettings.DEFAULT_DATABASE_NAME );

            @Override
            public AuthSubject subject()
            {
                return inner.subject();
            }
        };
    }
}
