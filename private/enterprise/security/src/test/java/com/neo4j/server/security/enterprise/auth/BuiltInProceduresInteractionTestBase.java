/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.procedure.enterprise.builtin.DbmsQueryId;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableObject;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.database.SystemGraphComponent;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListenerAdapter;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.newapi.Operations;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.test.Barrier;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.rule.concurrent.ThreadingRule;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;

import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ADMIN;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLISHER;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.READER;
import static java.lang.String.format;
import static java.time.OffsetDateTime.from;
import static java.time.OffsetDateTime.now;
import static java.time.OffsetDateTime.ofInstant;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.core.Every.everyItem;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.internal.helpers.collection.Iterables.single;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.TRUE;
import static org.neo4j.test.matchers.CommonMatchers.matchesOneToOneInAnyOrder;

public abstract class BuiltInProceduresInteractionTestBase<S> extends ProcedureInteractionTestBase<S>
{
    private final String ROLE = "role1";
    private final String SUBJECT = "role1Subject";
    private final String PASSWORD = "abc";

    @Override
    Set<Class> defaultProcedures()
    {
        Set<Class> procs = super.defaultProcedures();
        procs.add( TestProcedures.class );
        return procs;
    }

    //---------- list running transactions -----------

    @Test
    void shouldListSelfTransaction()
    {
        assertSuccess( adminSubject, "CALL dbms.listTransactions()",
                r -> assertKeyIs( r, "username", "adminSubject" ) );
    }

    @Test
    void listBlockedTransactions() throws Throwable
    {
        assertEmpty( adminSubject, "CREATE (:MyNode {prop: 2})" );
        String firstModifier = "MATCH (n:MyNode) set n.prop=3";
        String secondModifier = "MATCH (n:MyNode) set n.prop=4";
        DoubleLatch latch = new DoubleLatch( 2 );
        DoubleLatch blockedModifierLatch = new DoubleLatch( 2 );
        OffsetDateTime startTime = getStartTime();

        ThreadedTransaction<S> tx = new ThreadedTransaction<>( neo, latch );
        tx.execute( threading, writeSubject, firstModifier );
        latch.start();
        latch.waitForAllToStart();

        ThreadedTransaction<S> tx2 = new ThreadedTransaction<>( neo, blockedModifierLatch );
        tx2.executeEarly( threading, writeSubject, KernelTransaction.Type.EXPLICIT, secondModifier );

        waitTransactionToStartWaitingForTheLock();

        blockedModifierLatch.startAndWaitForAllToStart();
        String query = "CALL dbms.listTransactions()";
        assertSuccess( adminSubject, query, r ->
        {
            List<Map<String,Object>> maps = collectResults( r );

            Matcher<Map<String,Object>> listTransaction = listedTransactionOfInteractionLevel( startTime,
                    "adminSubject", query );
            Matcher<Map<String,Object>> blockedQueryMatcher = allOf( anyOf( hasCurrentQuery( secondModifier ),
                    hasCurrentQuery( firstModifier ) ), hasStatus( "Blocked by:" ) );
            Matcher<Map<String,Object>> executedModifier = allOf( hasCurrentQuery(""), hasStatus( "Running" ) );

            assertThat( maps, matchesOneToOneInAnyOrder( listTransaction, blockedQueryMatcher, executedModifier ) );
        } );

        latch.finishAndWaitForAllToFinish();
        tx.closeAndAssertSuccess();
        blockedModifierLatch.finishAndWaitForAllToFinish();
    }

    @Test
    void listTransactionWithMetadata() throws Throwable
    {
        String setMetaDataQuery = "CALL tx.setMetaData( { realUser: 'MyMan' } )";
        String matchQuery = "MATCH (n) RETURN n";
        String listTransactionsQuery = "CALL dbms.listTransactions()";

        DoubleLatch latch = new DoubleLatch( 2 );
        OffsetDateTime startTime = getStartTime();

        ThreadedTransaction<S> tx = new ThreadedTransaction<>( neo, latch );
        tx.execute( threading, writeSubject, setMetaDataQuery, matchQuery );

        latch.startAndWaitForAllToStart();

        assertSuccess( adminSubject, listTransactionsQuery, r ->
        {
            List<Map<String,Object>> maps = collectResults( r );
            Matcher<Map<String,Object>> thisTransaction =
                    listedTransactionOfInteractionLevel( startTime, "adminSubject", listTransactionsQuery );
            Matcher<Map<String,Object>> matchQueryTransactionMatcher =
                    listedTransactionWithMetaData( startTime, "writeSubject", matchQuery,  map( "realUser", "MyMan" ) );

            assertThat( maps, matchesOneToOneInAnyOrder( thisTransaction, matchQueryTransactionMatcher ) );
        } );

        latch.finishAndWaitForAllToFinish();
        tx.closeAndAssertSuccess();
    }

    @Test
    void listTransactionIncludeClosing() throws Throwable
    {
        String listTransactionsQuery = "CALL dbms.listTransactions()";
        String createQuery = "CREATE (n)";
        OffsetDateTime startTime = getStartTime();
        CountDownLatch committing = new CountDownLatch( 1 );
        CountDownLatch commit = new CountDownLatch( 1 );
        BlockingCommitTxListener txListener = new BlockingCommitTxListener( committing, commit );
        String databaseName = neo.getLocalGraph().databaseName();
        neo.registerTransactionEventListener( databaseName, txListener );

        DoubleLatch latch = new DoubleLatch( 2 );
        ThreadedTransaction<S> tx = new ThreadedTransaction<>( neo, latch );
        tx.execute( threading, writeSubject, createQuery );
        latch.startAndWaitForAllToStart();
        latch.finishAndWaitForAllToFinish();

        committing.await();

        assertSuccess( adminSubject, listTransactionsQuery, r ->
        {
            commit.countDown();
            List<Map<String,Object>> maps = collectResults( r );
            Matcher<Map<String,Object>> thisTransaction = listedTransactionOfInteractionLevel( startTime, "adminSubject", listTransactionsQuery );
            Matcher<Map<String,Object>> closingQueryMatcher = allOf( hasStartTimeAfter( startTime ), hasUsername( "writeSubject" ), hasStatus( "Closing" ) );

            assertThat( maps, matchesOneToOneInAnyOrder( thisTransaction, closingQueryMatcher ) );
        } );

        tx.closeAndAssertSuccess();
        neo.unregisterTransactionEventListener( databaseName, txListener );
    }

    private static class BlockingCommitTxListener extends TransactionEventListenerAdapter<Object>
    {
        private final CountDownLatch committing;
        private final CountDownLatch commit;

        private BlockingCommitTxListener( CountDownLatch committing, CountDownLatch commit )
        {
            this.committing = committing;
            this.commit = commit;
        }

        @Override
        public Object beforeCommit( TransactionData data, Transaction transaction, GraphDatabaseService databaseService ) throws Exception
        {
            committing.countDown();
            commit.await();
            return null;
        }
    }

    @SuppressWarnings( "unchecked" )
    @Test
    void listTransactionWithNullInMetadata()
    {
        GraphDatabaseFacade graph = neo.getLocalGraph();

        // null as value
        try ( Transaction tx = graph.beginTx() )
        {
            tx.execute( "CALL tx.setMetaData( { realUser: null })" );
            assertNull( getResultRowForMetadataQuery( tx ).get( "realUser" ) );
        }
        // null as key
        try ( Transaction tx = graph.beginTx() )
        {
            tx.execute( "CALL tx.setMetaData( { null: 'success' } )" );
            assertEquals( "success", getResultRowForMetadataQuery( tx ).get( "null" ) );
        }

        // nesting map with null as value
        try ( Transaction tx = graph.beginTx() )
        {
            tx.execute( "CALL tx.setMetaData( { nesting: { inner: null } } )" );
            assertNull( ((Map<String,Object>) getResultRowForMetadataQuery( tx ).get( "nesting" )).get( "inner" ) );
        }

        // nesting map with null as key
        try ( Transaction tx = graph.beginTx() )
        {
            tx.execute( "CALL tx.setMetaData( { nesting: { null: 'success' } } )" );
            assertEquals( "success", ((Map<String,Object>) getResultRowForMetadataQuery( tx ).get( "nesting" )).get( "null" ) );
        }
    }

    @Test
    void listTransactionsWithConnectionsDetail() throws Throwable
    {
        String matchQuery = "MATCH (n) RETURN n";
        String listTransactionsQuery = "CALL dbms.listTransactions()";

        DoubleLatch latch = new DoubleLatch( 2 );

        ThreadedTransaction<S> tx = new ThreadedTransaction<>( neo, latch );
        tx.execute( threading, writeSubject, matchQuery );

        latch.startAndWaitForAllToStart();

        assertSuccess( adminSubject, listTransactionsQuery, r ->
        {
            List<Map<String,Object>> maps = collectResults( r );
            assertThat( maps, everyItem( hasProtocol( neo.getConnectionProtocol() ) ) );
        } );

        latch.finishAndWaitForAllToFinish();
        tx.closeAndAssertSuccess();
    }

    @Test
    void listAllTransactionsWhenRunningAsAdmin() throws Throwable
    {
        DoubleLatch latch = new DoubleLatch( 3, true );
        OffsetDateTime startTime = getStartTime();

        ThreadedTransaction<S> read1 = new ThreadedTransaction<>( neo, latch );
        ThreadedTransaction<S> read2 = new ThreadedTransaction<>( neo, latch );

        String q1 = read1.execute( threading, readSubject, "UNWIND [1,2,3] AS x RETURN x" );
        String q2 = read2.execute( threading, writeSubject, "UNWIND [4,5,6] AS y RETURN y" );
        latch.startAndWaitForAllToStart();

        String query = "CALL dbms.listTransactions()";
        assertSuccess( adminSubject, query, r ->
        {
            List<Map<String,Object>> maps = collectResults( r );

            Matcher<Map<String,Object>> thisTransaction = listedTransactionOfInteractionLevel( startTime, "adminSubject", query );
            Matcher<Map<String,Object>> matcher1 = listedTransaction( startTime, "readSubject", q1 );
            Matcher<Map<String,Object>> matcher2 = listedTransaction( startTime, "writeSubject", q2 );

            assertThat( maps, matchesOneToOneInAnyOrder( matcher1, matcher2, thisTransaction ) );
        } );

        latch.finishAndWaitForAllToFinish();

        read1.closeAndAssertSuccess();
        read2.closeAndAssertSuccess();
    }

    @Test
    void listAllowedTransactions() throws Throwable
    {
        authDisabledAdminstrationCommand( "CREATE USER alice SET PASSWORD 'foo' CHANGE NOT REQUIRED" );
        authDisabledAdminstrationCommand( "CREATE ROLE custom" );
        authDisabledAdminstrationCommand( "GRANT ROLE custom TO alice" );
        authDisabledAdminstrationCommand( "GRANT ACCESS ON DEFAULT DATABASE TO custom" );
        authDisabledAdminstrationCommand( "GRANT SHOW TRANSACTION (readSubject,editorSubject) ON DATABASE * TO custom" );
        S subject = neo.login( "alice", "foo" );

        DoubleLatch latch = new DoubleLatch( 4, true );
        OffsetDateTime startTime = getStartTime();

        ThreadedTransaction<S> read1 = new ThreadedTransaction<>( neo, latch );
        ThreadedTransaction<S> read2 = new ThreadedTransaction<>( neo, latch );
        ThreadedTransaction<S> read3 = new ThreadedTransaction<>( neo, latch );

        String q1 = read1.execute( threading, readSubject, "UNWIND [1,2,3] AS x RETURN x" );
        String q2 = read2.execute( threading, editorSubject, "UNWIND [4,5,6] AS y RETURN y" );
        read3.execute( threading, writeSubject, "UNWIND [7,8,9] AS z RETURN z" );
        latch.startAndWaitForAllToStart();

        String query = "CALL dbms.listTransactions()";
        assertSuccess( subject, query, r ->
        {
            List<Map<String,Object>> maps = collectResults( r );

            Matcher<Map<String,Object>> thisTransaction = listedTransactionOfInteractionLevel( startTime, "alice", query );
            Matcher<Map<String,Object>> matcher1 = listedTransaction( startTime, "readSubject", q1 );
            Matcher<Map<String,Object>> matcher2 = listedTransaction( startTime, "editorSubject", q2 );

            assertThat( maps, matchesOneToOneInAnyOrder( matcher1, matcher2, thisTransaction ) );
        } );

        latch.finishAndWaitForAllToFinish();

        read1.closeAndAssertSuccess();
        read2.closeAndAssertSuccess();
        read3.closeAndAssertSuccess();
    }

    @Test
    void shouldNotListTransactionsWhenDenied() throws Throwable
    {
        authDisabledAdminstrationCommand( "CREATE USER alice SET PASSWORD 'foo' CHANGE NOT REQUIRED" );
        authDisabledAdminstrationCommand( "CREATE ROLE custom AS COPY OF admin" );
        authDisabledAdminstrationCommand( "GRANT ROLE custom TO alice" );
        authDisabledAdminstrationCommand( "DENY SHOW TRANSACTION (editorSubject) ON DEFAULT DATABASE TO custom" );
        S subject = neo.login( "alice", "foo" );

        DoubleLatch latch = new DoubleLatch( 4, true );
        OffsetDateTime startTime = getStartTime();

        ThreadedTransaction<S> read1 = new ThreadedTransaction<>( neo, latch );
        ThreadedTransaction<S> read2 = new ThreadedTransaction<>( neo, latch );
        ThreadedTransaction<S> read3 = new ThreadedTransaction<>( neo, latch );

        String q1 = read1.execute( threading, readSubject, "UNWIND [1,2,3] AS x RETURN x" );
        read2.execute( threading, editorSubject, "UNWIND [4,5,6] AS y RETURN y" );
        String q3 = read3.execute( threading, writeSubject, "UNWIND [7,8,9] AS z RETURN z" );
        latch.startAndWaitForAllToStart();

        String query = "CALL dbms.listTransactions()";
        assertSuccess( subject, query, r ->
        {
            List<Map<String,Object>> maps = collectResults( r );

            Matcher<Map<String,Object>> thisTransaction = listedTransactionOfInteractionLevel( startTime, "alice", query );
            Matcher<Map<String,Object>> matcher1 = listedTransaction( startTime, "readSubject", q1 );
            Matcher<Map<String,Object>> matcher2 = listedTransaction( startTime, "writeSubject", q3 );

            assertThat( maps, matchesOneToOneInAnyOrder( matcher1, matcher2, thisTransaction ) );
        } );

        latch.finishAndWaitForAllToFinish();

        read1.closeAndAssertSuccess();
        read2.closeAndAssertSuccess();
        read3.closeAndAssertSuccess();
    }

    @Test
    void listAllowedTransactionsDifferentDatabases() throws Throwable
    {
        authDisabledAdminstrationCommand( "CREATE DATABASE foo" );
        authDisabledAdminstrationCommand( "CREATE USER alice SET PASSWORD 'foo' CHANGE NOT REQUIRED" );
        authDisabledAdminstrationCommand( "CREATE ROLE custom" );
        authDisabledAdminstrationCommand( "GRANT ACCESS ON DATABASE * TO custom" );
        authDisabledAdminstrationCommand( "GRANT ROLE custom TO alice" );
        authDisabledAdminstrationCommand( "GRANT SHOW TRANSACTION ON DATABASE foo TO custom" );
        S subject = neo.login( "alice", "foo" );

        DoubleLatch latch = new DoubleLatch( 3, true );
        OffsetDateTime startTime = getStartTime();

        ThreadedTransaction<S> tx1 = new ThreadedTransaction<>( neo, latch );
        ThreadedTransaction<S> tx2 = new ThreadedTransaction<>( neo, latch );

        tx1.execute( threading, DEFAULT_DATABASE_NAME, readSubject, "UNWIND [1,2,3] AS x RETURN x" );
        String q2 = tx2.execute( threading, "foo", readSubject, "UNWIND [4,5,6] AS y RETURN y" );
        latch.startAndWaitForAllToStart();

        String query = "CALL dbms.listTransactions()";
        assertSuccess( subject, DEFAULT_DATABASE_NAME, query, r ->
        {
            List<Map<String,Object>> maps = collectResults( r );

            Matcher<Map<String,Object>> thisTransaction = listedTransactionOfInteractionLevel( startTime, "alice", query );

            assertThat( maps, matchesOneToOneInAnyOrder( thisTransaction ) );
        } );

        assertSuccess( subject, "foo", query, r ->
        {
            List<Map<String,Object>> maps = collectResults( r );

            Matcher<Map<String,Object>> thisTransaction = listedTransactionOfInteractionLevel( startTime, "alice", query );
            Matcher<Map<String,Object>> matcher1 = listedTransaction( startTime, "readSubject", q2 );

            assertThat( maps, matchesOneToOneInAnyOrder( matcher1, thisTransaction ) );
        } );

        latch.finishAndWaitForAllToFinish();

        tx1.closeAndAssertSuccess();
        tx2.closeAndAssertSuccess();
    }

    @Test
    void listTransactionInitialisationTraceWhenAvailable() throws Throwable
    {
        neo.tearDown();
        neo = setUpNeoServer( Map.of( GraphDatabaseSettings.transaction_tracing_level, GraphDatabaseSettings.TransactionTracingLevel.ALL.name(),
                                                 GraphDatabaseSettings.auth_enabled, FALSE ) );
        DoubleLatch latch = new DoubleLatch( 2, true );
        try
        {
            ThreadedTransaction<S> read1 = new ThreadedTransaction<>( neo, latch );
            read1.execute( threading, neo.login( "user1", "" ), "UNWIND [1,2,3] AS x RETURN x" );
            latch.startAndWaitForAllToStart();

            String query = "CALL dbms.listTransactions()";
            assertSuccess( neo.login( "admin", "" ), query, r ->
            {
                List<Object> results = getObjectsAsList( r, "initializationStackTrace" );
                for ( Object result : results )
                {
                    assertThat( result.toString(), Matchers.containsString( "Transaction initialization stacktrace" ) );
                }
            } );
        }
        finally
        {
            latch.finishAndWaitForAllToFinish();
        }
    }

    @Test
    void shouldOnlyListOwnTransactionsWhenNotRunningAsAdmin() throws Throwable
    {
        DoubleLatch latch = new DoubleLatch( 3, true );
        OffsetDateTime startTime = getStartTime();
        ThreadedTransaction<S> read1 = new ThreadedTransaction<>( neo, latch );
        ThreadedTransaction<S> read2 = new ThreadedTransaction<>( neo, latch );

        String q1 = read1.execute( threading, readSubject, "UNWIND [1,2,3] AS x RETURN x" );
        read2.execute( threading, writeSubject, "UNWIND [4,5,6] AS y RETURN y" );
        latch.startAndWaitForAllToStart();

        String query = "CALL dbms.listTransactions()";
        assertSuccess( readSubject, query, r ->
        {
            List<Map<String,Object>> maps = collectResults( r );

            Matcher<Map<String,Object>> thisTransaction = listedTransaction( startTime, "readSubject", query );
            Matcher<Map<String,Object>> queryMatcher = listedTransaction( startTime, "readSubject", q1 );

            assertThat( maps, matchesOneToOneInAnyOrder( queryMatcher, thisTransaction ) );
        } );

        latch.finishAndWaitForAllToFinish();

        read1.closeAndAssertSuccess();
        read2.closeAndAssertSuccess();
    }

    @Test
    void shouldListAllTransactionsWithAuthDisabled() throws Throwable
    {
        neo.tearDown();
        neo = setUpNeoServer( Map.of( GraphDatabaseSettings.auth_enabled, FALSE ) );

        DoubleLatch latch = new DoubleLatch( 2, true );
        OffsetDateTime startTime = getStartTime();

        ThreadedTransaction<S> read = new ThreadedTransaction<>( neo, latch );

        String q = read.execute( threading, neo.login( "user1", "" ), "UNWIND [1,2,3] AS x RETURN x" );
        latch.startAndWaitForAllToStart();

        String query = "CALL dbms.listTransactions()";
        try
        {
            assertSuccess( neo.login( "admin", "" ), query, r ->
            {
                List<Map<String,Object>> maps = collectResults( r );

                Matcher<Map<String,Object>> thisQuery = listedTransactionOfInteractionLevel( startTime, "", query ); // admin
                Matcher<Map<String,Object>> matcher1 = listedTransaction( startTime, "", q ); // user1
                assertThat( maps, matchesOneToOneInAnyOrder( matcher1, thisQuery ) );
            } );
        }
        finally
        {
            latch.finishAndWaitForAllToFinish();
        }
        read.closeAndAssertSuccess();
    }

    @Test
    void allDatabasePrivilegeShouldNotImplyListTransaction() throws Throwable
    {
        authDisabledAdminstrationCommand( "CREATE USER alice SET PASSWORD 'foo' CHANGE NOT REQUIRED" );
        authDisabledAdminstrationCommand( "CREATE ROLE custom" );
        authDisabledAdminstrationCommand( "GRANT ROLE custom TO alice" );
        authDisabledAdminstrationCommand( "GRANT ALL ON DEFAULT DATABASE TO custom" );
        S subject = neo.login( "alice", "foo" );

        DoubleLatch latch = new DoubleLatch( 3, true );
        OffsetDateTime startTime = getStartTime();
        ThreadedTransaction<S> read1 = new ThreadedTransaction<>( neo, latch );
        ThreadedTransaction<S> read2 = new ThreadedTransaction<>( neo, latch );

        String q1 = read1.execute( threading, subject, "UNWIND [1,2,3] AS x RETURN x" );
        read2.execute( threading, readSubject, "UNWIND [4,5,6] AS y RETURN y" );
        latch.startAndWaitForAllToStart();

        String query = "CALL dbms.listTransactions()";
        assertSuccess( subject, query, r ->
        {
            List<Map<String,Object>> maps = collectResults( r );

            Matcher<Map<String,Object>> thisTransaction = listedTransaction( startTime, "alice", query );
            Matcher<Map<String,Object>> queryMatcher = listedTransaction( startTime, "alice", q1 );

            assertThat( maps, matchesOneToOneInAnyOrder( queryMatcher, thisTransaction ) );
        } );

        latch.finishAndWaitForAllToFinish();

        read1.closeAndAssertSuccess();
        read2.closeAndAssertSuccess();
    }

    @Test
    void listMemoryPools()
    {
        assertSuccess( adminSubject, "CALL dbms.listPools()", r ->
        {
            List<Map<String,Object>> maps = collectResults( r );
            assertEquals( 8, maps.size() );
            assertTrue( maps.stream().anyMatch( map -> "Transaction".equals( map.get( "group" ) ) ) );
            assertTrue( maps.stream().anyMatch( map -> "Transaction".equals( map.get( "group" ) ) && "system".equals( map.get( "databaseName" ) ) ) );
            assertTrue( maps.stream().anyMatch( map -> "Transaction".equals( map.get( "group" ) ) && "neo4j".equals( map.get( "databaseName" ) ) ) );
            assertTrue( maps.stream().anyMatch( map -> "Other".equals( map.get( "group" ) ) ) );
            assertTrue( maps.stream().anyMatch( map -> "Other".equals( map.get( "group" ) ) && "system".equals( map.get( "databaseName" ) ) ) );
            assertTrue( maps.stream().anyMatch( map -> "Other".equals( map.get( "group" ) ) && "neo4j".equals( map.get( "databaseName" ) ) ) );
            assertTrue( maps.stream().anyMatch( map -> "Netty".equals( map.get( "group" ) ) ) );
            assertTrue( maps.stream().anyMatch( map -> "Page Cache".equals( map.get( "group" ) ) ) );
        } );
    }

    @Test
    void killAlreadyTerminatedTransactionEndsSuccessfully()
    {
        DoubleLatch latch = new DoubleLatch( 2, true );
        try
        {
            ThreadedTransaction<S> read1 = new ThreadedTransaction<>( neo, latch );

            String q1 = read1.execute( threading, readSubject, "UNWIND [1,2,3] AS x RETURN x" );
            latch.startAndWaitForAllToStart();

            String listTransactionQuery = "CALL dbms.listTransactions()";
            String unwindTransactionId = getTransactionIdExecutingQuery( q1, listTransactionQuery, readSubject );
            String killTransactionQueryTemplate = "CALL dbms.killTransaction('%s')";
            assertSuccess( readSubject, format( killTransactionQueryTemplate, unwindTransactionId ), r ->
            {
                List<Map<String,Object>> killQueryResult = collectResults( r );
                assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasUsername( "readSubject" ) ) );
                assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasResultEntry( "message", "Transaction terminated." ) ) );
                assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasResultEntry( "transactionId", unwindTransactionId ) ) );
            } );

            assertSuccess( readSubject, format( killTransactionQueryTemplate, unwindTransactionId ), r ->
            {
                List<Map<String,Object>> killQueryResult = collectResults( r );
                assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasUsername( "readSubject" ) ) );
                assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasResultEntry( "message", "Transaction terminated." ) ) );
                assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasResultEntry( "transactionId", unwindTransactionId ) ) );
            } );
        }
        finally
        {
            latch.finishAndWaitForAllToFinish();
        }

    }

    @Test
    void failToKillTransactionForOtherUserByNonAdmin()
    {
        DoubleLatch latch = new DoubleLatch( 2, true );
        try
        {
            ThreadedTransaction<S> read1 = new ThreadedTransaction<>( neo, latch );

            String q1 = read1.execute( threading, writeSubject, "UNWIND [1,2,3] AS x RETURN x" );
            latch.startAndWaitForAllToStart();

            String listTransactionQuery = "CALL dbms.listTransactions()";
            String unwindTransactionId = getTransactionIdExecutingQuery( q1, listTransactionQuery, writeSubject );
            String killTransactionQueryTemplate = "CALL dbms.killTransaction('%s')";
            assertSuccess( readSubject, format( killTransactionQueryTemplate, unwindTransactionId ), r ->
            {
                List<Map<String,Object>> killQueryResult = collectResults( r );
                assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasUsername( "readSubject" ) ) );
                assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasResultEntry( "message", "Transaction not found." ) ) );
                assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasResultEntry( "transactionId", unwindTransactionId ) ) );
            } );
        }
        finally
        {
            latch.finishAndWaitForAllToFinish();
        }
    }

    @Test
    void killAnyTransactionWithAuthDisabled() throws Throwable
    {
        neo.tearDown();
        neo = setUpNeoServer( Map.of( GraphDatabaseSettings.auth_enabled, FALSE ) );
        DoubleLatch latch = new DoubleLatch( 2, true );
        try
        {
            ThreadedTransaction<S> read = new ThreadedTransaction<>( neo, latch );

            String q1 = read.execute( threading, neo.login( "user1", "" ), "UNWIND [1,2,3] AS x RETURN x" );
            latch.startAndWaitForAllToStart();

            S admin = neo.login( "admin", "" );
            String listTransactionQuery = "CALL dbms.listTransactions()";
            String unwindTransactionId = getTransactionIdExecutingQuery( q1, listTransactionQuery, admin );
            String killTransactionQueryTemplate = "CALL dbms.killTransaction('%s')";
            assertSuccess( admin, format( killTransactionQueryTemplate, unwindTransactionId ), r ->
            {
                List<Map<String,Object>> killQueryResult = collectResults( r );
                assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasResultEntry( "message", "Transaction terminated." ) ) );
                assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasResultEntry( "transactionId", unwindTransactionId ) ) );
            } );
        }
        finally
        {
            latch.finishAndWaitForAllToFinish();
        }
    }

    @Test
    void killTransactionMarksTransactionForTermination()
    {
        DoubleLatch latch = new DoubleLatch( 2, true );
        try
        {
            ThreadedTransaction<S> read1 = new ThreadedTransaction<>( neo, latch );

            String q1 = read1.execute( threading, readSubject, "UNWIND [1,2,3] AS x RETURN x" );
            latch.startAndWaitForAllToStart();

            String listTransactionQuery = "CALL dbms.listTransactions()";
            String unwindTransactionId = getTransactionIdExecutingQuery( q1, listTransactionQuery, readSubject );
            String killTransactionQueryTemplate = "CALL dbms.killTransaction('%s')";
            assertSuccess( readSubject, format( killTransactionQueryTemplate, unwindTransactionId ), r ->
            {
                List<Map<String,Object>> killQueryResult = collectResults( r );
                assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasUsername( "readSubject" ) ) );
                assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasResultEntry( "message", "Transaction terminated." ) ) );
                assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasResultEntry( "transactionId", unwindTransactionId ) ) );
            } );
        }
        finally
        {
            latch.finishAndWaitForAllToFinish();
        }
    }

    private String getTransactionIdExecutingQuery( String q1, String listTransactionQuery, S subject )
    {
        return getTransactionIdExecutingQuery( q1, DEFAULT_DATABASE_NAME, listTransactionQuery, subject );
    }

    private String getTransactionIdExecutingQuery( String q1, String database, String listTransactionQuery, S subject )
    {
        MutableObject<String> transactionIdContainer = new MutableObject<>();
        assertSuccess( subject, database, listTransactionQuery, r ->
        {
            List<Map<String,Object>> listTransactionsResult = collectResults( r );
            String transactionId = listTransactionsResult.stream().filter( map -> map.containsValue( q1 ) ).map(
                    map -> map.get( "transactionId" ).toString() ).findFirst().orElseThrow( () -> new RuntimeException( "Expected unwind query not found." ) );
            transactionIdContainer.setValue( transactionId );
        } );
        return transactionIdContainer.getValue();
    }

    @Test
    void killNotExistingTransaction()
    {
        String query = "CALL dbms.killTransaction('neo4j-transaction-17')";
        assertSuccess( readSubject, query, r ->
        {
            List<Map<String,Object>> result = collectResults( r );
            assertThat( result, matchesOneToOneInAnyOrder( hasUsername( "readSubject" ) ) );
            assertThat( result, matchesOneToOneInAnyOrder( hasResultEntry( "message", "Transaction not found." ) ) );
            assertThat( result, matchesOneToOneInAnyOrder( hasResultEntry( "transactionId", "neo4j-transaction-17" ) ) );
        } );
    }

    @Test
    void killAllowedTransactions() throws Exception
    {
        authDisabledAdminstrationCommand( "CREATE USER alice SET PASSWORD 'foo' CHANGE NOT REQUIRED" );
        authDisabledAdminstrationCommand( "CREATE ROLE custom" );
        authDisabledAdminstrationCommand( "GRANT ROLE custom TO alice" );
        authDisabledAdminstrationCommand( "GRANT ACCESS ON DEFAULT DATABASE TO custom" );
        authDisabledAdminstrationCommand( "GRANT TERMINATE TRANSACTION (readSubject) ON DATABASE * TO custom" );
        S subject = neo.login( "alice", "foo" );

        DoubleLatch latch = new DoubleLatch( 3, true );
        try
        {
            ThreadedTransaction<S> tx1 = new ThreadedTransaction<>( neo, latch );
            ThreadedTransaction<S> tx2 = new ThreadedTransaction<>( neo, latch );

            String q1 = tx1.execute( threading, readSubject, "UNWIND [1,2,3] AS x RETURN x" );
            String q2 = tx2.execute( threading, writeSubject, "UNWIND [4,5,6] AS y RETURN y" );
            latch.startAndWaitForAllToStart();

            String listTransactionQuery = "CALL dbms.listTransactions()";
            String txId1 = getTransactionIdExecutingQuery( q1, listTransactionQuery, adminSubject );
            String txId2 = getTransactionIdExecutingQuery( q2, listTransactionQuery, adminSubject );
            String killTransactionQueryTemplate = "CALL dbms.killTransaction('%s')";
            assertSuccess( subject, format( killTransactionQueryTemplate, txId1 ), r ->
            {
                List<Map<String,Object>> killQueryResult = collectResults( r );
                assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasUsername( "readSubject" ) ) );
                assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasResultEntry( "message", "Transaction terminated." ) ) );
                assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasResultEntry( "transactionId", txId1 ) ) );
            } );
            assertSuccess( subject, format( killTransactionQueryTemplate, txId2 ), r ->
            {
                List<Map<String,Object>> killQueryResult = collectResults( r );
                assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasUsername( "alice" ) ) );
                assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasResultEntry( "message", "Transaction not found." ) ) );
                assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasResultEntry( "transactionId", txId2 ) ) );
            } );
        }
        finally
        {
            latch.finishAndWaitForAllToFinish();
        }
    }

    @Test
    void shouldFailKillDeniedTransactions() throws Exception
    {
        authDisabledAdminstrationCommand( "CREATE USER alice SET PASSWORD 'foo' CHANGE NOT REQUIRED" );
        authDisabledAdminstrationCommand( "CREATE ROLE custom AS COPY OF admin" );
        authDisabledAdminstrationCommand( "GRANT ROLE custom TO alice" );
        authDisabledAdminstrationCommand( "DENY TERMINATE TRANSACTION (writeSubject) ON DEFAULT DATABASE TO custom" );
        S subject = neo.login( "alice", "foo" );

        DoubleLatch latch = new DoubleLatch( 3, true );
        try
        {
            ThreadedTransaction<S> tx1 = new ThreadedTransaction<>( neo, latch );
            ThreadedTransaction<S> tx2 = new ThreadedTransaction<>( neo, latch );

            String q1 = tx1.execute( threading, readSubject, "UNWIND [1,2,3] AS x RETURN x" );
            String q2 = tx2.execute( threading, writeSubject, "UNWIND [4,5,6] AS y RETURN y" );
            latch.startAndWaitForAllToStart();

            String listTransactionQuery = "CALL dbms.listTransactions()";
            String txId1 = getTransactionIdExecutingQuery( q1, listTransactionQuery, adminSubject );
            String txId2 = getTransactionIdExecutingQuery( q2, listTransactionQuery, adminSubject );
            String killTransactionQueryTemplate = "CALL dbms.killTransaction('%s')";
            assertSuccess( subject, format( killTransactionQueryTemplate, txId1 ), r ->
            {
                List<Map<String,Object>> killQueryResult = collectResults( r );
                assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasUsername( "readSubject" ) ) );
                assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasResultEntry( "message", "Transaction terminated." ) ) );
                assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasResultEntry( "transactionId", txId1 ) ) );
            } );
            assertSuccess( subject, format( killTransactionQueryTemplate, txId2 ), r ->
            {
                List<Map<String,Object>> killQueryResult = collectResults( r );
                assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasUsername( "alice" ) ) );
                assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasResultEntry( "message", "Transaction not found." ) ) );
                assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasResultEntry( "transactionId", txId2 ) ) );
            } );
        }
        finally
        {
            latch.finishAndWaitForAllToFinish();
        }
    }

    @Test
    void killAllowedTransactionsDifferentDatabases() throws Exception
    {
        authDisabledAdminstrationCommand( "CREATE USER alice SET PASSWORD 'foo' CHANGE NOT REQUIRED" );
        authDisabledAdminstrationCommand( "CREATE ROLE custom" );
        authDisabledAdminstrationCommand( "GRANT ROLE custom TO alice" );
        authDisabledAdminstrationCommand( "GRANT ACCESS ON DATABASE * TO custom" );
        authDisabledAdminstrationCommand( "CREATE DATABASE foo" );
        authDisabledAdminstrationCommand( "GRANT TERMINATE TRANSACTION (*) ON DATABASE foo TO custom" );
        S subject = neo.login( "alice", "foo" );

        DoubleLatch latch = new DoubleLatch( 3, true );
        try
        {
            ThreadedTransaction<S> tx1 = new ThreadedTransaction<>( neo, latch );
            ThreadedTransaction<S> tx2 = new ThreadedTransaction<>( neo, latch );

            String q1 = tx1.execute( threading, DEFAULT_DATABASE_NAME, readSubject, "UNWIND [1,2,3] AS x RETURN x" );
            String q2 = tx2.execute( threading, "foo", readSubject, "UNWIND [4,5,6] AS y RETURN y" );
            latch.startAndWaitForAllToStart();

            String listTransactionQuery = "CALL dbms.listTransactions()";
            String txId1 = getTransactionIdExecutingQuery( q1, DEFAULT_DATABASE_NAME, listTransactionQuery, adminSubject );
            String txId2 = getTransactionIdExecutingQuery( q2, "foo", listTransactionQuery, adminSubject );
            String killTransactionQueryTemplate = "CALL dbms.killTransaction('%s')";
            assertSuccess( subject, DEFAULT_DATABASE_NAME, format( killTransactionQueryTemplate, txId1 ), r ->
            {
                List<Map<String,Object>> killQueryResult = collectResults( r );
                assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasUsername( "alice" ) ) );
                assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasResultEntry( "message", "Transaction not found." ) ) );
                assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasResultEntry( "transactionId", txId1 ) ) );
            } );
            assertSuccess( subject, "foo", format( killTransactionQueryTemplate, txId2 ), r ->
            {
                List<Map<String,Object>> killQueryResult = collectResults( r );
                assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasUsername( "readSubject" ) ) );
                assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasResultEntry( "message", "Transaction terminated." ) ) );
                assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasResultEntry( "transactionId", txId2 ) ) );
            } );
        }
        finally
        {
            latch.finishAndWaitForAllToFinish();
        }
    }

    @Test
    void allDatabasePrivilegeShouldNotImplyKillTransaction() throws Throwable
    {
        authDisabledAdminstrationCommand( "CREATE USER alice SET PASSWORD 'foo' CHANGE NOT REQUIRED" );
        authDisabledAdminstrationCommand( "CREATE ROLE custom" );
        authDisabledAdminstrationCommand( "GRANT ROLE custom TO alice" );
        authDisabledAdminstrationCommand( "GRANT ALL ON DEFAULT DATABASE TO custom" );
        S subject = neo.login( "alice", "foo" );

        DoubleLatch latch = new DoubleLatch( 2, true );
        try
        {
            ThreadedTransaction<S> read1 = new ThreadedTransaction<>( neo, latch );

            String q1 = read1.execute( threading, writeSubject, "UNWIND [1,2,3] AS x RETURN x" );
            latch.startAndWaitForAllToStart();

            String listTransactionQuery = "CALL dbms.listTransactions()";
            String unwindTransactionId = getTransactionIdExecutingQuery( q1, listTransactionQuery, writeSubject );
            String killTransactionQueryTemplate = "CALL dbms.killTransaction('%s')";
            assertSuccess( subject, format( killTransactionQueryTemplate, unwindTransactionId ), r ->
            {
                List<Map<String,Object>> killQueryResult = collectResults( r );
                assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasUsername( "alice" ) ) );
                assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasResultEntry( "message", "Transaction not found." ) ) );
                assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasResultEntry( "transactionId", unwindTransactionId ) ) );
            } );
        }
        finally
        {
            latch.finishAndWaitForAllToFinish();
        }
    }

    //---------- list running queries -----------

    @Test
    void shouldListAllQueryIncludingMetaData() throws Throwable
    {
        String setMetaDataQuery = "CALL tx.setMetaData( { realUser: 'MyMan' } )";
        String matchQuery = "MATCH (n) RETURN n";
        String listQueriesQuery = "CALL dbms.listQueries()";

        DoubleLatch latch = new DoubleLatch( 2 );
        OffsetDateTime startTime = now( ZoneOffset.UTC );

        ThreadedTransaction<S> tx = new ThreadedTransaction<>( neo, latch );
        tx.execute( threading, writeSubject, setMetaDataQuery, matchQuery );

        latch.startAndWaitForAllToStart();

        assertSuccess( adminSubject, listQueriesQuery, r ->
        {
            List<Map<String,Object>> maps = collectResults( r );
            Matcher<Map<String,Object>> thisQuery =
                    listedQueryOfInteractionLevel( startTime, "adminSubject", listQueriesQuery );
            Matcher<Map<String,Object>> matchQueryMatcher =
                    listedQueryWithMetaData( startTime, "writeSubject", matchQuery, map( "realUser", "MyMan" ) );

            assertThat( maps, matchesOneToOneInAnyOrder( thisQuery, matchQueryMatcher ) );
        } );

        latch.finishAndWaitForAllToFinish();
        tx.closeAndAssertSuccess();
    }

    @Test
    void shouldListAllQueryWithConnectionDetails() throws Throwable
    {
        String matchQuery = "MATCH (n) RETURN n";
        String listQueriesQuery = "CALL dbms.listQueries()";

        DoubleLatch latch = new DoubleLatch( 2 );

        ThreadedTransaction<S> tx = new ThreadedTransaction<>( neo, latch );
        tx.execute( threading, writeSubject, matchQuery );

        latch.startAndWaitForAllToStart();

        assertSuccess( adminSubject, listQueriesQuery, r ->
        {
            List<Map<String,Object>> maps = collectResults( r );
            assertThat( maps, everyItem( hasProtocol( neo.getConnectionProtocol() ) ) );
        } );

        latch.finishAndWaitForAllToFinish();
        tx.closeAndAssertSuccess();
    }

    @Test
    void shouldListAllQueriesWhenRunningAsAdmin() throws Throwable
    {
        DoubleLatch latch = new DoubleLatch( 3, true );
        OffsetDateTime startTime = getStartTime();

        ThreadedTransaction<S> read1 = new ThreadedTransaction<>( neo, latch );
        ThreadedTransaction<S> read2 = new ThreadedTransaction<>( neo, latch );

        String q1 = read1.execute( threading, readSubject, "UNWIND [1,2,3] AS x RETURN x" );
        String q2 = read2.execute( threading, writeSubject, "UNWIND [4,5,6] AS y RETURN y" );
        latch.startAndWaitForAllToStart();

        String query = "CALL dbms.listQueries()";
        assertSuccess( adminSubject, query, r ->
        {
            List<Map<String,Object>> maps = collectResults( r );

            Matcher<Map<String,Object>> thisQuery = listedQueryOfInteractionLevel( startTime, "adminSubject", query );
            Matcher<Map<String,Object>> matcher1 = listedQuery( startTime, "readSubject", q1 );
            Matcher<Map<String,Object>> matcher2 = listedQuery( startTime, "writeSubject", q2 );

            assertThat( maps, matchesOneToOneInAnyOrder( matcher1, matcher2, thisQuery ) );
        } );

        latch.finishAndWaitForAllToFinish();

        read1.closeAndAssertSuccess();
        read2.closeAndAssertSuccess();
    }

    @Test
    void shouldOnlyListQueriesForAllowedUsers() throws Throwable
    {
        authDisabledAdminstrationCommand( "CREATE USER alice SET PASSWORD 'foo' CHANGE NOT REQUIRED" );
        authDisabledAdminstrationCommand( "CREATE ROLE custom" );
        authDisabledAdminstrationCommand( "GRANT ROLE custom TO alice" );
        authDisabledAdminstrationCommand( "GRANT ACCESS ON DEFAULT DATABASE TO custom" );
        authDisabledAdminstrationCommand( "GRANT SHOW TRANSACTION (readSubject,editorSubject) ON DATABASE * TO custom" );
        S subject = neo.login( "alice", "foo" );

        DoubleLatch latch = new DoubleLatch( 4, true );
        OffsetDateTime startTime = getStartTime();

        ThreadedTransaction<S> read1 = new ThreadedTransaction<>( neo, latch );
        ThreadedTransaction<S> read2 = new ThreadedTransaction<>( neo, latch );
        ThreadedTransaction<S> read3 = new ThreadedTransaction<>( neo, latch );

        String q1 = read1.execute( threading, readSubject, "UNWIND [1,2,3] AS x RETURN x" );
        read2.execute( threading, writeSubject, "UNWIND [4,5,6] AS y RETURN y" );
        String q3 = read3.execute( threading, editorSubject, "UNWIND [7,8,9] AS z RETURN z" );
        latch.startAndWaitForAllToStart();

        String query = "CALL dbms.listQueries()";
        assertSuccess( subject, query, r ->
        {
            List<Map<String,Object>> maps = collectResults( r );

            Matcher<Map<String,Object>> thisQuery = listedQueryOfInteractionLevel( startTime, "alice", query );
            Matcher<Map<String,Object>> matcher1 = listedQuery( startTime, "readSubject", q1 );
            Matcher<Map<String,Object>> matcher2 = listedQuery( startTime, "editorSubject", q3 );

            assertThat( maps, matchesOneToOneInAnyOrder( matcher1, matcher2, thisQuery ) );
        } );

        latch.finishAndWaitForAllToFinish();

        read1.closeAndAssertSuccess();
        read2.closeAndAssertSuccess();
        read3.closeAndAssertSuccess();
    }

    @Test
    void shouldNotListQueriesForDeniedUsers() throws Throwable
    {
        authDisabledAdminstrationCommand( "CREATE USER alice SET PASSWORD 'foo' CHANGE NOT REQUIRED" );
        authDisabledAdminstrationCommand( "CREATE ROLE custom AS COPY OF admin" );
        authDisabledAdminstrationCommand( "GRANT ROLE custom TO alice" );
        authDisabledAdminstrationCommand( "DENY SHOW TRANSACTION (writeSubject) ON DEFAULT DATABASE TO custom" );
        S subject = neo.login( "alice", "foo" );

        DoubleLatch latch = new DoubleLatch( 4, true );
        OffsetDateTime startTime = getStartTime();

        ThreadedTransaction<S> read1 = new ThreadedTransaction<>( neo, latch );
        ThreadedTransaction<S> read2 = new ThreadedTransaction<>( neo, latch );
        ThreadedTransaction<S> read3 = new ThreadedTransaction<>( neo, latch );

        String q1 = read1.execute( threading, readSubject, "UNWIND [1,2,3] AS x RETURN x" );
        read2.execute( threading, writeSubject, "UNWIND [4,5,6] AS y RETURN y" );
        String q3 = read3.execute( threading, editorSubject, "UNWIND [7,8,9] AS z RETURN z" );
        latch.startAndWaitForAllToStart();

        String query = "CALL dbms.listQueries()";
        assertSuccess( subject, query, r ->
        {
            List<Map<String,Object>> maps = collectResults( r );

            Matcher<Map<String,Object>> thisQuery = listedQueryOfInteractionLevel( startTime, "alice", query );
            Matcher<Map<String,Object>> matcher1 = listedQuery( startTime, "readSubject", q1 );
            Matcher<Map<String,Object>> matcher2 = listedQuery( startTime, "editorSubject", q3 );

            assertThat( maps, matchesOneToOneInAnyOrder( matcher1, matcher2, thisQuery ) );
        } );

        latch.finishAndWaitForAllToFinish();

        read1.closeAndAssertSuccess();
        read2.closeAndAssertSuccess();
        read3.closeAndAssertSuccess();
    }

    @Test
    void shouldListAllowedQueriesDifferentDatabases() throws Throwable
    {
        authDisabledAdminstrationCommand( "CREATE USER alice SET PASSWORD 'foo' CHANGE NOT REQUIRED" );
        authDisabledAdminstrationCommand( "CREATE ROLE custom" );
        authDisabledAdminstrationCommand( "GRANT ROLE custom TO alice" );
        authDisabledAdminstrationCommand( "GRANT ACCESS ON DATABASE * TO custom" );
        authDisabledAdminstrationCommand( "CREATE DATABASE foo" );
        authDisabledAdminstrationCommand( "GRANT SHOW TRANSACTION (*) ON DATABASE foo TO custom" );
        S subject = neo.login( "alice", "foo" );

        DoubleLatch latch = new DoubleLatch( 3, true );
        OffsetDateTime startTime = getStartTime();

        ThreadedTransaction<S> tx1 = new ThreadedTransaction<>( neo, latch );
        ThreadedTransaction<S> tx2 = new ThreadedTransaction<>( neo, latch );

        tx1.execute( threading, DEFAULT_DATABASE_NAME, readSubject, "UNWIND [1,2,3] AS x RETURN x" );
        String q2 = tx2.execute( threading, "foo", readSubject, "UNWIND [4,5,6] AS y RETURN y" );
        latch.startAndWaitForAllToStart();

        String query = "CALL dbms.listQueries()";
        assertSuccess( subject, DEFAULT_DATABASE_NAME, query, r ->
        {
            List<Map<String,Object>> maps = collectResults( r );

            Matcher<Map<String,Object>> thisQuery = listedQueryOfInteractionLevel( startTime, "alice", query );

            assertThat( maps, matchesOneToOneInAnyOrder( thisQuery ) );
        } );

        assertSuccess( subject, "foo", query, r ->
        {
            List<Map<String,Object>> maps = collectResults( r );

            Matcher<Map<String,Object>> thisQuery = listedQueryOfInteractionLevel( startTime, "alice", query );
            Matcher<Map<String,Object>> matcher1 = listedQuery( startTime, "readSubject", q2 );

            assertThat( maps, matchesOneToOneInAnyOrder( matcher1, thisQuery ) );
        } );

        latch.finishAndWaitForAllToFinish();

        tx1.closeAndAssertSuccess();
        tx2.closeAndAssertSuccess();
    }

    @Test
    void shouldOnlyListOwnQueriesWhenNotRunningAsAdmin() throws Throwable
    {
        DoubleLatch latch = new DoubleLatch( 3, true );
        OffsetDateTime startTime = getStartTime();
        ThreadedTransaction<S> read1 = new ThreadedTransaction<>( neo, latch );
        ThreadedTransaction<S> read2 = new ThreadedTransaction<>( neo, latch );

        String q1 = read1.execute( threading, readSubject, "UNWIND [1,2,3] AS x RETURN x" );
        read2.execute( threading, writeSubject, "UNWIND [4,5,6] AS y RETURN y" );
        latch.startAndWaitForAllToStart();

        String query = "CALL dbms.listQueries()";
        assertSuccess( readSubject, query, r ->
        {
            List<Map<String,Object>> maps = collectResults( r );

            Matcher<Map<String,Object>> thisQuery = listedQuery( startTime, "readSubject", query );
            Matcher<Map<String,Object>> queryMatcher = listedQuery( startTime, "readSubject", q1 );

            assertThat( maps, matchesOneToOneInAnyOrder( queryMatcher, thisQuery ) );
        } );

        latch.finishAndWaitForAllToFinish();

        read1.closeAndAssertSuccess();
        read2.closeAndAssertSuccess();
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    void shouldListQueriesEvenIfUsingPeriodicCommit() throws Throwable
    {
        for ( int i = 8; i <= 11; i++ )
        {
            // Spawns a throttled HTTP server, runs a PERIODIC COMMIT that fetches data from this server,
            // and checks that the query is visible when using listQueries()

            // Given
            final DoubleLatch latch = new DoubleLatch( 3, true );
            final Barrier.Control barrier = new Barrier.Control();

            // Serve CSV via local web server, let Jetty find a random port for us
            Server server = createHttpServer( latch, barrier, i, 50 - i );
            server.start();
            int localPort = getLocalPort( server );

            OffsetDateTime startTime = getStartTime();

            // When
            ThreadedTransaction<S> write = new ThreadedTransaction<>( neo, latch );

            try
            {
                String writeQuery = write.executeEarly( threading, writeSubject, KernelTransaction.Type.IMPLICIT,
                        format( "USING PERIODIC COMMIT 10 LOAD CSV FROM 'http://localhost:%d' AS line ", localPort ) +
                        "CREATE (n:A {id: line[0], square: line[1]}) " + "RETURN count(*)" );
                latch.startAndWaitForAllToStart();

                // Then
                String query = "CALL dbms.listQueries()";
                assertSuccess( adminSubject, query, r ->
                {
                    List<Map<String,Object>> maps = collectResults( r );

                    Matcher<Map<String,Object>> thisMatcher = listedQuery( startTime, "adminSubject", query );
                    Matcher<Map<String,Object>> writeMatcher = listedQuery( startTime, "writeSubject", writeQuery );

                    assertThat( maps, hasItem( thisMatcher ) );
                    assertThat( maps, hasItem( writeMatcher ) );
                } );
            }
            finally
            {
                // When
                barrier.release();
                latch.finishAndWaitForAllToFinish();
                server.stop();

                // Then
                write.closeAndAssertSuccess();
            }
        }
    }

    @Test
    void shouldListAllQueriesWithAuthDisabled() throws Throwable
    {
        neo.tearDown();
        neo = setUpNeoServer( Map.of( GraphDatabaseSettings.auth_enabled, FALSE ) );

        DoubleLatch latch = new DoubleLatch( 2, true );
        OffsetDateTime startTime = getStartTime();

        ThreadedTransaction<S> read = new ThreadedTransaction<>( neo, latch );

        String q = read.execute( threading, neo.login( "user1", "" ), "UNWIND [1,2,3] AS x RETURN x" );
        latch.startAndWaitForAllToStart();

        String query = "CALL dbms.listQueries()";
        try
        {
            assertSuccess( neo.login( "admin", "" ), query, r ->
            {
                List<Map<String,Object>> maps = collectResults( r );

                Matcher<Map<String,Object>> thisQuery = listedQueryOfInteractionLevel( startTime, "", query ); // admin
                Matcher<Map<String,Object>> matcher1 = listedQuery( startTime, "", q ); // user1
                assertThat( maps, matchesOneToOneInAnyOrder( matcher1, thisQuery ) );
            } );
        }
        finally
        {
            latch.finishAndWaitForAllToFinish();
        }
        read.closeAndAssertSuccess();
    }

    @Test
    void allDatabasePrivilegeShouldNotImplyListQuery() throws Throwable
    {
        authDisabledAdminstrationCommand( "CREATE USER alice SET PASSWORD 'foo' CHANGE NOT REQUIRED" );
        authDisabledAdminstrationCommand( "CREATE ROLE custom" );
        authDisabledAdminstrationCommand( "GRANT ROLE custom TO alice" );
        authDisabledAdminstrationCommand( "GRANT ALL ON DEFAULT DATABASE TO custom" );
        S subject = neo.login( "alice", "foo" );

        DoubleLatch latch = new DoubleLatch( 3, true );
        OffsetDateTime startTime = getStartTime();
        ThreadedTransaction<S> read1 = new ThreadedTransaction<>( neo, latch );
        ThreadedTransaction<S> read2 = new ThreadedTransaction<>( neo, latch );

        String q1 = read1.execute( threading, subject, "UNWIND [1,2,3] AS x RETURN x" );
        read2.execute( threading, writeSubject, "UNWIND [4,5,6] AS y RETURN y" );
        latch.startAndWaitForAllToStart();

        String query = "CALL dbms.listQueries()";
        assertSuccess( subject, query, r ->
        {
            List<Map<String,Object>> maps = collectResults( r );

            Matcher<Map<String,Object>> thisQuery = listedQuery( startTime, "alice", query );
            Matcher<Map<String,Object>> queryMatcher = listedQuery( startTime, "alice", q1 );

            assertThat( maps, matchesOneToOneInAnyOrder( queryMatcher, thisQuery ) );
        } );

        latch.finishAndWaitForAllToFinish();

        read1.closeAndAssertSuccess();
        read2.closeAndAssertSuccess();
    }

    //---------- Create Tokens query -------

    @Test
    void shouldCreateLabel()
    {
        assertFail( editorSubject, "CREATE (:MySpecialLabel)", CREATE_LABEL_OPS_NOT_ALLOWED );
        assertFail( editorSubject, "CALL db.createLabel('MySpecialLabel')", CREATE_LABEL_OPS_NOT_ALLOWED );
        assertEmpty( writeSubject, "CALL db.createLabel('MySpecialLabel')" );
        assertSuccess( writeSubject, "MATCH (n:MySpecialLabel) RETURN count(n) AS count", r ->
                assertEquals(  r.next().get( "count" ),  valueOf( 0L ) ) );
        assertEmpty( editorSubject, "CREATE (:MySpecialLabel)" );
    }

    @Test
    void shouldCreateRelationshipType()
    {
        assertEmpty( writeSubject, "CREATE (a:Node {id:0}) CREATE ( b:Node {id:1} )" );
        assertFail( editorSubject, "MATCH (a:Node), (b:Node) WHERE a.id = 0 AND b.id = 1 CREATE (a)-[:MySpecialRelationship]->(b)",
                CREATE_RELTYPE_OPS_NOT_ALLOWED );
        assertFail( editorSubject, "CALL db.createRelationshipType('MySpecialRelationship')", CREATE_RELTYPE_OPS_NOT_ALLOWED );
        assertEmpty( writeSubject, "CALL db.createRelationshipType('MySpecialRelationship')" );
        assertSuccess( editorSubject, "MATCH (n)-[c:MySpecialRelationship]-(m) RETURN count(c) AS count",
                r -> assertEquals(  r.next().get( "count" ),  valueOf( 0L ) ) );
        assertEmpty( editorSubject, "MATCH (a:Node), (b:Node) WHERE a.id = 0 AND b.id = 1 CREATE (a)-[:MySpecialRelationship]->(b)" );
    }

    @Test
    void shouldCreateProperty()
    {
        assertFail( editorSubject, "CREATE (a) SET a.MySpecialProperty = 'a'", CREATE_PROPERTYKEY_OPS_NOT_ALLOWED );
        assertFail( editorSubject, "CALL db.createProperty('MySpecialProperty')", CREATE_PROPERTYKEY_OPS_NOT_ALLOWED );
        assertEmpty( writeSubject, "CALL db.createProperty('MySpecialProperty')" );
        assertSuccess( editorSubject, "MATCH (n) WHERE n.MySpecialProperty IS NULL RETURN count(n) AS count",
                r -> assertEquals(  r.next().get( "count" ),   valueOf( 3L ) ) );
        assertEmpty( editorSubject, "CREATE (a) SET a.MySpecialProperty = 'a'" );
    }

    //---------- terminate query -----------

    /*
     * User starts query1 that takes a lock and runs for a long time.
     * User starts query2 that needs to wait for that lock.
     * query2 is blocked waiting for lock to be released.
     * Admin terminates query2.
     * query2 is immediately terminated, even though locks have not been released.
     */
    @Test
    void queryWaitingForLocksShouldBeKilledBeforeLocksAreReleased() throws Throwable
    {
        try ( Support support = Support.getSupport() )
        {
            assertEmpty( adminSubject, "CREATE (:MyNode {prop: 2})" );

            // create new latch
            support.doubleLatch = new DoubleLatch( 2 );

            // start never-ending query
            String query1 = "MATCH (n:MyNode) SET n.prop = 5 WITH * CALL test.neverEnding(" + support.getId() + ") RETURN 1";
            ThreadedTransaction<S> tx1 = new ThreadedTransaction<>( neo, new DoubleLatch() );
            tx1.executeEarly( threading, writeSubject, KernelTransaction.Type.EXPLICIT, query1 );

            // wait for query1 to be stuck in procedure with its write lock
            support.doubleLatch.startAndWaitForAllToStart();

            // start query2
            ThreadedTransaction<S> tx2 = new ThreadedTransaction<>( neo, new DoubleLatch() );
            String query2 = "MATCH (n:MyNode) SET n.prop = 10 RETURN 1";
            tx2.executeEarly( threading, writeSubject, KernelTransaction.Type.EXPLICIT, query2 );

            assertQueryIsRunning( query2 );

            // get the query id of query2 and kill it
            assertSuccess( adminSubject,
                    "CALL dbms.listQueries() YIELD query, queryId " +
                            "WITH query, queryId WHERE query = '" + query2 + "'" +
                            "CALL dbms.killQuery(queryId) YIELD queryId AS killedId " +
                            "RETURN 1",
                    itr -> assertThat( Iterators.count( itr ), equalTo( 1L ) ) ); // consume iterator so resources are closed

            tx2.closeAndAssertSomeTermination();

            // allow query1 to exit procedure and finish
            support.doubleLatch.finish();
            tx1.closeAndAssertSuccess();
        }
    }

    @Test
    void shouldKillQueryAsAdmin() throws Throwable
    {
        executeTwoQueriesAndKillTheFirst( readSubject, readSubject, adminSubject );
    }

    @Test
    void shouldKillQueryAsUser() throws Throwable
    {
        executeTwoQueriesAndKillTheFirst( readSubject, writeSubject, readSubject );
    }

    @Test
    void shouldOnlyKillQueryForUserWithPrivilege() throws Throwable
    {
        authDisabledAdminstrationCommand( "CREATE USER alice SET PASSWORD 'foo' CHANGE NOT REQUIRED" );
        authDisabledAdminstrationCommand( "CREATE ROLE custom" );
        authDisabledAdminstrationCommand( "GRANT ROLE custom TO alice" );
        authDisabledAdminstrationCommand( "GRANT ACCESS ON DEFAULT DATABASE TO custom" );
        authDisabledAdminstrationCommand( "GRANT TERMINATE TRANSACTION (readSubject) ON DATABASE * TO custom" );
        S subject = neo.login( "alice", "foo" );
        executeTwoQueriesAndKillTheFirst( readSubject, writeSubject, subject );
        executeQueryAndFailToKill( writeSubject, subject );
    }

    @Test
    void shouldNotKillQueryForUserDeniedPrivilege() throws Throwable
    {
        authDisabledAdminstrationCommand( "CREATE USER alice SET PASSWORD 'foo' CHANGE NOT REQUIRED" );
        authDisabledAdminstrationCommand( "CREATE ROLE custom AS COPY OF admin" );
        authDisabledAdminstrationCommand( "GRANT ROLE custom TO alice" );
        authDisabledAdminstrationCommand( "DENY TERMINATE TRANSACTION (writeSubject) ON DEFAULT DATABASE TO custom" );
        S subject = neo.login( "alice", "foo" );
        executeTwoQueriesAndKillTheFirst( readSubject, writeSubject, subject );
        executeQueryAndFailToKill( writeSubject, subject );
    }

    @Test
    void shouldOnlyKillQueryForDatabaseWithPrivilege() throws Throwable
    {
        authDisabledAdminstrationCommand( "CREATE USER alice SET PASSWORD 'foo' CHANGE NOT REQUIRED" );
        authDisabledAdminstrationCommand( "CREATE ROLE custom" );
        authDisabledAdminstrationCommand( "GRANT ROLE custom TO alice" );
        authDisabledAdminstrationCommand( "GRANT ACCESS ON DATABASE * TO custom" );
        authDisabledAdminstrationCommand( "CREATE DATABASE foo" );
        authDisabledAdminstrationCommand( "GRANT TERMINATE TRANSACTION ON DATABASE foo TO custom" );
        S subject = neo.login( "alice", "foo" );
        DoubleLatch latch = new DoubleLatch( 3 );
        ThreadedTransaction<S> tx1 = new ThreadedTransaction<>( neo, latch );
        ThreadedTransaction<S> tx2 = new ThreadedTransaction<>( neo, latch );
        String q1 = tx1.execute( threading, DEFAULT_DATABASE_NAME, readSubject, "UNWIND [1,2,3] AS x RETURN x" );
        String q2 = tx2.execute( threading, "foo", readSubject, "UNWIND [4,5,6] AS y RETURN y" );
        latch.startAndWaitForAllToStart();

        String id1 = extractQueryId( q1 );
        String id2 = extractQueryId( q2 );

        assertFail(
                DEFAULT_DATABASE_NAME,
                subject,
                "CALL dbms.killQuery('" + id1 + "') YIELD username RETURN *",
                PERMISSION_DENIED
        );

        assertSuccess(
                subject,
                "foo",
                "CALL dbms.killQuery('" + id2 + "') YIELD username " +
                "RETURN count(username) AS count, username", r ->
                {
                    List<Map<String,Object>> actual = collectResults( r );
                    Matcher<Map<String,Object>> mapMatcher = allOf(
                            hasEntry( equalTo( "count" ), anyOf( ONE_AS_INT, ONE_AS_LONG ) ),
                            hasEntry( equalTo( "username" ), equalTo( "readSubject" ) )
                    );
                    assertThat( actual, matchesOneToOneInAnyOrder( mapMatcher ) );
                }
        );

        latch.finishAndWaitForAllToFinish();
        tx1.closeAndAssertSuccess();
        tx2.closeAndAssertExplicitTermination();

        assertEmpty( adminSubject, "CALL dbms.listQueries() YIELD query WITH * WHERE NOT query CONTAINS 'listQueries' RETURN *" );
    }

    private void executeTwoQueriesAndKillTheFirst( S executor1, S executor2, S killer ) throws Throwable
    {
        DoubleLatch latch = new DoubleLatch( 3 );
        ThreadedTransaction<S> tx1 = new ThreadedTransaction<>( neo, latch );
        ThreadedTransaction<S> tx2 = new ThreadedTransaction<>( neo, latch );
        String q1 = tx1.execute( threading, executor1, "UNWIND [1,2,3] AS x RETURN x" );
        tx2.execute( threading, executor2, "UNWIND [4,5,6] AS y RETURN y" );
        latch.startAndWaitForAllToStart();

        String id1 = extractQueryId( q1 );

        assertSuccess(
                killer,
                "CALL dbms.killQuery('" + id1 + "') YIELD username " +
                "RETURN count(username) AS count, username", r ->
                {
                    List<Map<String,Object>> actual = collectResults( r );
                    Matcher<Map<String,Object>> mapMatcher = allOf(
                            hasEntry( equalTo( "count" ), anyOf( ONE_AS_INT, ONE_AS_LONG ) ),
                            hasEntry( equalTo( "username" ), equalTo( "readSubject" ) )
                    );
                    assertThat( actual, matchesOneToOneInAnyOrder( mapMatcher ) );
                }
        );

        latch.finishAndWaitForAllToFinish();
        tx1.closeAndAssertExplicitTermination();
        tx2.closeAndAssertSuccess();

        assertEmpty( adminSubject,
                "CALL dbms.listQueries() YIELD query WITH * WHERE NOT query CONTAINS 'listQueries' RETURN *" );
    }

    @Test
    void shouldSelfKillQuery()
    {
        String result = neo.executeQuery(
                readSubject,
                DEFAULT_DATABASE_NAME,
                "WITH 'Hello' AS marker CALL dbms.listQueries() YIELD queryId AS id, query " +
                "WITH * WHERE query CONTAINS 'Hello' CALL dbms.killQuery(id) YIELD username " +
                "RETURN count(username) AS count, username",
                emptyMap(),
                Iterators::count // consume result to flush any errors
        );

        assertThat( result, containsString( "Explicitly terminated by the user." ) );

        assertEmpty(
                adminSubject,
                "CALL dbms.listQueries() YIELD query WITH * WHERE NOT query CONTAINS 'listQueries' RETURN *" );
    }

    @Test
    void shouldFailToTerminateOtherUsersQuery() throws Throwable
    {
        executeQueryAndFailToKill( readSubject, writeSubject );
    }

    @Test
    void allDatabasePrivilegeShouldNotImplyKillQuery() throws Throwable
    {
        authDisabledAdminstrationCommand( "CREATE USER alice SET PASSWORD 'foo' CHANGE NOT REQUIRED" );
        authDisabledAdminstrationCommand( "CREATE ROLE custom" );
        authDisabledAdminstrationCommand( "GRANT ROLE custom TO alice" );
        authDisabledAdminstrationCommand( "GRANT ALL ON DEFAULT DATABASE TO custom" );
        S subject = neo.login( "alice", "foo" );

        executeQueryAndFailToKill( readSubject, subject );
    }

    private void executeQueryAndFailToKill( S executor, S killer ) throws Throwable
    {
        DoubleLatch latch = new DoubleLatch( 2, true );
        ThreadedTransaction<S> tx = new ThreadedTransaction<>( neo, latch );
        String q1 = tx.execute( threading, executor, "UNWIND [1,2,3] AS x RETURN x" );
        latch.startAndWaitForAllToStart();

        String id1 = extractQueryId( q1 );

        try
        {
            assertFail(
                    killer,
                    "CALL dbms.killQuery('" + id1 + "') YIELD username RETURN *",
                    PERMISSION_DENIED
            );
            latch.finishAndWaitForAllToFinish();
            tx.closeAndAssertSuccess();
        }
        catch ( Throwable t )
        {
            latch.finishAndWaitForAllToFinish();
            throw t;
        }

        assertEmpty(
                adminSubject,
                "CALL dbms.listQueries() YIELD query WITH * WHERE NOT query CONTAINS 'listQueries' RETURN *" );
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    void shouldTerminateQueriesEvenIfUsingPeriodicCommit() throws Throwable
    {
        for ( int batchSize = 8; batchSize <= 11; batchSize++ )
        {
            // Spawns a throttled HTTP server, runs a PERIODIC COMMIT that fetches data from this server,
            // and checks that the query is visible when using listQueries()

            // Given
            final DoubleLatch latch = new DoubleLatch( 3, true );
            final Barrier.Control barrier = new Barrier.Control();

            // Serve CSV via local web server, let Jetty find a random port for us
            Server server = createHttpServer( latch, barrier, batchSize, 50 - batchSize );
            server.start();
            int localPort = getLocalPort( server );

            // When
            ThreadedTransaction<S> write = new ThreadedTransaction<>( neo, latch );

            try
            {
                String writeQuery = write.executeEarly( threading, writeSubject, KernelTransaction.Type.IMPLICIT,
                        format( "USING PERIODIC COMMIT 10 LOAD CSV FROM 'http://localhost:%d' AS line ", localPort ) +
                        "CREATE (n:A {id: line[0], square: line[1]}) RETURN count(*)" );
                latch.startAndWaitForAllToStart();

                // Then
                String writeQueryId = extractQueryId( writeQuery );

                assertSuccess(
                        adminSubject,
                        "CALL dbms.killQuery('" + writeQueryId + "') YIELD username " +
                        "RETURN count(username) AS count, username", r ->
                        {
                            List<Map<String,Object>> actual = collectResults( r );
                            Matcher<Map<String,Object>> mapMatcher = allOf(
                                    hasEntry( equalTo( "count" ), anyOf( ONE_AS_INT, ONE_AS_LONG ) ),
                                    hasEntry( equalTo( "username" ), equalTo( "writeSubject" ) )
                            );
                            assertThat( actual, matchesOneToOneInAnyOrder( mapMatcher ) );
                        }
                );
            }
            finally
            {
                // When
                barrier.release();
                latch.finishAndWaitForAllToFinish();

                // Then
                // We cannot assert on explicit termination here, because if the termination is detected when trying
                // to lock we will only get the general TransactionTerminatedException
                // (see {@link LockClientStateHolder}).
                write.closeAndAssertSomeTermination();

                // stop server after assertion to avoid other kind of failures due to races (e.g., already closed
                // lock clients )
                server.stop();
            }
        }
    }

    @Test
    void shouldKillMultipleUserQueries() throws Throwable
    {
        DoubleLatch latch = new DoubleLatch( 5 );
        ThreadedTransaction<S> read1 = new ThreadedTransaction<>( neo, latch );
        ThreadedTransaction<S> read2 = new ThreadedTransaction<>( neo, latch );
        ThreadedTransaction<S> read3 = new ThreadedTransaction<>( neo, latch );
        ThreadedTransaction<S> write = new ThreadedTransaction<>( neo, latch );
        String q1 = read1.execute( threading, readSubject, "UNWIND [1,2,3] AS x RETURN x" );
        String q2 = read2.execute( threading, readSubject, "UNWIND [4,5,6] AS y RETURN y" );
        read3.execute( threading, readSubject, "UNWIND [7,8,9] AS z RETURN z" );
        write.execute( threading, writeSubject, "UNWIND [11,12,13] AS q RETURN q" );
        latch.startAndWaitForAllToStart();

        String id1 = extractQueryId( q1 );
        String id2 = extractQueryId( q2 );

        String idParam = "['" + id1 + "', '" + id2 + "']";

        assertSuccess(
                adminSubject,
                "CALL dbms.killQueries(" + idParam + ") YIELD username " +
                "RETURN count(username) AS count, username", r ->
                {
                    List<Map<String,Object>> actual = collectResults( r );
                    Matcher<Map<String,Object>> mapMatcher = allOf(
                            hasEntry( equalTo( "count" ), anyOf( TWO_AS_INT, TWO_AS_LONG ) ),
                            hasEntry( equalTo( "username" ), equalTo( "readSubject" ) )
                    );
                    assertThat( actual, matchesOneToOneInAnyOrder( mapMatcher ) );
                }
        );

        latch.finishAndWaitForAllToFinish();
        read1.closeAndAssertExplicitTermination();
        read2.closeAndAssertExplicitTermination();
        read3.closeAndAssertSuccess();
        write.closeAndAssertSuccess();

        assertEmpty(
                adminSubject,
                "CALL dbms.listQueries() YIELD query WITH * WHERE NOT query CONTAINS 'listQueries' RETURN *" );
    }

    @Test
    void shouldListQueriesAndTransactionWithTransactionPrivilege() throws Throwable
    {
        authDisabledAdminstrationCommand( "CREATE USER alice SET PASSWORD 'foo' CHANGE NOT REQUIRED" );
        authDisabledAdminstrationCommand( "CREATE ROLE custom" );
        authDisabledAdminstrationCommand( "GRANT ROLE custom TO alice" );
        authDisabledAdminstrationCommand( "GRANT ACCESS ON DEFAULT DATABASE TO custom" );
        authDisabledAdminstrationCommand( "GRANT TRANSACTION MANAGEMENT (readSubject) ON DATABASE * TO custom" );
        S subject = neo.login( "alice", "foo" );

        OffsetDateTime startTime = getStartTime();
        DoubleLatch latch = new DoubleLatch( 3 );
        ThreadedTransaction<S> tx1 = new ThreadedTransaction<>( neo, latch );
        ThreadedTransaction<S> tx2 = new ThreadedTransaction<>( neo, latch );
        String q1 = tx1.execute( threading, readSubject, "UNWIND [1,2,3] AS x RETURN x" );
        tx2.execute( threading, writeSubject, "UNWIND [4,5,6] AS y RETURN y" );
        latch.startAndWaitForAllToStart();

        String listQueries = "CALL dbms.listQueries()";
        assertSuccess( subject, listQueries, r ->
        {
            List<Map<String,Object>> maps = collectResults( r );

            Matcher<Map<String,Object>> thisQuery = listedQueryOfInteractionLevel( startTime, "alice", listQueries );
            Matcher<Map<String,Object>> matcher = listedQuery( startTime, "readSubject", q1 );

            assertThat( maps, matchesOneToOneInAnyOrder( matcher, thisQuery ) );
        } );

        String listTransactions = "CALL dbms.listTransactions()";
        assertSuccess( subject, listTransactions, r ->
        {
            List<Map<String,Object>> maps = collectResults( r );

            Matcher<Map<String,Object>> thisTransaction = listedTransactionOfInteractionLevel( startTime, "alice", listTransactions );
            Matcher<Map<String,Object>> matcher = listedTransaction( startTime, "readSubject", q1 );

            assertThat( maps, matchesOneToOneInAnyOrder( matcher, thisTransaction ) );
        } );

        latch.finishAndWaitForAllToFinish();
        tx1.closeAndAssertSuccess();
        tx2.closeAndAssertSuccess();
    }

    @Test
    void shouldListAndKillQueriesAndTransactionsWithManagementPrivilege() throws Throwable
    {
        authDisabledAdminstrationCommand( "CREATE USER alice SET PASSWORD 'foo' CHANGE NOT REQUIRED" );
        authDisabledAdminstrationCommand( "CREATE ROLE custom" );
        authDisabledAdminstrationCommand( "GRANT ROLE custom TO alice" );
        authDisabledAdminstrationCommand( "GRANT ACCESS ON DEFAULT DATABASE TO custom" );
        authDisabledAdminstrationCommand( "GRANT TRANSACTION MANAGEMENT ON DATABASE * TO custom" );
        S subject = neo.login( "alice", "foo" );
        DoubleLatch transactionLatch = new DoubleLatch( 2 );
        DoubleLatch queryLatch = new DoubleLatch( 2 );

        OffsetDateTime startTime = getStartTime();
        ThreadedTransaction<S> tx1 = new ThreadedTransaction<>( neo, queryLatch );
        ThreadedTransaction<S> tx2 = new ThreadedTransaction<>( neo, transactionLatch );
        String q1 = tx1.execute( threading, readSubject, "UNWIND [1,2,3] AS x RETURN x" );
        String q2 = tx2.execute( threading, writeSubject, "UNWIND [4,5,6] AS y RETURN y" );
        queryLatch.startAndWaitForAllToStart();
        transactionLatch.startAndWaitForAllToStart();

        String queryId = extractQueryId( q1 );

        String listQueries = "CALL dbms.listQueries()";
        assertSuccess( subject, listQueries, r ->
        {
            List<Map<String,Object>> maps = collectResults( r );

            Matcher<Map<String,Object>> thisQuery = listedQueryOfInteractionLevel( startTime, "alice", listQueries );
            Matcher<Map<String,Object>> matcher1 = listedQuery( startTime, "readSubject", q1 );
            Matcher<Map<String,Object>> matcher2 = listedQuery( startTime, "writeSubject", q2 );

            assertThat( maps, matchesOneToOneInAnyOrder( matcher1, matcher2, thisQuery ) );
        } );

        assertSuccess(
                subject,
                "CALL dbms.killQuery('" + queryId + "') YIELD username " +
                "RETURN count(username) AS count, username", r ->
                {
                    List<Map<String,Object>> actual = collectResults( r );
                    Matcher<Map<String,Object>> mapMatcher = allOf(
                            hasEntry( equalTo( "count" ), anyOf( ONE_AS_INT, ONE_AS_LONG ) ),
                            hasEntry( equalTo( "username" ), equalTo( "readSubject" ) )
                    );
                    assertThat( actual, matchesOneToOneInAnyOrder( mapMatcher ) );
                }
        );

        queryLatch.finishAndWaitForAllToFinish();
        tx1.closeAndAssertExplicitTermination();

        String listTransactions = "CALL dbms.listTransactions()";
        assertSuccess( subject, listTransactions, r ->
        {
            List<Map<String,Object>> maps = collectResults( r );

            Matcher<Map<String,Object>> thisTransaction = listedTransactionOfInteractionLevel( startTime, "alice", listTransactions );
            Matcher<Map<String,Object>> matcher = listedTransaction( startTime, "writeSubject", q2 );

            assertThat( maps, matchesOneToOneInAnyOrder( matcher, thisTransaction ) );
        } );

        String txId = getTransactionIdExecutingQuery( q2, listTransactions, adminSubject );
        assertSuccess( subject, format( "CALL dbms.killTransaction('%s')", txId ), r ->
        {
            List<Map<String,Object>> killQueryResult = collectResults( r );
            assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasUsername( "writeSubject" ) ) );
            assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasResultEntry( "message", "Transaction terminated." ) ) );
            assertThat( killQueryResult, matchesOneToOneInAnyOrder( hasResultEntry( "transactionId", txId ) ) );
        } );

        transactionLatch.finishAndWaitForAllToFinish();
        tx2.closeAndAssertExplicitTermination();

        assertEmpty(
                adminSubject,
                "CALL dbms.listQueries() YIELD query WITH * WHERE NOT query CONTAINS 'listQueries' RETURN *" );
    }

    String extractQueryId( String writeQuery )
    {
        return toRawValue( single( collectSuccessResult( adminSubject, "CALL dbms.listQueries()" )
                .stream()
                .filter( m -> m.get( "query" ).equals( valueOf( writeQuery ) ) )
                .collect( toList() ) )
                .get( "queryId" ) )
                .toString();
    }

    //---------- set tx meta data -----------

    @Test
    void shouldHaveSetTXMetaDataProcedure()
    {
        assertEmpty( writeSubject, "CALL tx.setMetaData( { aKey: 'aValue' } )" );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    void readUpdatedMetadataValue() throws Throwable
    {
        String testValue = "testValue";
        String testKey = "test";
        try ( InternalTransaction transaction = neo
                .beginLocalTransactionAsUser( writeSubject, KernelTransaction.Type.EXPLICIT ) )
        {
            transaction.execute( "CALL tx.setMetaData({" + testKey + ":'" + testValue + "'})" );
            Map<String,Object> metadata =
                    (Map<String,Object>) transaction.execute( "CALL tx.getMetaData " ).next().get( "metadata" );
            assertEquals( testValue, metadata.get( testKey ) );
        }
    }

    @Test
    void readEmptyMetadataInOtherTransaction()
    {
        String testValue = "testValue";
        String testKey = "test";

        assertEmpty( writeSubject, "CALL tx.setMetaData({" + testKey + ":'" + testValue + "'})" );
        assertSuccess( writeSubject, "CALL tx.getMetaData", mapResourceIterator ->
        {
            Map<String,Object> metadata = mapResourceIterator.next();
            assertNull( metadata.get( testKey ) );
            mapResourceIterator.close();
        } );
    }

    //---------- config manipulation -----------

    @Test
    void setConfigValueShouldBeAccessibleOnlyToAdmins()
    {
        String call = "CALL dbms.setConfigValue('dbms.logs.query.enabled', 'off')";
        assertFail( writeSubject, call, PERMISSION_DENIED );
        assertFail( schemaSubject, call, PERMISSION_DENIED );
        assertFail( readSubject, call, PERMISSION_DENIED );

        assertEmpty( adminSubject, call );
    }

    //---------- procedure guard -----------

    @Test
    void shouldTerminateLongRunningProcedureThatChecksTheGuardRegularlyIfKilled() throws Throwable
    {
        final DoubleLatch latch = new DoubleLatch( 2, true );
        try ( Support support = Support.getSupport() )
        {
            support.volatileLatch = latch;

            String loopQuery = "CALL test.loop(" + support.getId() + ")";

            Thread loopQueryThread =
                    new Thread( () -> assertFail( readSubject, loopQuery, "Explicitly terminated by the user." ) );
            loopQueryThread.start();
            latch.startAndWaitForAllToStart();

            try
            {
                String loopId = extractQueryId( loopQuery );

                assertSuccess(
                        adminSubject,
                        "CALL dbms.killQuery('" + loopId + "') YIELD username " +
                                "RETURN count(username) AS count, username", r ->
                        {
                            List<Map<String,Object>> actual = collectResults( r );
                            Matcher<Map<String,Object>> mapMatcher = allOf(
                                    hasEntry( equalTo( "count" ), anyOf( ONE_AS_INT, ONE_AS_LONG ) ),
                                    hasEntry( equalTo( "username" ), equalTo( "readSubject" ) )
                            );
                            assertThat( actual, matchesOneToOneInAnyOrder( mapMatcher ) );
                        }
                );
            }
            finally
            {
                latch.finishAndWaitForAllToFinish();
            }

            // there is a race with "test.loop" procedure - after decrementing latch it may take time to actually exit
            loopQueryThread.join( 10_000 );

            assertEmpty(
                    adminSubject,
                    "CALL dbms.listQueries() YIELD query WITH * WHERE NOT query CONTAINS 'listQueries' RETURN *" );
        }
    }

    @Test
    void shouldHandleWriteAfterAllowedReadProcedureForWriteUser() throws Throwable
    {
        setupTestSubject();
        assertDDLCommandSuccess( adminSubject, String.format( "GRANT ROLE %s TO %s", PUBLISHER, SUBJECT ) );
        assertEmpty( neo.login( SUBJECT, PASSWORD ),
                "CALL test.allowedReadProcedure() YIELD value CREATE (:NEWNODE {name:value})" );
    }

    @Test
    void shouldNotAllowNonWriterToWriteAfterCallingAllowedWriteProc() throws Exception
    {
        assertDDLCommandSuccess( adminSubject, "CREATE USER notAllowedToWrite SET PASSWORD 'abc' CHANGE NOT REQUIRED" );
        createRoleWithAccess( ROLE, "notAllowedToWrite" );
        assertDDLCommandSuccess( adminSubject, String.format( "GRANT ROLE %s to notAllowedToWrite", READER ) );
        // should be able to invoke allowed procedure
        assertSuccess( neo.login( "notAllowedToWrite", "abc" ), "CALL test.allowedWriteProcedure()",
                itr -> assertEquals( 2, (int) itr.stream().count() ) );
        // should not be able to do writes
        assertFail( neo.login( "notAllowedToWrite", "abc" ),
                "CALL test.allowedWriteProcedure() YIELD value CREATE (:NEWNODE {name:value})", "Create node with labels 'NEWNODE' is not allowed" );
    }

    @Test
    void shouldNotAllowUnauthorizedAccessToProcedure() throws Exception
    {
        assertDDLCommandSuccess( adminSubject, "CREATE USER nopermission SET PASSWORD 'abc' CHANGE NOT REQUIRED" );
        createRoleWithAccess( "Access", "nopermission" );
        grantAccess( "Access" );

        // should get result from empty sub graph
        assertSuccess( neo.login( "nopermission", "abc" ), "CALL test.numNodes()", r -> assertKeyIs( r, "count", "0" ) );

        // should not be able to invoke any procedure
        assertFail( neo.login( "nopermission", "abc" ), "CALL test.staticWriteProcedure()", "Create node with labels '' is not allowed" );
        assertFail( neo.login( "nopermission", "abc" ), "CALL test.staticSchemaProcedure()", SCHEMA_OPS_NOT_ALLOWED );
    }

    @Test
    void shouldNotAllowNonReaderToReadAfterCallingAllowedReadProc() throws Exception
    {
        setupTestSubject();
        assertSuccess( neo.login( SUBJECT, PASSWORD ), "CALL test.allowedReadProcedure()",
                itr -> assertEquals( 1, (int) itr.stream().count() ) );
        assertSuccess( neo.login( SUBJECT, PASSWORD ), "CALL test.allowedReadProcedure() YIELD value MATCH (n) RETURN count(n) AS count",
                       itr -> assertThat( itr.next().get( "count" ), equalTo( valueOf( 0L ) ) ) );
    }

    @Test
    void shouldHandleNestedReadProcedures() throws Throwable
    {
        setupTestSubject();
        assertSuccess( neo.login( SUBJECT, PASSWORD ),
                "CALL test.nestedAllowedProcedure('test.allowedReadProcedure') YIELD value",
                r -> assertKeyIs( r, "value", "foo" ) );
    }

    @Test
    void shouldHandleDoubleNestedReadProcedures() throws Throwable
    {
        setupTestSubject();
        assertSuccess( neo.login( SUBJECT, PASSWORD ),
                "CALL test.doubleNestedAllowedProcedure YIELD value",
                r -> assertKeyIs( r, "value", "foo" ) );
    }

    @Test
    void shouldFailNestedAllowedWriteProcedureFromAllowedReadProcedure() throws Throwable
    {
        setupTestSubject();
        assertFail( neo.login( SUBJECT, PASSWORD ),
                "CALL test.nestedAllowedProcedure('test.allowedWriteProcedure') YIELD value",
                "Create node with labels 'VeryUniqueLabel' is not allowed" );
    }

    @Test
    void shouldFailNestedAllowedWriteProcedureFromAllowedReadProcedureEvenIfAdmin() throws Throwable
    {
        setupTestSubject();
        assertDDLCommandSuccess( adminSubject, String.format( "GRANT ROLE %s TO %s", ADMIN, SUBJECT ) );
        assertFail( neo.login( SUBJECT, PASSWORD ),
                "CALL test.nestedAllowedProcedure('test.allowedWriteProcedure') YIELD value",
                "Create node with labels 'VeryUniqueLabel' is not allowed" );
    }

    @Test
    void shouldRestrictNestedReadProcedureFromAllowedWriteProcedures() throws Throwable
    {
        setupTestSubject();
        assertFail( neo.login( SUBJECT, PASSWORD ),
                "CALL test.failingNestedAllowedWriteProcedure YIELD value",
                "Create node with labels 'VeryUniqueLabel' is not allowed" );
    }

    @Test
    void shouldHandleNestedReadProcedureWithDifferentAllowedRole() throws Throwable
    {
        setupTestSubject();
        assertSuccess( neo.login( SUBJECT, PASSWORD ),
                "CALL test.nestedAllowedProcedure('test.otherAllowedReadProcedure') YIELD value",
                r -> assertKeyIs( r, "value", "foo" )
        );
    }

    @Test
    void shouldFailNestedAllowedWriteProcedureFromNormalReadProcedure() throws Throwable
    {
        setupTestSubject();
        assertDDLCommandSuccess( adminSubject, String.format( "GRANT ROLE %s TO %s", PUBLISHER, SUBJECT ) );

        // Even if subject has WRITE permission the procedure should restrict to READ
        assertFail( neo.login( SUBJECT, PASSWORD ),
                "CALL test.nestedReadProcedure('test.allowedWriteProcedure') YIELD value",
                "Create node with labels 'VeryUniqueLabel' is not allowed" );
    }

    @Test
    void shouldHandleFunctionWithAllowed() throws Throwable
    {
        setupTestSubject();
        assertSuccess( neo.login( SUBJECT, PASSWORD ),
                "RETURN test.allowedFunction1() AS value",
                r -> assertKeyIs( r, "value", "foo" ) );
    }

    @Test
    void shouldHandleNestedFunctionsWithAllowed() throws Throwable
    {
       setupTestSubject();
       assertDDLCommandSuccess( adminSubject, String.format( "GRANT ROLE %s TO %s", ROLE, SUBJECT ) );
       assertSuccess( neo.login( SUBJECT, PASSWORD ),
                "RETURN test.nestedAllowedFunction('test.allowedFunction1()') AS value",
                r -> assertKeyIs( r, "value", "foo" ) );
    }

    @Test
    void shouldHandleNestedFunctionWithDifferentAllowedRole() throws Throwable
    {
        setupTestSubject();
        assertSuccess( neo.login( SUBJECT, PASSWORD ),
                "RETURN test.nestedAllowedFunction('test.allowedFunction2()') AS value",
                r -> assertKeyIs( r, "value", "foo" )
        );
    }

    //---------- clearing query cache -----------

    @Test
    void shouldNotClearQueryCachesIfNotAdmin()
    {
        assertFail( noneSubject, "CALL db.clearQueryCaches()", ACCESS_DENIED );
        assertFail( readSubject, "CALL db.clearQueryCaches()", PERMISSION_DENIED );
        assertFail( writeSubject, "CALL db.clearQueryCaches()", PERMISSION_DENIED );
        assertFail( schemaSubject, "CALL db.clearQueryCaches()", PERMISSION_DENIED );
    }

    @Test
    void shouldClearQueryCachesIfAdmin()
    {
        assertSuccess( adminSubject,"CALL db.clearQueryCaches()", ResourceIterator::close );
        // any answer is okay, as long as it isn't denied. That is why we don't care about the actual result here
    }

    //---------- system graph initialization/migration -----------

    @Test
    void shouldNotListSystemGraphComponentDetailsIfNotAdmin()
    {
        setupFakeSystemComponents();
        assertFail( noneSubject, "CALL dbms.upgradeStatusDetails()", ACCESS_DENIED );
        assertFail( readSubject, "CALL dbms.upgradeStatusDetails()", PERMISSION_DENIED );
        assertFail( writeSubject, "CALL dbms.upgradeStatusDetails()", PERMISSION_DENIED );
        assertFail( schemaSubject, "CALL dbms.upgradeStatusDetails()", PERMISSION_DENIED );
    }

    @Test
    void shouldListSystemGraphComponentDetailsIfAdmin()
    {
        setupFakeSystemComponents();
        assertSuccess( adminSubject, "CALL dbms.upgradeStatusDetails()", r ->
        {
            HashMap<String, String> statuses = new HashMap<>();
            while ( r.hasNext() )
            {
                Map<String,Object> row = r.next();
                statuses.put( resultAsString( row, "component" ), resultAsString( row, "status" ) );
            }
            r.close();
            assertThat( "Expected all test components to be found", statuses.size(), equalTo( 8 ) );
            assertThat(statuses.get("system-graph"), containsString( SystemGraphComponent.Status.REQUIRES_UPGRADE.name()));
            assertThat(statuses.get("component_A"), containsString( SystemGraphComponent.Status.CURRENT.name()));
            assertThat(statuses.get("component_B"), containsString( SystemGraphComponent.Status.CURRENT.name()));
            assertThat(statuses.get("component_C"), containsString( SystemGraphComponent.Status.REQUIRES_UPGRADE.name()));
            assertThat(statuses.get("component_D"), containsString( SystemGraphComponent.Status.REQUIRES_UPGRADE.name()));
        } );
    }

    @Test
    void shouldNotUpgradeSystemGraphWithDetailsIfNotAdmin()
    {
        setupFakeSystemComponents();
        assertFail( noneSubject, "CALL dbms.upgradeDetails()", ACCESS_DENIED );
        assertFail( readSubject, "CALL dbms.upgradeDetails()", PERMISSION_DENIED );
        assertFail( writeSubject, "CALL dbms.upgradeDetails()", PERMISSION_DENIED );
        assertFail( schemaSubject, "CALL dbms.upgradeDetails()", PERMISSION_DENIED );
    }

    @Test
    void shouldUpgradeSystemGraphWithDetailsIfAdmin()
    {
        setupFakeSystemComponents();
        assertSuccess( adminSubject,"CALL dbms.upgradeDetails()", r ->
        {
            assertThat( "Expected at least one result", r.hasNext(), equalTo( true ) );
            HashMap<String,String> statuses = new HashMap<>();
            HashMap<String,String> results = new HashMap<>();
            while ( r.hasNext() )
            {
                Map<String,Object> row = r.next();
                statuses.put( resultAsString( row, "component" ), resultAsString( row, "status" ) );
                results.put( resultAsString( row, "component" ), resultAsString( row, "upgradeResult" ) );
            }
            r.close();
            assertThat( "Expected all test components to be found", statuses.size(), equalTo( 8 ) );
            assertThat( statuses.get( "system-graph" ), containsString( SystemGraphComponent.Status.REQUIRES_UPGRADE.name() ) );
            assertThat( statuses.get( "component_A" ), containsString( SystemGraphComponent.Status.CURRENT.name() ) );
            assertThat( statuses.get( "component_B" ), containsString( SystemGraphComponent.Status.CURRENT.name() ) );
            assertThat( statuses.get( "component_C" ), containsString( SystemGraphComponent.Status.CURRENT.name() ) );
            assertThat( statuses.get( "component_D" ), containsString( SystemGraphComponent.Status.REQUIRES_UPGRADE.name() ) );
            assertThat( results.get( "system-graph" ), containsString( "Failed: component_D" ) );
            assertThat( results.get( "component_C" ), containsString( "Upgraded" ) );
            assertThat( results.get( "component_D" ), containsString( "Upgrade failed" ) );
            r.close();
        }  );
    }

    @Test
    void shouldNotListSystemGraphComponentsIfNotAdmin()
    {
        setupFakeSystemComponents();
        assertFail( noneSubject, "CALL dbms.upgradeStatus()", ACCESS_DENIED );
        assertFail( readSubject, "CALL dbms.upgradeStatus()", PERMISSION_DENIED );
        assertFail( writeSubject, "CALL dbms.upgradeStatus()", PERMISSION_DENIED );
        assertFail( schemaSubject, "CALL dbms.upgradeStatus()", PERMISSION_DENIED );
    }

    @Test
    void shouldListSystemGraphComponentsIfAdmin()
    {
        setupFakeSystemComponents();
        assertSuccess( adminSubject, "CALL dbms.upgradeStatus()", r ->
        {
            assertThat( "Expected one result", r.hasNext(), equalTo( true ) );
            Map<String,Object> row = r.next();
            String status = resultAsString( row, "status" );
            String description = resultAsString( row, "description" );
            String resolution = resultAsString( row, "resolution" );
            assertThat( "Expected only one result", r.hasNext(), equalTo( false ) );
            assertThat( status, containsString( SystemGraphComponent.Status.REQUIRES_UPGRADE.name() ) );
            assertThat( description, containsString( SystemGraphComponent.Status.REQUIRES_UPGRADE.description() ) );
            assertThat( resolution, containsString( SystemGraphComponent.Status.REQUIRES_UPGRADE.resolution() ) );
            r.close();
        } );
    }

    @Test
    void shouldNotUpgradeSystemGraphIfNotAdmin()
    {
        setupFakeSystemComponents();
        assertFail( noneSubject, "CALL dbms.upgrade()", ACCESS_DENIED );
        assertFail( readSubject, "CALL dbms.upgrade()", PERMISSION_DENIED );
        assertFail( writeSubject, "CALL dbms.upgrade()", PERMISSION_DENIED );
        assertFail( schemaSubject, "CALL dbms.upgrade()", PERMISSION_DENIED );
    }

    @Test
    void shouldUpgradeSystemGraphIfAdmin()
    {
        setupFakeSystemComponents();
        assertSuccess( adminSubject,"CALL dbms.upgrade()", r ->
        {
            assertThat( "Expected one result", r.hasNext(), equalTo( true ) );
            Map<String,Object> row = r.next();
            String status = resultAsString( row, "status" );
            String result = resultAsString( row, "upgradeResult" );
            assertThat( "Expected only one result", r.hasNext(), equalTo( false ) );
            assertThat( status, containsString( SystemGraphComponent.Status.REQUIRES_UPGRADE.name() ) );
            assertThat( result, containsString( "Failed: component_D" ) );
            r.close();
        }  );
    }

    /*
    ==================================================================================
     */

    private static class TestSystemGraphComponent implements SystemGraphComponent
    {
        final String component;
        SystemGraphComponent.Status status;
        Exception onInit;
        Exception onMigrate;

        TestSystemGraphComponent( String component, SystemGraphComponent.Status status, Exception onInit, Exception onMigrate )
        {
            this.component = component;
            this.status = status;
            this.onInit = onInit;
            this.onMigrate = onMigrate;
        }

        @Override
        public String component()
        {
            return component;
        }

        @Override
        public Status detect( Transaction tx )
        {
            return status;
        }

        @Override
        public Optional<Exception> initializeSystemGraph( GraphDatabaseService system )
        {
            if ( status == Status.UNINITIALIZED )
            {
                if ( onInit == null )
                {
                    status = Status.CURRENT;
                }
                else
                {
                    return Optional.of( onInit );
                }
            }
            return Optional.empty();
        }

        @Override
        public Optional<Exception> upgradeToCurrent( Transaction tx )
        {
            if ( status == Status.REQUIRES_UPGRADE )
            {
                if ( onMigrate == null )
                {
                    status = Status.CURRENT;
                }
                else
                {
                    return Optional.of( onMigrate );
                }
            }
            return Optional.empty();
        }
    }

    private SystemGraphComponent makeSystemComponentCurrent( String component )
    {
        return new TestSystemGraphComponent( component, SystemGraphComponent.Status.CURRENT, null, null );
    }

    @SuppressWarnings( "SameParameterValue" )
    private SystemGraphComponent makeSystemComponentUpgradeSucceeds( String component )
    {
        return new TestSystemGraphComponent( component, SystemGraphComponent.Status.REQUIRES_UPGRADE, null, null );
    }

    @SuppressWarnings( "SameParameterValue" )
    private SystemGraphComponent makeSystemComponentUpgradeFails( String component )
    {
        return new TestSystemGraphComponent( component, SystemGraphComponent.Status.REQUIRES_UPGRADE, null,
                new RuntimeException( "Upgrade failed because this is a test" ) );
    }

    private void setupFakeSystemComponents()
    {
        SystemGraphComponents initializers = neo.getSystemGraph().getDependencyResolver().resolveDependency( SystemGraphComponents.class );
        initializers.register( makeSystemComponentCurrent( "component_A" ) );
        initializers.register( makeSystemComponentCurrent( "component_B" ) );
        initializers.register( makeSystemComponentUpgradeSucceeds( "component_C" ) );
        initializers.register( makeSystemComponentUpgradeFails( "component_D" ) );
    }

    private String resultAsString( Map<String,Object> row, String key )
    {
        Object result = row.get( key );
        if ( result instanceof TextValue )
        {
            return ((TextValue) result).stringValue();
        }
        else if ( result instanceof Value )
        {
            return ((Value) result).asObjectCopy().toString();
        }
        else
        {
            return result.toString();
        }
    }

    private void setupTestSubject()
    {
        assertDDLCommandSuccess( adminSubject, String.format( "CREATE USER %s SET PASSWORD '%s' CHANGE NOT REQUIRED", SUBJECT, PASSWORD  ));
        createRoleWithAccess( ROLE, SUBJECT );
    }

    private void assertQueryIsRunning( String query )
    {
        assertEventually( "Query did not appear in dbms.listQueries output", () -> queryIsRunning( query ), TRUE, 1, MINUTES );
    }

    private boolean queryIsRunning( String targetQuery )
    {
        String query = "CALL dbms.listQueries() YIELD query WITH query WHERE query = '" + targetQuery + "' RETURN 1";
        MutableBoolean resultIsNotEmpty = new MutableBoolean();
        neo.executeQuery( adminSubject, DEFAULT_DATABASE_NAME, query, emptyMap(), itr -> resultIsNotEmpty.setValue( itr.hasNext() ) );
        return resultIsNotEmpty.booleanValue();
    }

    @SuppressWarnings( "unchecked" )
    private static Map<String,Object> getResultRowForMetadataQuery( Transaction tx )
    {
        Result result = tx.execute( "call tx.getMetaData() yield metadata return metadata" );
        Map<String,Object> row = (Map<String,Object>) result.next().get( "metadata" );
        assertFalse( result.hasNext() );
        return row;
    }

    //---------- jetty helpers for serving CSV files -----------

    private static int getLocalPort( Server server )
    {
        return ((ServerConnector) (server.getConnectors()[0])).getLocalPort();

    }

    //---------- matchers-----------

    private Matcher<Map<String,Object>> listedTransactionOfInteractionLevel( OffsetDateTime startTime, String
            username, String currentQuery )
    {
        return allOf(
                hasCurrentQuery( currentQuery ),
                hasUsername( username ),
                hasTransactionId(),
                hasStartTimeAfter( startTime ),
                hasProtocol( neo.getConnectionProtocol() )
        );
    }

    private static Matcher<Map<String,Object>> listedQuery( OffsetDateTime startTime, String username, String query )
    {
        return allOf(
                hasQuery( query ),
                hasUsername( username ),
                hasQueryId(),
                hasStartTimeAfter( startTime ),
                hasNoParameters()
        );
    }

    private static Matcher<Map<String,Object>> listedTransaction( OffsetDateTime startTime, String username, String currentQuery )
    {
        return allOf(
                hasCurrentQuery( currentQuery ),
                hasUsername( username ),
                hasTransactionId(),
                hasStartTimeAfter( startTime )
        );
    }

    /**
     * Executes a query through the NeoInteractionLevel required
     */
    private Matcher<Map<String,Object>> listedQueryOfInteractionLevel( OffsetDateTime startTime, String username,
            String query )
    {
        return allOf(
                hasQuery( query ),
                hasUsername( username ),
                hasQueryId(),
                hasStartTimeAfter( startTime ),
                hasNoParameters(),
                hasProtocol( neo.getConnectionProtocol() )
        );
    }

    private static Matcher<Map<String,Object>> listedQueryWithMetaData( OffsetDateTime startTime, String username, String query, Map<String,Object> metaData )
    {
        return allOf(
                hasQuery( query ),
                hasUsername( username ),
                hasQueryId(),
                hasStartTimeAfter( startTime ),
                hasNoParameters(),
                hasMetaData( metaData )
        );
    }

    private static Matcher<Map<String,Object>> listedTransactionWithMetaData( OffsetDateTime startTime, String username, String currentQuery,
            Map<String,Object> metaData )
    {
        return allOf(
                hasCurrentQuery( currentQuery ),
                hasUsername( username ),
                hasTransactionId(),
                hasStartTimeAfter( startTime ),
                hasMetaData( metaData )
        );
    }

    @SuppressWarnings( "unchecked" )
    private static Matcher<Map<String,Object>> hasQuery( String query )
    {
        Matcher<?> queryMatcher = hasEntry( equalTo( "query" ), Matchers.<Object>equalTo( query ) );
        return (Matcher<Map<String,Object>>) queryMatcher;
    }

    @SuppressWarnings( "unchecked" )
    private static Matcher<Map<String,Object>> hasCurrentQuery( String currentQuery )
    {
        Matcher<?> currentQueryMatcher = hasEntry( equalTo( "currentQuery" ), equalTo( currentQuery ) );
        return (Matcher<Map<String,Object>>) currentQueryMatcher;
    }

    @SuppressWarnings( "unchecked" )
    private static Matcher<Map<String,Object>> hasStatus( String statusPrefix )
    {
        Matcher<?> statusMatcher = hasEntry( equalTo( "status" ), startsWith( statusPrefix ) );
        return (Matcher<Map<String,Object>>) statusMatcher;
    }

    @SuppressWarnings( "unchecked" )
    private static Matcher<Map<String,Object>> hasResultEntry( String entryKey, String entryPrefix )
    {
        Matcher<?> resultsEntryMatcher = hasEntry( equalTo( entryKey ), startsWith( entryPrefix ) );
        return (Matcher<Map<String,Object>>) resultsEntryMatcher;
    }

    @SuppressWarnings( "unchecked" )
    private static Matcher<Map<String,Object>> hasUsername( String username )
    {
        Matcher<?> usernameMatcher = hasEntry( equalTo( "username" ), equalTo( username ) );
        return (Matcher<Map<String,Object>>) usernameMatcher;
    }

    @SuppressWarnings( "unchecked" )
    private static Matcher<Map<String,Object>> hasQueryId()
    {
        Matcher<String> queryId = equalTo( "queryId" );
        Matcher<String> valueMatcher = allOf( isA( String.class ), containsString( DbmsQueryId.QUERY_ID_SEPARATOR ) );
        Matcher<?> queryIdMatcher = hasEntry( queryId, valueMatcher );
        return (Matcher<Map<String,Object>>) queryIdMatcher;
    }

    @SuppressWarnings( "unchecked" )
    private static Matcher<Map<String,Object>> hasTransactionId()
    {
        Matcher<String> transactionId = equalTo( "transactionId" );
        Matcher<String> valueMatcher =
                allOf( isA( String.class ), containsString( "-transaction-" ) );
        Matcher<?> transactionIdMatcher = hasEntry( transactionId, valueMatcher );
        return (Matcher<Map<String,Object>>) transactionIdMatcher;
    }

    @SuppressWarnings( "unchecked" )
    private static Matcher<Map<String,Object>> hasStartTimeAfter( OffsetDateTime startTime )
    {
        Matcher<?> startTimeMatcher = hasEntry( equalTo( "startTime" ), new BaseMatcher<String>()
        {
            @Override
            public void describeTo( Description description )
            {
                description.appendText( "should be after " + startTime.toString() );
            }

            @Override
            public boolean matches( Object item )
            {
                OffsetDateTime otherTime = from( ISO_OFFSET_DATE_TIME.parse( item.toString() ) );
                return startTime.compareTo( otherTime ) <= 0;
            }
        } );
        return (Matcher<Map<String,Object>>) startTimeMatcher;
    }

    @SuppressWarnings( "unchecked" )
    private static Matcher<Map<String,Object>> hasNoParameters()
    {
        Matcher<?> noParametersMatcher = hasEntry( equalTo( "parameters" ), equalTo( emptyMap() ) );
        return (Matcher<Map<String,Object>>) noParametersMatcher;
    }

    @SuppressWarnings( "unchecked" )
    private static Matcher<Map<String,Object>> hasProtocol( String expected )
    {
        Matcher<?> protocolMatcher = hasEntry( "protocol", expected );
        return (Matcher<Map<String,Object>>) protocolMatcher;
    }

    @SuppressWarnings( "unchecked" )
    private static Matcher<Map<String,Object>> hasMetaData( Map<String,Object> expected )
    {
        Matcher<?> metaDataMatcher =
                hasEntry( equalTo( "metaData" ), allOf( expected.entrySet().stream().map( entryMapper() ).collect( toList() ) ) );
        return (Matcher<Map<String,Object>>) metaDataMatcher;
    }

    @SuppressWarnings( "unchecked" )
    private static Function<Entry<String,Object>,Matcher<Entry<String,Object>>> entryMapper()
    {
        return entry ->
        {
            Matcher<String> keyMatcher = equalTo( entry.getKey() );
            Matcher<Object> valueMatcher = equalTo( entry.getValue() );
            Matcher<?> entryMatcher = hasEntry( keyMatcher, valueMatcher );
            return (Matcher<Entry<String,Object>>) entryMatcher;
        };
    }

    private List<Map<String,Object>> collectResults( ResourceIterator<Map<String,Object>> results )
    {
        List<Map<String,Object>> maps = results.stream().collect( Collectors.toList() );
        List<Map<String,Object>> transformed = new ArrayList<>( maps.size() );
        for ( Map<String,Object> map : maps )
        {
            Map<String,Object> transformedMap = new HashMap<>( map.size() );
            for ( Entry<String,Object> entry : map.entrySet() )
            {
                transformedMap.put( entry.getKey(), toRawValue( entry.getValue() ) );
            }
            transformed.add( transformedMap );
        }
        return transformed;
    }

    static Server createHttpServer( DoubleLatch latch, Barrier.Control innerBarrier, int firstBatchSize, int otherBatchSize )
    {
        Server server = new Server( 0 );
        server.setHandler( new AbstractHandler()
        {
            @Override
            public void handle(
                    String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response )
                    throws IOException
            {
                response.setContentType( "text/plain; charset=utf-8" );
                response.setStatus( HttpServletResponse.SC_OK );
                PrintWriter out = response.getWriter();

                writeBatch( out, firstBatchSize );
                out.flush();
                latch.start();
                innerBarrier.reached();

                latch.finish();
                writeBatch( out, otherBatchSize );
                baseRequest.setHandled(true);
            }

            private void writeBatch( PrintWriter out, int batchSize )
            {
                for ( int i = 0; i < batchSize; i++ )
                {
                    out.write( format( "%d %d\n", i, i * i ) );
                    i++;
                }
            }
        } );
        return server;
    }

    private static OffsetDateTime getStartTime()
    {
        return ofInstant( Instant.ofEpochMilli( now().toEpochSecond() ), ZoneOffset.UTC );
    }

    private static void waitTransactionToStartWaitingForTheLock() throws InterruptedException
    {
        while ( Thread.getAllStackTraces().keySet().stream().noneMatch(
                ThreadingRule.waitingWhileIn( Operations.class, "acquireExclusiveNodeLock" ) ) )
        {
            TimeUnit.MILLISECONDS.sleep( 10 );
        }
    }

    @SuppressWarnings( "unused" )
    public static class TestProcedures
    {
        @Procedure( name = "test.neverEnding" )
        public void neverEndingWithLock( @Name( "supportId" ) long supportId )
        {
            Support support = Support.getSupport( supportId );
            support.doubleLatch.start();
            support.doubleLatch.finishAndWaitForAllToFinish();
        }
    }
}
