/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

public interface NeoInteractionLevel<S>
{
    GraphDatabaseFacade getLocalGraph();

    GraphDatabaseFacade getSystemGraph();

    default void removeAccessFromPublicRole()
    {
        try ( Transaction tx = getSystemGraph().beginTx() )
        {
            tx.execute( "REVOKE ACCESS ON DEFAULT DATABASE FROM PUBLIC" );
            tx.commit();
        }
    }

    void shutdown();

    FileSystemAbstraction fileSystem();

    InternalTransaction beginLocalTransactionAsUser( S subject, KernelTransaction.Type txType ) throws Throwable;

    InternalTransaction beginLocalTransactionAsUser( S subject, KernelTransaction.Type txType, String database ) throws Throwable;

    /*
     * The returned String is empty if the query executed as expected, and contains an error msg otherwise
     */
    String executeQuery( S subject, String database, String call, Map<String,Object> params,
            Consumer<ResourceIterator<Map<String,Object>>> resultConsumer );

    S login( String username, String password ) throws Exception;

    void updateAuthToken( S subject, String username, String password );

    String nameOf( S subject );

    void tearDown() throws Throwable;

    void assertAuthenticated( S subject );

    void assertPasswordChangeRequired( S subject );

    void assertUnauthenticated( S subject );

    String getConnectionProtocol();

    void registerTransactionEventListener( String databaseName, TransactionEventListener<?> listener );

    void unregisterTransactionEventListener( String databaseName, TransactionEventListener<?> listener );
}
