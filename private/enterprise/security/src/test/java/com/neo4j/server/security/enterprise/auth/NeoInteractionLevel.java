/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

public interface NeoInteractionLevel<S>
{
    EnterpriseUserManager getLocalUserManager() throws Exception;

    GraphDatabaseFacade getLocalGraph();

    FileSystemAbstraction fileSystem();

    InternalTransaction beginLocalTransactionAsUser( S subject, KernelTransaction.Type txType ) throws Throwable;

    /*
     * The returned String is empty if the query executed as expected, and contains an error msg otherwise
     */
    String executeQuery( S subject, String call, Map<String,Object> params,
            Consumer<ResourceIterator<Map<String, Object>>> resultConsumer );

    S login( String username, String password ) throws Exception;

    void logout( S subject ) throws Exception;

    void updateAuthToken( S subject, String username, String password );

    String nameOf( S subject );

    void tearDown() throws Throwable;

    static String tempPath( String prefix, String suffix ) throws IOException
    {
        Path path = Files.createTempFile( prefix, suffix );
        Files.delete( path );
        return path.toString();
    }

    void assertAuthenticated( S subject );

    void assertPasswordChangeRequired( S subject );

    void assertInitFailed( S subject );

    void assertSessionKilled( S subject );

    String getConnectionProtocol();

    HostnamePort lookupConnector( String connectorKey );
}
