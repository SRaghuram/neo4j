/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.factory;

import org.junit.Test;

import java.lang.reflect.Proxy;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.impl.api.ReadOnlyTransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.storageengine.api.StorageEngine;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class HighlyAvailableCommitProcessFactoryTest
{
    @Test
    public void createReadOnlyCommitProcess()
    {
        HighlyAvailableCommitProcessFactory factory = new HighlyAvailableCommitProcessFactory(
                new DelegateInvocationHandler<>( TransactionCommitProcess.class ) );

        Config config = Config.defaults( GraphDatabaseSettings.read_only, "true" );

        TransactionCommitProcess commitProcess = factory.create( mock( TransactionAppender.class ),
                mock( StorageEngine.class ), config );

        assertThat( commitProcess, instanceOf( ReadOnlyTransactionCommitProcess.class ) );
    }

    @Test
    public void createRegularCommitProcess()
    {
        HighlyAvailableCommitProcessFactory factory = new HighlyAvailableCommitProcessFactory(
                new DelegateInvocationHandler<>( TransactionCommitProcess.class ) );

        TransactionCommitProcess commitProcess = factory.create( mock( TransactionAppender.class ),
                mock( StorageEngine.class ), Config.defaults() );

        assertThat( commitProcess, not( instanceOf( ReadOnlyTransactionCommitProcess.class ) ) );
        assertThat( Proxy.getInvocationHandler( commitProcess ), instanceOf( DelegateInvocationHandler.class ) );
    }
}
