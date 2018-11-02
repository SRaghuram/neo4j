/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster;

import java.util.concurrent.Executor;

import org.neo4j.cluster.com.message.MessageSender;
import org.neo4j.cluster.com.message.MessageSource;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectInputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectOutputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AcceptorInstanceStore;
import org.neo4j.cluster.protocol.election.ElectionCredentialsProvider;
import org.neo4j.cluster.timeout.TimeoutStrategy;
import org.neo4j.kernel.configuration.Config;

/**
 * Factory for instantiating ProtocolServers.
 *
 * @see ProtocolServer
 */
public interface ProtocolServerFactory
{
    ProtocolServer newProtocolServer( InstanceId me, TimeoutStrategy timeouts, MessageSource input, MessageSender output,
                                      AcceptorInstanceStore acceptorInstanceStore,
                                      ElectionCredentialsProvider electionCredentialsProvider,
                                      Executor stateMachineExecutor,
                                      ObjectInputStreamFactory objectInputStreamFactory,
                                      ObjectOutputStreamFactory objectOutputStreamFactory,
                                      Config config );
}
