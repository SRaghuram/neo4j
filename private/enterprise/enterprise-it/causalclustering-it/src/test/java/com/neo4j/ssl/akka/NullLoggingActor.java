/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.ssl.akka;

import akka.actor.AbstractActor;
import akka.dispatch.RequiresMessageQueue;
import akka.event.LoggerMessageQueueSemantics;
import akka.event.Logging;

public class NullLoggingActor extends AbstractActor implements RequiresMessageQueue<LoggerMessageQueueSemantics>
{
    @Override
    public Receive createReceive()
    {
        return receiveBuilder()
                .match( Logging.LogEvent.class, ignore -> {} )
                .match( Logging.InitializeLogger.class, ignored -> sender().tell( Logging.loggerInitialized(), self() ) )
                .build();
    }
}
