/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka

import akka.actor.AbstractActor
import akka.actor.ActorLogging
import akka.actor.TimerScheduler
import akka.actor.Timers

/**
  * We need this abstract class because some actors need both Timers and ActorLogging traits.
  *
  * The alternatives for actors needing just a single of these traits are
  * [[akka.actor.AbstractLoggingActor]] and [[akka.actor.AbstractActorWithTimers]].
  */
abstract class AbstractActorWithTimersAndLogging extends AbstractActor with Timers with ActorLogging {
  /**
    * Start and cancel timers via the enclosed `TimerScheduler`.
    */
  final def getTimers: TimerScheduler = timers
}
