/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness.workload.full;

import com.neo4j.cc_robustness.CcInstance;
import com.neo4j.cc_robustness.Orchestrator;
import com.neo4j.cc_robustness.util.Duration;
import com.neo4j.cc_robustness.workload.ShutdownType;
import com.neo4j.cc_robustness.workload.ShutdownTypeSelector;

import java.util.LinkedList;

import org.neo4j.logging.LogProvider;

import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

class Killer extends Worker
{
    private final LinkedList<CustomShutdownEntry> shutdownQueue = new LinkedList<>();
    private final Configuration config;
    private final InstanceHealth instanceHealth;
    private volatile boolean killerPaused;

    Killer( Configuration config, Orchestrator orchestrator, Condition endCondition, RemoteControl remoteControl, ReportingFactory reporting,
            InstanceHealth instanceHealth, LogProvider logProvider )
    {
        super( "Killer", config, orchestrator, endCondition, remoteControl, reporting, logProvider );
        this.config = config;
        this.instanceHealth = instanceHealth;
        remoteControl.addListener( new RemoteControlListener.Adapter()
        {
            @Override
            public void killerPausedChanged( boolean pause )
            {
                killerPaused = pause;
            }
        } );
    }

    void queue( String server, ShutdownType type, boolean runNow )
    {
        shutdownQueue.offer( new CustomShutdownEntry( server, type ) );
        log.info( "QUEUED " + server + " to go down. Queue not at " + shutdownQueue );

        if ( runNow )
        {
            wakeUp();
        }
    }

    @Override
    protected boolean paused()
    {
        return super.paused() || killerPaused;
    }

    @Override
    protected void doOneOperation() throws Exception
    {
        CustomShutdownEntry customShutdown = shutdownQueue.poll();
        Integer serverIdInt = getQueuedServerId( customShutdown );
        CcInstance instance = serverIdInt == null ? config.instanceSelector().select( orchestrator ) : orchestrator.getCcInstance( serverIdInt );
        if ( instance == null )
        {
            return;
        }
        int serverId = instance.getServerId();
        setCurrentServerId( serverId );
        ShutdownType shutdownType = config.shutdownTypeSelector().select();
        String leaderString = instance.isLeader() ? " (leader)" : "";
        if ( !instanceHealth.isOk( instance ) )
        {
            log.info( "Instance " + serverId + " isn't OK, so will not bring it down as it would potentially solve (and mask) the problem" );
            return;
        }

        log.info( "Bringing DOWN " + serverId + leaderString + " " + shutdownType );

        orchestrator.shutdownCcInstance( serverId, shutdownType, config.gentleness() != Gentleness.not );
        reporting.clearErrors( serverId );
        log.info( serverId + " is now down, verifying consistency" );
        long start = nanoTime();
        try
        {
            instance.verifyConsistencyOffline();
        }
        catch ( AssertionError e )
        {
            log.error( getClass().getSimpleName() + " =============== CONSISTENCY CHECK FAILED?" );
            throw e;
        }
        long timeVerify = NANOSECONDS.toMillis( nanoTime() - start );
        sleeep( config.waitBeforeResurrect().to( MILLISECONDS, random ) );
        log.info( "Bringing up " + serverId );
        start = nanoTime();
        orchestrator.startCcInstance( serverId );
        long timeStarted = NANOSECONDS.toMillis( nanoTime() - start );
        log.info( serverId + " is up again timeStart:" + timeStarted + "ms, timeVerify:" + timeVerify + "ms" );
    }

    private Integer getQueuedServerId( CustomShutdownEntry customShutdown )
    {
        try
        {
            return customShutdown != null ? (customShutdown.serverDefinition.equals( "master" ) ? orchestrator.getLeaderServerId() : Integer.parseInt(
                    customShutdown.serverDefinition )) : null;
        }
        catch ( Exception e )
        {
            log.warn( "Invalid instance spec " + customShutdown );
            return null;
        }
    }

    public interface Configuration extends Worker.Configuration
    {
        ShutdownTypeSelector shutdownTypeSelector();

        Duration waitBeforeResurrect();
    }

    private static class CustomShutdownEntry
    {
        private final String serverDefinition; // serverId or "master"
        private final ShutdownType type; // null means default

        CustomShutdownEntry( String serverDefinition, ShutdownType type )
        {
            this.serverDefinition = serverDefinition;
            this.type = type;
        }

        @Override
        public String toString()
        {
            return "[" + serverDefinition + " " + type + "]";
        }
    }
}
