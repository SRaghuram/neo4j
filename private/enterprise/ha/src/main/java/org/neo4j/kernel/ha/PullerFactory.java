/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha;

import org.neo4j.cluster.InstanceId;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachine;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.com.slave.InvalidEpochExceptionHandler;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

/**
 * Helper factory that provide more convenient way of construction and dependency management for update pulling
 * related components
 */
public class PullerFactory
{
    private final RequestContextFactory requestContextFactory;
    private final Master master;
    private final LastUpdateTime lastUpdateTime;
    private final LogProvider logging;
    private final InstanceId serverId;
    private final InvalidEpochExceptionHandler invalidEpochHandler;
    private final long pullInterval;
    private final JobScheduler jobScheduler;
    private final DependencyResolver dependencyResolver;
    private final AvailabilityGuard availabilityGuard;
    private final HighAvailabilityMemberStateMachine memberStateMachine;
    private final Monitors monitors;
    private final String activeDatabaseName;

    public PullerFactory( RequestContextFactory requestContextFactory, Master master, LastUpdateTime lastUpdateTime, LogProvider logging, InstanceId serverId,
            InvalidEpochExceptionHandler invalidEpochHandler, long pullInterval, JobScheduler jobScheduler, DependencyResolver dependencyResolver,
            AvailabilityGuard availabilityGuard, HighAvailabilityMemberStateMachine memberStateMachine, Monitors monitors, Config config )
    {

        this.requestContextFactory = requestContextFactory;
        this.master = master;
        this.lastUpdateTime = lastUpdateTime;
        this.logging = logging;
        this.serverId = serverId;
        this.invalidEpochHandler = invalidEpochHandler;
        this.pullInterval = pullInterval;
        this.jobScheduler = jobScheduler;
        this.dependencyResolver = dependencyResolver;
        this.availabilityGuard = availabilityGuard;
        this.memberStateMachine = memberStateMachine;
        this.monitors = monitors;
        this.activeDatabaseName = config.get( GraphDatabaseSettings.active_database );
    }

    public SlaveUpdatePuller createSlaveUpdatePuller()
    {
        return new SlaveUpdatePuller( requestContextFactory, master, lastUpdateTime, logging, serverId, availabilityGuard, invalidEpochHandler,
                jobScheduler, monitors.newMonitor( SlaveUpdatePuller.Monitor.class ) );
    }

    public UpdatePullingTransactionObligationFulfiller createObligationFulfiller( UpdatePuller updatePuller )
    {
        return new UpdatePullingTransactionObligationFulfiller( updatePuller, memberStateMachine, serverId, () ->
        {
            GraphDatabaseFacade databaseFacade =
                    this.dependencyResolver.resolveDependency( DatabaseManager.class ).getDatabaseFacade( activeDatabaseName ).get();
            DependencyResolver databaseResolver = databaseFacade.getDependencyResolver();
            return databaseResolver.resolveDependency( TransactionIdStore.class );
        } );
    }

    public UpdatePullerScheduler createUpdatePullerScheduler( UpdatePuller updatePuller )
    {
        return new UpdatePullerScheduler( jobScheduler, logging, updatePuller, pullInterval );
    }
}
