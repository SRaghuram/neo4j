package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.Inbound;
import com.neo4j.dbms.database.ClusteredDatabaseContext;

import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

public class LeaderTransferService extends LifecycleAdapter implements FailedLeaderTransferHandler
{
    private final TransferLeader transferLeader;
    private JobScheduler jobScheduler;
    private final long schedulingTime;
    private final TimeUnit timeUnit;
    private JobHandle<?> jobHandle;

    public LeaderTransferService( JobScheduler jobScheduler, long schedulingTime, TimeUnit timeUnit, TopologyService topologyService, Config config,
            DatabaseManager<ClusteredDatabaseContext> databaseManager,
            Inbound.MessageHandler<RaftMessages.ReceivedInstantRaftIdAwareMessage<?>> messageHandler, MemberId myself )
    {
        this.jobScheduler = jobScheduler;
        this.schedulingTime = schedulingTime;
        this.timeUnit = timeUnit;
        this.transferLeader = new TransferLeader( topologyService, config, databaseManager, messageHandler, myself, ( validTopologies, myself1 ) -> null );
    }

    @Override
    public void start() throws Exception
    {
        jobHandle = jobScheduler.scheduleRecurring( Group.LEADER_TRANSFER_SERICE, transferLeader, schedulingTime, timeUnit );
    }

    @Override
    public void stop()
    {
        if ( jobHandle != null )
        {
            jobHandle.cancel();
        }
    }

    @Override
    public void handle( RaftMessages.LeadershipTransfer.Rejection rejection )
    {

    }
}
