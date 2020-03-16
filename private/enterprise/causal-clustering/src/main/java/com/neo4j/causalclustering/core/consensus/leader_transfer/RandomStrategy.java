package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.shuffle;

class RandomStrategy implements SelectionStrategy
{
    @Override
    public LeaderTransferContext select( List<DatabaseCoreTopology> validTopologies, MemberId myself )
    {
        var databaseCoreTopologies = new ArrayList<>( validTopologies );
        shuffle( databaseCoreTopologies );
        for ( DatabaseCoreTopology validTopology : databaseCoreTopologies )
        {
            var members = validTopology.members().keySet().stream().filter( memberId -> !memberId.equals( myself ) ).collect( Collectors.toList() );
            shuffle( members );
            if ( !members.isEmpty() )
            {
                return new LeaderTransferContext( validTopology.raftId(), members.get( 0 ) );
            }
        }
        return null;
    }
}
