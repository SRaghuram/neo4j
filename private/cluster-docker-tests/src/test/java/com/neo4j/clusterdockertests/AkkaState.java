/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.clusterdockertests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.neo4j.test.driver.ClusterChecker;
import com.neo4j.test.driver.DriverFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;
import org.neo4j.driver.TransactionWork;
import org.neo4j.driver.Value;
import org.neo4j.junit.jupiter.causal_cluster.Neo4jServer;

import static com.neo4j.clusterdockertests.MetricsReader.isEqualTo;
import static com.neo4j.clusterdockertests.MetricsReader.replicatedDataMetric;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Akka state is a json string returned from a cypher query. This class extracts the main information that we are interested in
 */
final class AkkaState
{
    private final String selfAddress;
    private final String leader;
    private final Set<String> members;
    private final Set<String> unreachable;
    private final Set<String> up;
    private final Set<String> notUp;

    private static final ObjectMapper mapper = new ObjectMapper();

    private static final Function<String,Map<String,Object>> toMap = s ->
    {
        try
        {
            return mapper.readValue( s, Map.class );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    };

    private AkkaState( String selfAddress, String leader, Set<String> members, Set<String> unreachable,
                       Set<String> up, Set<String> notUp )
    {
        this.selfAddress = selfAddress;
        this.leader = leader;
        this.members = members;
        this.unreachable = unreachable;
        this.up = up;
        this.notUp = notUp;
    }

    public static void checkMetrics( Set<Neo4jServer> servers, DriverFactory driverFactory, int databaseCount ) throws IOException
    {
        var metricsReader = new MetricsReader();
        var expectedMetrics = new MetricsReader.MetricExpectations();
        var aliveMembers = (int) servers.stream().filter( Neo4jServer::isContainerRunning ).count();

        expectedMetrics.add( replicatedDataMetric( "raft_id_published_by_member.visible" ), isEqualTo( databaseCount ) )
                       .add( replicatedDataMetric( "per_db_leader_name.visible" ),          isEqualTo( databaseCount ) )
                       .add( replicatedDataMetric( "member_data.visible" ),                 isEqualTo( aliveMembers ) )
                       .add( replicatedDataMetric( "member_db_state.visible" ),             isEqualTo( aliveMembers * databaseCount ) )
                       .add( replicatedDataMetric( "raft_member_mapping.visible" ),         isEqualTo( aliveMembers * databaseCount ) );

        // TODO: make concurrent using async
        for ( var server : servers )
        {
            if ( server.isContainerRunning() )
            {
                try ( Driver driver = driverFactory.graphDatabaseDriver( server.getDirectBoltUri() ) )
                {
                    metricsReader.checkExpectations( driver, expectedMetrics );
                }
            }
        }
    }

    public Set<String> getMembers()
    {
        return Set.copyOf( members );
    }

    public Set<String> getUnreachable()
    {
        return Set.copyOf( unreachable );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        AkkaState akkaState = (AkkaState) o;
        return // we deliberately don't include self address in equality because it would make comparing cluster state from different cores very fiddly
                leader.equals( akkaState.leader ) &&
                members.equals( akkaState.members ) &&
                unreachable.equals( akkaState.unreachable ) &&
                up.equals( akkaState.up ) &&
                notUp.equals( akkaState.notUp );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                // don't include self address in hash because it would make comparing cluster state from different cores very fiddly
                leader, members, unreachable, up, notUp
        );
    }

    static AkkaState FromQueryResult( Map<String,Value> result )
    {
        assert result.size() == 7;

        Map<String,Object> clusterStatus = toMap.apply( result.get( "ClusterStatus" ).asString() );
        List<Map<String,Object>> clusterStateMembers = (List<Map<String,Object>>) clusterStatus.get( "members" );

        Set<String> members = Arrays.stream( result.get( "Members" ).asString().split( "," ) ).filter( m -> !m.isEmpty() )
                                    .collect( Collectors.toSet() );

        assertThat( members ).isEqualTo( clusterStateMembers.stream()
                                                            .map( m -> (String) m.get( "address" ) )
                                                            .filter( m -> !((String) m).isEmpty() ).collect( Collectors.toSet() ) );

        // The akka state reports two different flavours of "unreachable". We just take the union for simplicity
        Set<String> unreachable = Arrays.stream( result.get( "Unreachable" ).asString().split( "," ) )
                                        .filter( m -> !m.isEmpty() ).collect( Collectors.toSet() );

        unreachable.addAll( ((List<Map<String,Object>>) clusterStatus.get( "unreachable" )).stream()
                                                                                           .map( m -> (String) m.get( "node" ) )
                                                                                           .filter( m -> !m.isEmpty() )
                                                                                           .collect( Collectors.toSet() ) );

        Set<String> up = clusterStateMembers.stream()
                                            .filter( m -> ((String) m.get( "status" )).equalsIgnoreCase( "Up" ) )
                                            .map( m -> (String) m.get( "address" ) )
                                            .filter( m -> !m.isEmpty() )
                                            .collect( Collectors.toSet() );

        Set<String> notUp = clusterStateMembers.stream()
                                               .filter( m -> !((String) m.get( "status" )).equalsIgnoreCase( "Up" ) )
                                               .map( m -> (String) m.get( "address" ) )
                                               .filter( m -> !m.isEmpty() )
                                               .collect( Collectors.toSet() );

        return new AkkaState(
                (String) clusterStatus.get( "self-address" ),
                result.get( "Leader" ).asString(),
                members,
                unreachable,
                up,
                notUp
        );
    }

    @Override
    public String toString()
    {
        return "AkkaState{" +
               "selfAddress='" + selfAddress + '\'' +
               ", leader='" + leader + '\'' +
               ", members=" + members +
               ", unreachable=" + unreachable +
               ", up=" + up +
               ", notUp=" + notUp +
               '}';
    }

    public static AkkaState verifyAllServersShowSameAkkaState( ClusterChecker checker ) throws ExecutionException, InterruptedException, TimeoutException
    {
        Map<URI,AkkaState> akkaStates = checker.runOnAllServers( getAkkaState );
        assertThat( akkaStates ).hasSize( checker.size() );
        Set<AkkaState> asSet = Set.copyOf( akkaStates.values() );
        assertThat( asSet ).hasSize( 1 );
        AkkaState akkaState = asSet.stream().findFirst().get();
        assertThat( akkaState.getMembers().size() - akkaState.getUnreachable().size() ).isGreaterThanOrEqualTo( checker.size() );
        return akkaState;
    }

    private static final TransactionWork<AkkaState> getAkkaState = tx ->
    {
        Result result = tx.run(
                "CALL dbms.queryJmx(\"akka:*\") YIELD attributes " +
                "UNWIND [ k in keys(attributes) | [k,attributes[k].value] ] as val " +
                "WITH val WHERE val[1] is not NULL " +
                "RETURN val[0] as key, val[1] as value" );
        Map<String,Value> records = result.stream()
                                          .collect( Collectors.toMap( r -> r.get( "key" ).asString(), r -> r.get( "value" ) ) );
        assertThat( records.keySet() )
                .containsOnly( "Leader", "Unreachable", "Singleton", "Available", "MemberStatus", "Members",
                               "ClusterStatus" );

        return AkkaState.FromQueryResult( records );
    };
}
