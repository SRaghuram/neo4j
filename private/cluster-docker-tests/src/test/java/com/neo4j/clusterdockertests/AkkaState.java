/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.clusterdockertests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.neo4j.test.driver.ClusterChecker;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.driver.Result;
import org.neo4j.driver.TransactionWork;
import org.neo4j.driver.Value;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Akka state is a json string returned from a cypher query.
 * This class extracts the main information that we are interested in
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

    public static AkkaState verifyAllServersShowSameAkkaState( ClusterChecker checker ) throws ExecutionException, InterruptedException
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
