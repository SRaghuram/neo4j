/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.core.ServerGroupName;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

import org.neo4j.configuration.Config;

import static com.neo4j.causalclustering.core.consensus.leader_transfer.LeadershipPriorityGroupSetting.READER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class LeadershipPriorityGroupSettingReaderTest
{
    @Test
    void shouldFindCorrectGroupForDatabase()
    {
        var one = new LeadershipPriorityGroupSetting( "one" );
        var two = new LeadershipPriorityGroupSetting( "two" );
        var three = new LeadershipPriorityGroupSetting( "three" );
        var invalid = new LeadershipPriorityGroupSetting( "" );

        var setting = Map.of(
                one.setting().name(), "1",
                two.setting().name(), "2",
                three.setting().name(), "3",
                invalid.setting().name(), "4" );
        var config = Config.newBuilder()
                .setRaw( setting ).build();

        var read = READER.read( config );
        assertThat( read ).hasSize( 3 );
        assertEquals( read.get( "one" ), new ServerGroupName( "1" ) );
        assertEquals( read.get( "two" ), new ServerGroupName( "2" ) );
        assertEquals( read.get( "three" ), new ServerGroupName( "3" ) );
    }
}
