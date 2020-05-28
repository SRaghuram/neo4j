/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.configuration.ServerGroupName;

import java.util.Map;
import java.util.Objects;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.GroupSetting;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.collection.Pair;

import static com.neo4j.configuration.ServerGroupName.SERVER_GROUP_NAME;
import static java.util.stream.Collectors.toMap;

@ServiceProvider
public class LeadershipPriorityGroupSetting extends GroupSetting
{
    public static final LeadershipPriorityGroupSettingReader READER = new LeadershipPriorityGroupSettingReader();

    private static final String PREFIX = "causal_clustering.leadership_priority_group";

    @Description( "A list of group names where leadership should be prioritised. This does not guarantee leadership on these groups at all times, but" +
                  " the cluster will attempt to transfer leadership to these groups when possible." )
    private final Setting<ServerGroupName> leadership_priority_group = getBuilder( SERVER_GROUP_NAME, ServerGroupName.EMPTY ).build();

    public LeadershipPriorityGroupSetting( String databaseName )
    {
        super( databaseName );
    }

    // For service loading
    public LeadershipPriorityGroupSetting()
    {
        this( "" );
    }

    @Override
    public String getPrefix()
    {
        return PREFIX;
    }

    Setting<ServerGroupName> setting()
    {
        return leadership_priority_group;
    }

    public static class LeadershipPriorityGroupSettingReader
    {
        private LeadershipPriorityGroupSettingReader()
        {
        }

        /**
         * Reads prioritised groups from config.
         *
         * @return map containing database name as key and server group as value.
         */
        public Map<String,ServerGroupName> read( Config config )
        {
            return config.getGroups( LeadershipPriorityGroupSetting.class )
                    .entrySet()
                    .stream()
                    .map( entry -> Pair.of( entry.getKey(), config.get( entry.getValue().setting() ) ) )
                    .filter( pair -> !Objects.equals( pair.other(), ServerGroupName.EMPTY ) )
                    .collect( toMap( Pair::first, Pair::other ) );
        }
    }
}
