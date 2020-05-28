/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.enterprise.builtin;

import java.util.List;
import java.util.regex.Pattern;

import org.neo4j.configuration.Config;

import static com.neo4j.configuration.EnterpriseEditionSettings.dynamic_setting_whitelist;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public class SettingsWhitelist
{
    private final List<Pattern> settingsWhitelist;

    public SettingsWhitelist( Config config )
    {
        settingsWhitelist = refreshWhiteList( config.get( dynamic_setting_whitelist ) );
    }

    private List<Pattern> refreshWhiteList( List<String> whiteList )
    {
        if ( whiteList == null || whiteList.isEmpty() )
        {
            return emptyList();
        }
        return whiteList.stream()
                .map( s -> s.replace( "*", ".*" ) )
                .map( Pattern::compile )
                .collect( toList() );
    }

    boolean isWhiteListed( String settingName )
    {
        if ( settingsWhitelist != null )
        {
            return settingsWhitelist.stream().anyMatch( pattern -> pattern.matcher( settingName ).matches() );
        }
        return false;
    }
}
