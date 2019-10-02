/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.enterprise.builtinprocs;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.neo4j.kernel.configuration.Config;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings.dynamic_setting_whitelist;

public class SettingsWhitelist
{
    private final List<Pattern> settingsWhitelist;

    public SettingsWhitelist( Config config )
    {
        settingsWhitelist = refreshWhiteList( config.get( dynamic_setting_whitelist ) );
    }

    private List<Pattern> refreshWhiteList( String whiteList )
    {
        if ( StringUtils.isEmpty( whiteList ) )
        {
            return emptyList();
        }
        return Arrays.stream( whiteList.trim().split( "," ) )
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
