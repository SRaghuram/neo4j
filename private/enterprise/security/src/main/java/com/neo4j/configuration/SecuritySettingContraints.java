/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.configuration;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.configuration.SettingConstraint;
import org.neo4j.graphdb.config.Configuration;

import static java.lang.String.format;

final class SecuritySettingContraints
{
    private SecuritySettingContraints()
    {
    }

    private static final String GROUP_DELIMITER = ";";
    private static final String KEY_VALUE_DELIMITER = "=";
    private static final String ROLE_DELIMITER = ",";

    private static final String KEY_GROUP = "\\s*('(.+)'|\"(.+)\"|(\\S)|(\\S.*\\S))\\s*";
    private static final String ROLE_GROUP = "\\s*([a-zA-Z0-9_]+)";
    private static final String ROLE_GROUPS = "(" + ROLE_GROUP + "(" + ROLE_DELIMITER + ROLE_GROUP + ")*)?";
    private static final String KEY_VALUE_GROUP = KEY_GROUP + KEY_VALUE_DELIMITER + ROLE_GROUPS;
    private static final Pattern keyValuePattern = Pattern.compile( KEY_VALUE_GROUP );

    static SettingConstraint<String> validateGroupMapping()
    {
        return new SettingConstraint<>()
        {
            @Override
            public void validate( String groupMapping, Configuration config )
            {
                // Accepted: [ldap-group=[role(,role)*]*;]*
                for ( String groupAndRoles : groupMapping.split( GROUP_DELIMITER ) )
                {
                    if ( !groupAndRoles.isEmpty() )
                    {
                        Matcher matcher = keyValuePattern.matcher( groupAndRoles );
                        if ( !matcher.find() )
                        {
                            throw new IllegalArgumentException( format( "'%s' could not be parsed", groupAndRoles ) );
                        }
                    }
                }
            }

            @Override
            public String getDescription()
            {
                return "must be semicolon separated list of key-value pairs or empty";
            }
        };
    }

    static SettingConstraint<String> validateUserTemplate()
    {
        return new SettingConstraint<>()
        {
            @Override
            public void validate( String template, Configuration config )
            {
                if ( !template.contains( "{0}" ) )
                {
                    throw new IllegalArgumentException( format( "'%s' must contain '{0}", template ) );
                }
            }

            @Override
            public String getDescription()
            {
                return "Must be a string containing '{0}' to understand where to insert the runtime authentication principal.";
            }
        };
    }

    static SettingConstraint<String> nonEmpty()
    {
        return new SettingConstraint<>()
        {
            @Override
            public void validate( String value, Configuration config )
            {
                if ( value.isEmpty() )
                {
                    throw new IllegalArgumentException( getDescription() );
                }
            }

            @Override
            public String getDescription()
            {
                return "Can not be empty";
            }
        };
    }

    static SettingConstraint<List<String>> nonEmptyList()
    {
        return new SettingConstraint<>()
        {
            @Override
            public void validate( List<String> value, Configuration config )
            {
                if ( value.isEmpty() )
                {
                    throw new IllegalArgumentException( getDescription() );
                }
            }

            @Override
            public String getDescription()
            {
                return "Can not be empty";
            }
        };
    }
}
