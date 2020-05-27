/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.configuration;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.regex.Pattern;

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.configuration.GroupSetting;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.service.Services;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SettingClassesValidationIT
{
    private static final Pattern VALID_SETTINGS_NAME = Pattern.compile( "^[a-z0-9_]+$" );

    @Test
    void validateInternalSettingsAndNaming()
    {
        Services.loadAll( SettingsDeclaration.class ).stream().map( Object::getClass ).forEach( SettingClassesValidationIT::validateField );
        Services.loadAll( GroupSetting.class ).stream().map( Object::getClass ).forEach( SettingClassesValidationIT::validateField );
    }

    private static void validateField( Class<?> settingsDeclarationClass )
    {
        boolean publicApi = isPublicApi( settingsDeclarationClass );

        for ( Field declaredField : settingsDeclarationClass.getDeclaredFields() )
        {
            if ( declaredField.getType().isAssignableFrom( Setting.class ) )
            {
                // Name should not have camel case
                String settingName = settingsDeclarationClass.getName() + "." + declaredField.getName();
                assertTrue( VALID_SETTINGS_NAME.matcher( declaredField.getName() ).matches(),
                        "Setting name " + settingName + " does not follow naming convention" );

                if ( publicApi )
                {
                    // Internal settings should not be part of public api
                    assertFalse( declaredField.isAnnotationPresent( Internal.class ),
                            "Setting " + settingName + " is part of public api but marked as @Internal." );
                }
            }
        }
    }

    private static boolean isPublicApi( Class<?> settingsDeclarationClass )
    {
        Class<?> clazz = settingsDeclarationClass;
        while ( clazz != null )
        {
            if ( clazz.isAnnotationPresent( PublicApi.class ) )
            {
                return true;
            }
            clazz = clazz.getSuperclass();
        }
        return false;
    }
}