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
import org.neo4j.configuration.SettingImpl;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.service.Services;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SettingClassesValidationIT
{
    private static final Pattern VALID_SETTINGS_NAME = Pattern.compile( "^[a-z0-9_]+$" );

    @Test
    void validateInternalSettingsAndNaming() throws IllegalAccessException
    {
        for ( SettingsDeclaration settingsDeclaration : Services.loadAll( SettingsDeclaration.class ) )
        {
            validateField( settingsDeclaration );
        }
        for ( GroupSetting groupSetting : Services.loadAll( GroupSetting.class ) )
        {
            validateField( groupSetting );
        }
    }

    @Test
    void validateDefaultValuesOfSettingsAreParsable()
    {
        Services.loadAll( SettingsDeclaration.class ).stream().map( Object::getClass ).forEach( SettingClassesValidationIT::validateParsingOfField );
    }

    private static void validateField( Object settingsDeclaration ) throws IllegalAccessException
    {
        Class<?> settingsDeclarationClass = settingsDeclaration.getClass();
        boolean publicApi = isPublicApi( settingsDeclarationClass );

        for ( Field declaredField : settingsDeclarationClass.getDeclaredFields() )
        {
            if ( declaredField.getType().isAssignableFrom( Setting.class ) )
            {
                Setting<?> setting = (Setting<?>) declaredField.get( settingsDeclaration );

                // Java API name should not be camel case
                String settingName = settingsDeclarationClass.getName() + "." + declaredField.getName();
                assertTrue( VALID_SETTINGS_NAME.matcher( declaredField.getName() ).matches(),
                        "Setting name " + settingName + " does not follow naming convention" );

                // Config parameter should not contain hyphen
                if ( setting != null )
                {
                    assertFalse( setting.name().contains( "-" ), "The config parameter " + setting.name() + " contains a '-' which is not allowed" );
                }

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

    private static void validateParsingOfField( Class<?> settingsDeclarationClass )
    {
        for ( Field declaredField : settingsDeclarationClass.getDeclaredFields() )
        {
            if ( declaredField.getType().isAssignableFrom( Setting.class ) )
            {
                try
                {
                    // Check that the SettingValueParser can parse the default value.
                    SettingImpl setting = (SettingImpl) declaredField.get( null );
                    String value = setting.valueToString( setting.defaultValue() );
                    // ...but skip checking the settings that doesn't have a default value (valueToString converts that to 'No Value').
                    if ( !value.equals( "No Value" ) )
                    {
                        assertDoesNotThrow( () -> setting.parse( value ) );
                        assertEquals( setting.parse( value ), setting.defaultValue() );
                    }
                }
                catch ( IllegalAccessException e )
                {
                    throw new RuntimeException( e );
                }
            }
        }
    }
}
