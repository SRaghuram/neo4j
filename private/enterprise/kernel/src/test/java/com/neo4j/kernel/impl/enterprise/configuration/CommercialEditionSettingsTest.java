/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.enterprise.configuration;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.internal.id.IdType;

import static com.neo4j.kernel.impl.enterprise.configuration.CommercialEditionSettings.idTypesToReuse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CommercialEditionSettingsTest
{
    @Test
    void idTypesToReuseAllowedValues()
    {
        for ( IdType type : IdType.values() )
        {
            if ( type == IdType.NODE || type == IdType.RELATIONSHIP )
            {
                assertIdTypesToReuseAllows( type );
            }
            else
            {
                assertIdTypesToReuseDisallows( type );
            }
        }

        assertIdTypesToReuseAllows( IdType.NODE, IdType.RELATIONSHIP );
        assertIdTypesToReuseAllows( IdType.RELATIONSHIP, IdType.NODE );

        assertIdTypesToReuseDisallows( IdType.NODE, IdType.RELATIONSHIP, IdType.RELATIONSHIP_GROUP );
        assertIdTypesToReuseDisallows( IdType.SCHEMA, IdType.NEOSTORE_BLOCK );
    }

    @Test
    void idTypesToReuseCaseInsensitive()
    {
        Config config1 = Config.defaults( idTypesToReuse, "node, relationship" );
        assertEquals( asList( IdType.NODE, IdType.RELATIONSHIP ), config1.get( idTypesToReuse ) );

        Config config2 = Config.defaults( idTypesToReuse, "rElAtIoNshiP, NoDe" );
        assertEquals( asList( IdType.RELATIONSHIP, IdType.NODE ), config2.get( idTypesToReuse ) );
    }

    private static void assertIdTypesToReuseAllows( IdType type, IdType... otherTypes )
    {
        Config config = configWithIdTypes( type, otherTypes );
        List<IdType> types = config.get( idTypesToReuse );
        assertEquals( asList( type, otherTypes ), types );
    }

    private static void assertIdTypesToReuseDisallows( IdType type, IdType... otherTypes )
    {
        assertThrows( IllegalArgumentException.class, () ->
        {
            Config config = configWithIdTypes( type, otherTypes );
            config.get( idTypesToReuse );
        } );
    }

    private static Config configWithIdTypes( IdType type, IdType... otherTypes )
    {
        return Config.defaults( idTypesToReuse, stringList( type, otherTypes ) );
    }

    @SafeVarargs
    private static <T> String stringList( T element, T... elements )
    {
        return StringUtils.join( asList( element, elements ), "," );
    }

    @SafeVarargs
    private static <T> List<T> asList( T element, T... elements )
    {
        List<T> list = new ArrayList<>();
        list.add( element );
        Collections.addAll( list, elements );
        return list;
    }
}
