/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.enterprise.id;

import com.neo4j.kernel.impl.enterprise.configuration.CommercialEditionSettings;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.id.configuration.IdTypeConfiguration;
import org.neo4j.internal.id.configuration.IdTypeConfigurationProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith( Parameterized.class )
public class CommercialIdTypeConfigurationProviderTest
{
    private final IdType reusableType;

    @Parameterized.Parameters
    public static List<IdType> data()
    {
        return Arrays.asList( IdType.PROPERTY,
                IdType.STRING_BLOCK,
                IdType.ARRAY_BLOCK,
                IdType.NODE,
                IdType.RELATIONSHIP,
                IdType.NODE_LABELS );
    }

    public CommercialIdTypeConfigurationProviderTest( IdType reusableType )
    {
        this.reusableType = reusableType;
    }

    @Test
    public void nonReusableTypeConfiguration()
    {
        IdTypeConfigurationProvider provider = createIdTypeProvider();
        IdTypeConfiguration typeConfiguration = provider.getIdTypeConfiguration( IdType.SCHEMA );
        assertFalse( "Schema record ids are not reusable.", typeConfiguration.allowAggressiveReuse() );
        assertEquals( "Schema record ids are not reusable.", 1024, typeConfiguration.getGrabSize() );
    }

    @Test
    public void reusableTypeConfiguration()
    {
        IdTypeConfigurationProvider provider = createIdTypeProvider();
        IdTypeConfiguration typeConfiguration = provider.getIdTypeConfiguration( reusableType );
        assertTrue( typeConfiguration.allowAggressiveReuse() );
        assertEquals( 50000, typeConfiguration.getGrabSize() );
    }

    private IdTypeConfigurationProvider createIdTypeProvider()
    {
        Map<String,String> params = MapUtil.stringMap( CommercialEditionSettings.idTypesToReuse.name(),
                IdType.NODE + "," + IdType.RELATIONSHIP );
        Config config = Config.defaults( params );
        return new CommercialIdTypeConfigurationProvider( config );
    }
}
