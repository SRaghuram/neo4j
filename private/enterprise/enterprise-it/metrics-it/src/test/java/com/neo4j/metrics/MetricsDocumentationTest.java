/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics;

import com.neo4j.metrics.source.Metrics;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Collection;

import org.neo4j.annotations.documented.Documented;
import org.neo4j.internal.helpers.ArrayUtil;
import org.neo4j.service.Services;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MetricsDocumentationTest
{

    @Test
    void validateMetricsClassesDocumented()
    {
        final Collection<Metrics> allMetrics = Services.loadAll( Metrics.class );
        assertThat( allMetrics ).isNotEmpty();

        for ( Metrics metricsClass : allMetrics )
        {
            Class<? extends Metrics> clazz = metricsClass.getClass();
            assertTrue( clazz.isAnnotationPresent( Documented.class ) );

            Field[] fields = clazz.getDeclaredFields();
            Class superclass = clazz.getSuperclass();
            while ( superclass != null )
            {
                fields = ArrayUtil.concat( fields, superclass.getDeclaredFields() );
                superclass = superclass.getSuperclass();
            }

            boolean foundAnnotatedField = false;
            for ( Field declaredField : fields )
            {
                if ( declaredField.isAnnotationPresent( Documented.class ) )
                {
                    foundAnnotatedField = true;
                    break;
                }
            }
            assertTrue( foundAnnotatedField, "Metrics classes must expose (and document) at least one metric: " + clazz.getName() );
        }
    }
}
