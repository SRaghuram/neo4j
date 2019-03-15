/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.schemastore;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.internal.schema.FulltextSchemaDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;

@PageCacheExtension
class GBPTreeSchemaStoreTest
{
    @Inject
    private TestDirectory directory;
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private PageCache pageCache;

    private final LabelSchemaDescriptor labelSchemaDescriptorSingleKey = SchemaDescriptor.forLabel( 1, 2 );
    private final LabelSchemaDescriptor labelSchemaDescriptorMultiKey = SchemaDescriptor.forLabel( 1, 2, 3 );
    private final FulltextSchemaDescriptor fulltextSchemaDescriptor = SchemaDescriptor.fulltext( EntityType.NODE, new int[]{1, 2}, new int[]{3, 4} );
    private final RelationTypeSchemaDescriptor relationTypeSchemaDescriptorSingleKey = SchemaDescriptor.forRelType( 1, 2 );
    private final RelationTypeSchemaDescriptor relationTypeSchemaDescriptorMultiKey = SchemaDescriptor.forRelType( 1, 2, 3 );
    private final List<SchemaDescriptor> schemaDescriptors = asList(
            labelSchemaDescriptorSingleKey, labelSchemaDescriptorMultiKey,
            fulltextSchemaDescriptor,
            relationTypeSchemaDescriptorSingleKey, relationTypeSchemaDescriptorMultiKey );
    private final IndexProviderDescriptor indexProviderDescriptor = new IndexProviderDescriptor( "Test-Provider-Key", "Test-Provider-Version" );

    @Test
    void shouldCreateAndLoadRules() throws IOException, MalformedSchemaRuleException, SchemaRuleNotFoundException
    {
        // given
        try ( GBPTreeSchemaStore store = new GBPTreeSchemaStore( pageCache, directory.file( "store" ), immediate(),
                new DefaultIdGeneratorFactory( fileSystem, immediate() ), SIMPLE_NAME_LOOKUP, false, PageCacheTracer.NULL, PageCursorTracer.NULL ) )
        {
            List<SchemaRule> schemaRules = createListOfSchemaRules( store );

            // when
            for ( SchemaRule schemaRule : schemaRules )
            {
                store.writeRule( schemaRule, PageCursorTracer.NULL );
            }

            // then
            assertEquals( new HashSet<>( schemaRules ), new HashSet<>( store.loadRules( PageCursorTracer.NULL ) ) );
            for ( SchemaRule schemaRule : schemaRules )
            {
                assertThat( store.loadRule( schemaRule.getId(), PageCursorTracer.NULL ) ).isEqualTo( schemaRule );
            }
        }
    }

    @Test
    void shouldDeleteRules() throws IOException, MalformedSchemaRuleException
    {
        // given
        try ( GBPTreeSchemaStore store = new GBPTreeSchemaStore( pageCache, directory.file( "store" ), immediate(),
                new DefaultIdGeneratorFactory( fileSystem, immediate() ), SIMPLE_NAME_LOOKUP, false, PageCacheTracer.NULL, PageCursorTracer.NULL ) )
        {
            List<SchemaRule> schemaRules = createListOfSchemaRules( store );
            for ( SchemaRule schemaRule : schemaRules )
            {
                store.writeRule( schemaRule, PageCursorTracer.NULL );
            }

            // when/then
            while ( !schemaRules.isEmpty() )
            {
                SchemaRule schemaRule = schemaRules.remove( 0 );
                store.deleteRule( schemaRule.getId(), PageCursorTracer.NULL );
                assertEquals( new HashSet<>( schemaRules ), new HashSet<>( store.loadRules( PageCursorTracer.NULL ) ) );
            }
            assertTrue( store.loadRules( PageCursorTracer.NULL ).isEmpty() );
        }
    }

    private List<SchemaRule> createListOfSchemaRules( GBPTreeSchemaStore store )
    {
        int nameTag = 0;
        List<SchemaRule> schemaRules = new ArrayList<>();
        for ( SchemaDescriptor schemaDescriptor : schemaDescriptors )
        {
            schemaRules.add( IndexPrototype.forSchema( schemaDescriptor, indexProviderDescriptor ).withName( "Some name " + nameTag++ ).materialise(
                    store.nextSchemaRuleId( PageCursorTracer.NULL ) ) );
            if ( schemaDescriptor.entityType() == EntityType.NODE )
            {
                schemaRules.add( IndexPrototype.uniqueForSchema( schemaDescriptor, indexProviderDescriptor ).withName( "Some name " + nameTag++ )
                        .materialise( store.nextSchemaRuleId( PageCursorTracer.NULL ) ) );
                schemaRules.add( ConstraintDescriptorFactory.uniqueForSchema( schemaDescriptor ).withName( "Some name " + nameTag++ )
                        .withId( store.nextSchemaRuleId( PageCursorTracer.NULL ) ) );
                schemaRules.add( ConstraintDescriptorFactory.uniqueForSchema( schemaDescriptor ).withName( "Some name " + nameTag++ )
                        .withOwnedIndexId( store.nextSchemaRuleId( PageCursorTracer.NULL ) ).withId( store.nextSchemaRuleId( PageCursorTracer.NULL ) ) );
                schemaRules.add( ConstraintDescriptorFactory.nodeKeyForSchema( schemaDescriptor ).withName( "Some name " + nameTag++ )
                        .withId( store.nextSchemaRuleId( PageCursorTracer.NULL ) ) );
            }
            if ( !schemaDescriptor.isFulltextSchemaDescriptor() )
            {
                schemaRules.add( ConstraintDescriptorFactory.existsForSchema( schemaDescriptor ).withName( "Some name " + nameTag++ )
                        .withId( store.nextSchemaRuleId( PageCursorTracer.NULL ) ) );
            }
        }
        return schemaRules;
    }

    private static TokenNameLookup SIMPLE_NAME_LOOKUP = new TokenNameLookup()
    {
        @Override
        public String labelGetName( int labelId )
        {
            return "Label" + labelId;
        }

        @Override
        public String relationshipTypeGetName( int relationshipTypeId )
        {
            return "RelType" + relationshipTypeId;
        }

        @Override
        public String propertyKeyGetName( int propertyKeyId )
        {
            return "property" + propertyKeyId;
        }
    };
}
