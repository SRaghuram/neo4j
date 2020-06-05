/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies;

import com.neo4j.causalclustering.routing.load_balancing.filters.Filter;
import org.junit.jupiter.api.Test;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class FilterConfigParserTest
{
    @Test
    void shouldThrowExceptionOnInvalidConfig()
    {
        String[] invalidConfigs = {
                "",
                ";",
                "(",
                ")",
                "()",
                ",",
                "\"",
                "\'",
                "groups",
                "min",
                "unknown",
                "unknown()",
                "unknown(something)",
                "min()",
                "min(2,5)",
                "groups()",
                "all(2)",
                "min(five)",
                "groups(group1_%)",
                "groups(group2_%)",
                "groups(group 2)",
                "%groups(group2)",
                "ta%gs(group2)",
                "ta-gs(group2)",
                "groups(group1),groups(group2)",
                "groups(group1);;groups(group2)",
                "groups(group1)+groups(group2)",
                "halt();groups(group)",
                "halt();halt()",
                "groups(group1);halt();groups(group2)",
                "groups(group1);groups(group2);halt();groups(group3)",
                "groups(group1) -> halt()",
                "halt() -> groups(group1)",
                "groups(group1) -> groups(group2) -> halt()",
                "groups(group1) -> halt() -> groups(group2)",
                "groups(group)->all()",
                "all()->all()",
                "groups(A)->all()->groups(B)",
        };

        // when
        for ( String invalidConfig : invalidConfigs )
        {
            try
            {
                Filter<ServerInfo> parsedFilter = FilterConfigParser.parse( invalidConfig );
                fail( format( "Config should have been invalid: '%s' but generated: %s", invalidConfig, parsedFilter ) );
            }
            catch ( InvalidFilterSpecification e )
            {
                // expected
            }
        }
    }

    @Test
    void shouldParseValidConfigs()
    {
        Object[][] validConfigs = {
                {
                        "min(2);",
                        FilterBuilder.filter().min( 2 )
                                .newRule().all() // implicit
                                .build()
                },
                {
                        "groups(5);",
                        FilterBuilder.filter().groups( "5" )
                                .newRule().all() // implicit
                                .build()
                },
                {
                        "all()",
                        FilterBuilder.filter().all().build()
                },
                {
                        "all() -> groups(5);",
                        FilterBuilder.filter().groups( "5" )
                                .newRule().all() // implicit
                                .build()
                },
                {
                        "all() -> groups(5);all()",
                        FilterBuilder.filter().groups( "5" )
                                .newRule().all()
                                .build()
                },
                {
                        "all() -> groups(A); all() -> groups(B); halt()",
                        FilterBuilder.filter().groups( "A" )
                                .newRule().groups( "B" )
                                .build()
                },
                {
                        "groups(groupA);",
                        FilterBuilder.filter().groups( "groupA" )
                                .newRule().all() // implicit
                                .build()
                },
                {
                        "groups(groupA,groupB); halt()",
                        FilterBuilder.filter().groups( "groupA", "groupB" ).build()
                },
                {
                        "groups ( groupA , groupB ); halt()",
                        FilterBuilder.filter().groups( "groupA", "groupB" ).build()
                },
                {
                        "groups(group1)->groups(group2); halt()",
                        FilterBuilder.filter().groups( "group1" ).groups( "group2" ).build()
                },
                {
                        "groups(group1)->groups(group2); halt();",
                        FilterBuilder.filter().groups( "group1" ).groups( "group2" ).build()
                },
                {
                        "groups(group-1)->groups(group-2); halt();",
                        FilterBuilder.filter().groups( "group-1" ).groups( "group-2" ).build()
                },
                {
                        "groups(group1)->groups(group2)->min(4); groups(group3,group4)->min(2); halt();",
                        FilterBuilder.filter().groups( "group1" ).groups( "group2" ).min( 4 )
                                .newRule().groups( "group3", "group4" ).min( 2 ).build()
                },
                {
                        "groups(group1,group2,group3,group4)->min(2); groups(group3,group4);",
                        FilterBuilder.filter().groups( "group1", "group2", "group3", "group4" ).min( 2 )
                                .newRule().groups( "group3", "group4" )
                                .newRule().all() // implicit
                                .build()
                }
        };

        // when
        for ( Object[] validConfig : validConfigs )
        {
            String config = (String) validConfig[0];
            Filter expectedFilter = (Filter) validConfig[1];

            try
            {
                Filter<ServerInfo> parsedFilter = FilterConfigParser.parse( config );
                assertEquals( expectedFilter, parsedFilter, format( "Config '%s' should generate expected filter", config ) );
            }
            catch ( InvalidFilterSpecification e )
            {
                fail( format( "Config should have been valid: '%s'", config ) );
            }
        }
    }
}
