/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.commands;

import static java.util.stream.Collectors.toList;

import java.io.File;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.jayway.jsonpath.JsonPath;

import net.minidev.json.JSONArray;

public class BatchJobCommandParameters
{
    public static List<String> getBatchJobCommandParameters() throws Exception
    {
        return JsonPath.parse(
                new File( "../src/main/stack/aws-batch-formation.json" ))
                .limit( 1 )
                // in CloudFormation template find first JobDefinitionResource and extract command
                .read( "$.Resources.*[?(@.Type == 'AWS::Batch::JobDefinitionResource')].Properties.ContainerProperties.Command", JSONArray.class )
                .stream()
                .findFirst()
                .map( JSONArray.class::cast )
                .get()
                .stream()
                // filter out all non string command line arguments (like references to secrets in SecretManager)
                .filter( String.class::isInstance )
                .map( String.class::cast )
                // filter out all of command arguments which are not parameter references
                .filter( value -> value.startsWith( "Ref::" ) )
                // remove prefix to get parameter name
                .map( value -> StringUtils.substringAfter( value, "Ref::" ) )
                .collect( toList() );
    }
}
