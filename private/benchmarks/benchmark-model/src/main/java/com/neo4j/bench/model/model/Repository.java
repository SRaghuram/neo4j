/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import java.util.Arrays;
import java.util.regex.Pattern;

import static java.lang.String.format;

public enum Repository
{
    // -----------------------
    // --- benchmark tools ---
    // -----------------------
    MICRO_BENCH( "micro",
                 "neo4j",
                 "neo-technology",
                 "^\\d\\.\\d$",
                 "^\\d{1,2}\\.\\d{1,2}\\.\\d{1,2}$" ),
    MACRO_BENCH( "macro",
                 "neo4j",
                 "neo-technology",
                 "^\\d\\.\\d$",
                 "^\\d{1,2}\\.\\d{1,2}\\.\\d{1,2}$" ),
    LDBC_BENCH( "ldbc",
                "neo4j",
                "neo-technology",
                "^\\d\\.\\d$",
                "^\\d{1,2}\\.\\d{1,2}\\.\\d{1,2}$" ),
    IMPORT_BENCH( "import-benchmarks",
                  "neo4j",
                  "neo-technology",
                  "^\\d\\.\\d$",
                  "^\\d{1,2}\\.\\d{1,2}\\.\\d{1,2}$" ),
    CAPS_BENCH( "caps-continuous-benchmarks",
                "caps-continuous-benchmarks",
                "neo-technology",
                "^master$",
                "^\\d{1,2}\\.\\d{1,2}\\.\\d{1,2}$" ),
    MORPHEUS_BENCH( "morpheus-integration",
                    "morpheus-integration",
                    "neo-technology",
                    "^master$",
                    "^\\d{1,2}\\.\\d{1,2}\\.\\d{1,2}$" ),
    ALGOS_JMH( "Graph Algorithms",
               "graph-analytics",
               "neo-technology",
               "^(master)|(\\d{1,2}\\.\\d{1,2})$",
               "^\\d{1,2}\\.\\d{1,2}\\.\\d{1,2}$" ),
    QUALITY_TASK( "Quality Task",
                  "quality-tasks",
                  "neo-technology",
                  "^master$",
                  "^\\d{1,2}\\.\\d{1,2}$" ),

    // -----------------------
    // --- target systems ----
    // -----------------------

    NEO4J( "neo4j",
           "neo-technology",
           "neo4j",
           "^\\d\\.\\d$",
           "^\\d{1,2}\\.\\d{1,2}\\.\\d{1,2}$" ),
    CAPS( "cypher-for-apache-spark",
          "cypher-for-apache-spark",
          "opencypher",
          "^master$",
          "^\\d{1,2}\\.\\d{1,2}\\.\\d{1,2}$" ),
    ALGOS( "Graph Algorithms",
           "graph-analytics",
           "neo4j",
           "^(master)|(\\d{1,2}\\.\\d{1,2})$",
           "^\\d{1,2}\\.\\d{1,2}\\.\\d{1,2}$" );

    public static Repository forName( String projectName )
    {
        return Arrays.stream( Repository.values() )
                     .filter( repository -> repository.projectName().equals( projectName ) )
                     .findFirst()
                     .orElseThrow( () -> new RuntimeException( "Invalid project repository: " + projectName ) );
    }

    private final String projectName;
    private final String repositoryName;
    private final String defaultOwner;
    private final Pattern standardBranch;
    private final Pattern validVersion;

    Repository( String projectName, String repositoryName, String defaultOwner, String standardBranchRegex, String validVersionRegex )
    {
        this.projectName = projectName;
        this.repositoryName = repositoryName;
        this.defaultOwner = defaultOwner;
        this.standardBranch = Pattern.compile( standardBranchRegex );
        this.validVersion = Pattern.compile( validVersionRegex );
    }

    public String projectName()
    {
        return projectName;
    }

    public String repositoryName()
    {
        return repositoryName;
    }

    public String defaultOwner()
    {
        return defaultOwner;
    }

    public String standardBranchPattern()
    {
        return standardBranch.toString();
    }

    public boolean isDefaultOwner( String owner )
    {
        return defaultOwner.equalsIgnoreCase( owner );
    }

    public boolean isStandardBranch( String branch )
    {
        return standardBranch.matcher( branch ).matches();
    }

    public boolean isValidVersion( String version )
    {
        return validVersion.matcher( version ).matches();
    }

    public void assertValidVersion( String version )
    {
        if ( !isValidVersion( version ) )
        {
            throw new RuntimeException( format( "Invalid version: '%s'", version ) );
        }
    }
}
