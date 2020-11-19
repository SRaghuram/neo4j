/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import java.util.Arrays;

import static java.lang.String.format;

public class BranchAndVersion
{
    public static void validate( Repository repository, String owner, String branch )
    {
        boolean isDefaultOwner = repository.isDefaultOwner( owner );
        boolean isStandardBranch = repository.isStandardBranch( branch.toLowerCase() );

        if ( isDefaultOwner && !isStandardBranch )
        {
            throw new RuntimeException(
                    format(
                            "Branch of repository '%s' owned by '%s' must follow the pattern '%s' but was '%s'",
                            repository.projectName(),
                            owner,
                            repository.standardBranchPattern(),
                            branch
                    ) );
        }
        if ( !isDefaultOwner && isStandardBranch )
        {
            throw new RuntimeException(
                    format(
                            "Forks of repository '%s' owned by '%s' must not follow the pattern '%s' but was '%s",
                            repository.projectName(),
                            owner,
                            repository.standardBranchPattern(),
                            branch
                    ) );
        }
    }

    public static boolean isPersonalBranch( Repository repository, String owner )
    {
        return !repository.isDefaultOwner( owner );
    }

    static boolean isValidSeriesBranch( String version, String branch )
    {
        String seriesString = versionToSeries( version );
        return branch.equalsIgnoreCase( seriesString );
    }

    static boolean isValidDropBranch( String version, String branch )
    {
        return branch.equals( dropVersionToDropBranch( version ) );
    }

    public static void assertBranchEqualsSeries( String version, String branch )
    {
        if ( !isValidSeriesBranch( version, branch ) && !isValidDropBranch( version, branch ) )
        {
            throw new RuntimeException( format( "Branch (%s) and version (%s) must be same as series (%s) for non-personal branches",
                                                branch, version, versionToSeries( version ) ) );
        }
    }

    private static String dropVersionToDropBranch( String version )
    {
        if ( !version.contains( "-drop" ) )
        {
            throw new IllegalArgumentException( "expected drop version but got " + version );
        }
        return version.substring( 0, version.lastIndexOf( "." ) );
    }

    private static String versionToSeries( String version )
    {
        String[] split = version.split( "\\." );
        if ( split.length != 4 && split.length != 3 )
        {
            throw new IllegalArgumentException( "Branch could not be converted to Version, wrong size expected x.z.y(-dropxz.y) but got " + version );
        }
        if ( split.length == 3 || split[2].contains( "-drop" ) )
        {
            return split[0] + "." + split[1];
        }
        throw new RuntimeException(
                "Branch could not be converted to Version, wrong size " + Arrays.toString( split ) + " expected x.z.y(-dropxz.y) but got " + version );
    }
}
