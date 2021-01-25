/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class BenchmarkTool
{
    private final Repository repository;
    private final String commit;
    private final String owner;
    private final String branch;

    /**
     * WARNING: Never call this explicitly.
     * No-params constructor is only used for JSON (de)serialization.
     */
    public BenchmarkTool()
    {
        this( Repository.MICRO_BENCH, "1", Repository.MICRO_BENCH.defaultOwner(), "1.1" );
    }

    /**
     * Class fields are used to construct github URIs in the following way:
     * <p>
     * https://github.com/{owner}/{repository.repositoryName}/commit/{commit}
     * <p>
     * E.g., this URI:
     * <p>
     * https://github.com/neo-technology/benchmarks/commit/830c3b526f057441bb01aec84a65b9a96d17fa20
     * <p>
     * Maps to:
     * <ul>
     * <li>repository.repositoryName = "benchmarks"</li>
     * <li>commit = "830c3b526f057441bb01aec84a65b9a96d17fa20"</li>
     * <li>owner = "neo-technology"</li>
     * </ul
     *
     * @param repository
     * @param commit
     * @param owner
     * @param branch
     */
    public BenchmarkTool( Repository repository, String commit, String owner, String branch )
    {
        this.repository = repository;
        this.commit = requireNonNull( commit );
        this.owner = owner;
        this.branch = branch;
        BranchAndVersion.validate( repository, owner, branch );
    }

    public String owner()
    {
        return owner;
    }

    public String branch()
    {
        return branch;
    }

    public String repositoryName()
    {
        return repository.repositoryName();
    }

    public String toolName()
    {
        return repository.projectName();
    }

    public String commit()
    {
        return commit;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        BenchmarkTool that = (BenchmarkTool) o;
        return repository == that.repository &&
               Objects.equals( commit, that.commit ) &&
               Objects.equals( owner, that.owner ) &&
               Objects.equals( branch, that.branch );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( repository, commit, owner, branch );
    }

    @Override
    public String toString()
    {
        return "BenchmarkTool{" +
               "name=" + repositoryName() +
               "toolName=" + toolName() +
               ", commit='" + commit + '\'' +
               ", owner='" + owner + '\'' +
               ", branch='" + branch + '\'' +
               '}';
    }
}
