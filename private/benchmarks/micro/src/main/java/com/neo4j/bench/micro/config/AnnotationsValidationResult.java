package com.neo4j.bench.micro.config;

public class AnnotationsValidationResult
{
    private final boolean valid;
    private final String message;

    AnnotationsValidationResult( boolean valid, String message )
    {
        this.valid = valid;
        this.message = message;
    }

    public boolean isValid()
    {
        return valid;
    }

    public String message()
    {
        return message;
    }
}
