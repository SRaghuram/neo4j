#
# Copyright (c) 2002-2018 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
# This file is a commercial add-on to Neo4j Enterprise Edition.
#


<#
.SYNOPSIS
Invokes a command which manages a Neo4j Server

.DESCRIPTION
Invokes a command which manages a Neo4j Server.

Invoke this function with a blank or missing command to list available commands

.PARAMETER CommandArgs
Remaining command line arguments are passed to the admin tool

.EXAMPLE
Invoke-Neo4jAdmin help

Prints the help text

.OUTPUTS
System.Int32
0 = Success
non-zero = an error occured

.NOTES
Only supported on version 3.x Neo4j Community and Enterprise Edition databases

#>
Function Invoke-Neo4jAdmin
{
  [cmdletBinding(SupportsShouldProcess=$false,ConfirmImpact='Low')]
  param (
    [parameter(Mandatory=$false,ValueFromRemainingArguments=$true)]
    [Object[]]$CommandArgs = @()
  )

  Begin
  {
  }

  Process
  {
    # The powershell command line interpeter converts comma delimited strings into a System.Object[] array
    # Search the CommandArgs array and convert anything that's System.Object[] back to a string type
    for($index = 0; $index -lt $CommandArgs.Length; $index++) {
      if ($CommandArgs[$index].GetType().ToString() -eq 'System.Object[]') {
        [string]$CommandArgs[$index] = $CommandArgs[$index] -join ','
      }
    }

    try
    {
      Return [int](Invoke-Neo4jUtility -Command 'admintool' -CommandArgs $CommandArgs -ErrorAction 'Stop')
    }
    catch {
      Write-Error $_
      Return 1
    }
  }

  End
  {
  }
}
