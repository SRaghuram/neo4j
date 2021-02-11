#
# Copyright (c) "Neo4j"
# Neo4j Sweden AB [http://neo4j.com]
# This file is a commercial add-on to Neo4j Enterprise Edition.
#

<#
.SYNOPSIS
Invokes a command which manipulates a Neo4j Server e.g Start, Stop, Install and Uninstall

.DESCRIPTION
Invokes a command which manipulates a Neo4j Server e.g Start, Stop, Install and Uninstall.  

Invoke this function with a blank or missing command to list available commands

.PARAMETER Command
A string of the command to run.  Pass a blank string for the help text

.EXAMPLE
Invoke-Neo4j

Outputs the available commands

.EXAMPLE
Invoke-Neo4j status -Verbose

Gets the status of the Neo4j Windows Service and outputs verbose information to the console.

.OUTPUTS
System.Int32
0 = Success
non-zero = an error occured

.NOTES
Only supported on version 4.x Neo4j Community and Enterprise Edition databases

#>
function Invoke-Neo4j
{
  [CmdletBinding(SupportsShouldProcess = $false,ConfirmImpact = 'Low')]
  param(
    [Parameter(Mandatory = $false,ValueFromRemainingArguments = $true)]
    [Object[]]$CommandArgs = @()
  )

  begin
  {
  }

  process
  {
    try
    {
      return [int](Invoke-Neo4jUtility -Command 'server' -CommandArgs $CommandArgs -ErrorAction 'Stop')
    }
    catch {
      Write-Error $_
      return 1
    }
  }

  end
  {
  }
}
