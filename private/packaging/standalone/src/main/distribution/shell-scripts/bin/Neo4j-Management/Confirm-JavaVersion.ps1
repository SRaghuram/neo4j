# Copyright (c) 2002-2018 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
# This file is a commercial add-on to Neo4j Enterprise Edition.


<#
.SYNOPSIS
Confirms whether the specificed java executable is suitable for Neo4j

.DESCRIPTION
Confirms whether the specificed java executable is suitable for Neo4j

.PARAMETER Path
Full path to the Java executable, java.exe

.EXAMPLE
Get-JavaVersion -Path 'C:\Program Files\Java\jre1.8.0_71\bin\java.exe'

Retrieves the Java version for 'C:\Program Files\Java\jre1.8.0_71\bin\java.exe'.

.OUTPUTS
System.Boolean

.NOTES
This function is private to the powershell module

#>
Function Confirm-JavaVersion
{
  [cmdletBinding(SupportsShouldProcess=$false,ConfirmImpact='Low')]
  param (
    [Parameter(Mandatory=$true,ValueFromPipeline=$false)]
    [String]$Path
  )

  Begin {    
  }
  
  Process {
    $stdError = New-Neo4jTempFile -Prefix 'stderr'
    
    # Run Java with redirection
    $args = @('-version')
    Write-Verbose "Executing $Path $args"
    $result = Start-Process -FilePath $Path -ArgumentList $args -NoNewWindow -Wait -RedirectStandardError $stdError -PassThru

    # Check the output
    if ($result.ExitCode -ne 0) {
      Write-Verbose "Java returned exit code $($result.ExitCode)"
      Write-Warning "Unable to determine Java Version"
      return $true
    }    
    if (-not (Test-Path -Path $stdError)) {
      Write-Verbose "Java did not output version information"
      Write-Warning "Unable to determine Java Version"
      return $true
    }
    
    $javaHelpText = "* Please use Oracle(R) Java(TM) 8, OpenJDK(TM) or IBM J9 to run Neo4j Server.`n" +
                    "* Please see https://neo4j.com/docs/ for Neo4j installation instructions."

    # Read the contents of the redirected output
    $content = (Get-Content -Path $stdError) -join "`n`r"

    # Remove the temp file
    Remove-Item -Path $stdError -Force | Out-Null
    
    # Use a simple regular expression to extract the java version
    Write-Verbose "Java version response: $content"
    if ($matches -ne $null) { $matches.Clear() }  
    if ($content -match 'version \"(.+)\"') {
      $javaVersion = $matches[1]
      Write-Verbose "Java Version detected as $javaVersion"
    } else {
      Write-Verbose "Could not determing the Java Version"
      Write-Warning "Unable to determine Java Version"
      return $true
    }
    
    # Check for Java Version Compatibility
    # Anything less than Java 1.8 will block execution
    # Note - This text comparsion will fail for '1.10.xxx' due to how string based comparisons of numbers works.
    if ($javaVersion -lt '1.8') {
      Write-Warning "ERROR! Neo4j cannot be started using java version $($javaVersion)"      
      Write-Warning $javaHelpText
      return $false
    }
    
    # Check for Java Edition
    $regex = '(Java HotSpot\(TM\)|OpenJDK|IBM) (64-Bit Server|Server|Client|J9) VM'
    if (-not ($content -match $regex)) {
      Write-Warning "WARNING! You are using an unsupported Java runtime"      
      Write-Warning $javaHelpText
    }

    return $true
  }
  
  End {
  }
}
