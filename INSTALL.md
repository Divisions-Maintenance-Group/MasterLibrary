create a nuget.config subbing your info....

<?xml version="1.0" encoding="utf-8"?>
<configuration>
    <packageSources>
        <clear />
        <add key="github" value="https://nuget.pkg.github.com/Divisions-Maintenance-Group/index.json" />
    </packageSources>
</configuration>

dotnet nuget add source --username <githubusername> --password <Personal access token> --store-password-in-clear-text --name github "https://nuget.pkg.github.com/Divisions-Maintenance-Group/index.json"

then run
dotnet pack --configuration Release

followed by


dotnet nuget push "bin/Release/MasterLibrary.1.0.2.nupkg" --api-key <PAT_GOES_HERE> --source "github"