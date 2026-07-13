
## 5. Java Binding Release Status (Jan 2026)

**Current Status**: Partial Setup.

The Java binding (`sop4j`) is configured for release to Maven Central via the **New Central Portal (central.sonatype.com)**.

### Configuration Details
*   **GroupId**: `io.github.sharedcode` (Verified and claimed on the Portal).
*   **ArtifactId**: `sop4j`.
*   **POM Configuration**:
    *   Uses `central-publishing-maven-plugin` (version 0.7.0).
    *   Uses `maven-gpg-plugin` with `loopback` pinentry mode.
    *   The publishing plugin is moved to the `release` profile to avoid interfering with regular builds.

### Deployment Blocker
We encountered a **401 Unauthorized** / **Broken Pipe** error during the `mvn deploy` phase.
*   **Cause**: The credentials in `settings.xml` likely need to be a **Publisher Token** generated from the New Central Portal, rather than legacy user credentials.
*   **GPG Key Issue**: The GPG Public Key (`A336CECD28725B8B15A94F3B4C20BCE16578D686`) has been generated locally but **not yet uploaded** to the Central Portal.
*   **UI Issue**: We were unable to locate the "Add GPG Key" button in the Central Portal UI (the `account` page link provided in docs led to a page without the expected functionality or the menu was disabled).

### Next Steps for Completion
1.  **Locate GPG Key Upload**: Find the correct UI location or CLI method to upload the public key to [central.sonatype.com](https://central.sonatype.com).
2.  **Generate Token**: Generate a User Token from the Portal.
3.  **Update Settings**: Update `~/.m2/settings.xml` with the new token for server ID `ossrh`.
4.  **Deploy**: Run `mvn clean deploy -DperformRelease=true -P release`.

**Note**: The release logic in `pom.xml` is currently valid but dormant until these specific credential/setup steps are resolved.
