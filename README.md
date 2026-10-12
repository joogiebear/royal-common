# royal-common

Shared code for the Royal plugins: pieces that several plugins would otherwise each write and fix on their own.

It is a library, not a plugin. Do not put it in a server's `plugins` folder. Each plugin shades it into its own jar and relocates it, so two plugins built against different versions never clash.

## Using it in a plugin

Add the repository and the dependency. Use a release version (for example `2026.42.0`, see the releases page) as the version. The repository is the `maven` branch of this repo, so no account or token is needed.

```xml
<repositories>
    <repository>
        <id>royal-common</id>
        <url>https://raw.githubusercontent.com/joogiebear/royal-common/maven</url>
    </repository>
</repositories>

<dependency>
    <groupId>com.github.joogiebear</groupId>
    <artifactId>royal-common</artifactId>
    <version>VERSION</version>
</dependency>
```

Shade and relocate it. Replace `royalbank` with the plugin's own package name.

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-shade-plugin</artifactId>
    <version>3.6.2</version>
    <executions>
        <execution>
            <phase>package</phase>
            <goals>
                <goal>shade</goal>
            </goals>
            <configuration>
                <createDependencyReducedPom>false</createDependencyReducedPom>
                <relocations>
                    <relocation>
                        <pattern>com.mystipixel.royalcommon</pattern>
                        <shadedPattern>com.mystipixel.royalbank.libs.royalcommon</shadedPattern>
                    </relocation>
                </relocations>
            </configuration>
        </execution>
    </executions>
</plugin>
```

The library only needs the JDK and the Bukkit API at runtime. JDBC drivers and the connection pool stay the plugin's own (Paper's library loader in the plugins today).

## What's in it

`com.mystipixel.royalcommon.storage`, for plugins that let several servers share one MySQL database:

- `TxRunner`, `Tx`, `Dialect`: one transaction per unit of work, row locks in a stable order, retry on deadlock or busy database.
- `DbExecutor`, `MainThread`: the plugin's single ordered database thread and the hand-back to the main thread.
- `BusyGuard`: one in-flight action per player.
- `VersionPoller`: notices rows another server changed.
- `PlayerHandoff`: waits for the previous server's quit save before loading a player.
- `Lease`: one server at a time runs a network-wide job.
- `SchemaVersion`: migrations that are safe when servers start together.
- `ServerId`: the id each server uses in leases and hand-off rows.

All of them work on SQLite too, where there is only one server.

## Building

Needs JDK 25 (paper-api 26.2 is compiled for it). The output targets Java 21.

```sh
mvn -B verify
```

## Releases

Run the Release workflow from GitHub Actions. It tags `YEAR.WEEK.PATCH`, writes `CHANGELOG.md`, publishes the jar to the `maven` branch and creates a GitHub release.
