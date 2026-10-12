# royal-common

Shared code for the Royal plugins: pieces that several plugins would otherwise each write and fix on their own.

It is a library, not a plugin. Do not put it in a server's `plugins` folder. Each plugin shades it into its own jar and relocates it, so two plugins built against different versions never clash.

## Using it in a plugin

Add the JitPack repository and the dependency. Use a release tag (for example `2026.41.0`) as the version.

```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>

<dependency>
    <groupId>com.github.joogiebear</groupId>
    <artifactId>royal-common</artifactId>
    <version>TAG</version>
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

## Building

Needs JDK 25 (paper-api 26.2 is compiled for it). The output targets Java 21.

```sh
mvn -B verify
```

## Releases

Run the Release workflow from GitHub Actions. It tags `YEAR.WEEK.PATCH`, writes `CHANGELOG.md` and creates a GitHub release. JitPack builds the tag the first time a plugin asks for it.
