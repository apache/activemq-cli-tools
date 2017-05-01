Apache ActiveMQ Command Line Tools
==================================

The ActiveMQ Command Line Tools project is home to several CLI based projects
useful for users of the ActiveMQ and ActiveMQ Artemis messaging brokers

## ActiveMQ KahaDB Export Tool

This tool can be used to export a KahaDB or MultiKahaDB store to Artemis XML format.  The resulting XML document can then be imported into an Artemis broker (version 2.0.0+) using the Artemis CLI tool.  See [Artemis CLI tool](https://activemq.apache.org/artemis/docs/2.0.0/tools.html)

### Usage
```
$ ./bin/export help
usage: export <command> [<args>]

The most commonly used export commands are:
    help      Display help information
    kahadb    Export a KahaDb store to Artemis XML
    mkahadb   Export a MultiKahaDb store to Artemis XML

See 'export help <command>' for more information on a specific command.
```
```
$ ./bin/export help kahadb
NAME
        export kahadb - Export a KahaDb store to Artemis XML

SYNOPSIS
        export kahadb [-c] [-f]
                [(--qp <queuePattern> | --queuePattern <queuePattern>)]
                (-s <source> | --source <source>) (-t <target> | --target <target>)
                [(--tp <topicPattern> | --topicPattern <topicPattern>)]

OPTIONS
        -c
            Compress output xml file using gzip

        -f
            Force XML output and overwrite existing file

        --qp <queuePattern>, --queuePattern <queuePattern>
            Queue Export Pattern

        -s <source>, --source <source>
            Data store directory location

        -t <target>, --target <target>
            Xml output file location

        --tp <topicPattern>, --topicPattern <topicPattern>
            Topic Export Pattern
```

### Examples:

Export entire store:

`./bin/export kahadb --source /some/directory/kahadb/ --target ~/some/directory/output.xml`

Export entire store and compress the resulting xml:

`./bin/export kahadb --source /some/directory/kahadb/ --target ~/some/directory/output.xml -c`

Export all topics but only queues matching pattern:

`./bin/export kahadb --qp test.queue.> --source /some/directory/kahadb/ --target ~/some/directory/output.xml`
