# Gonymizer
![GonymizerLogo.png](https://github.com/smithoss/gonymizer/blob/master/docs/images/gonymize_small.png?raw=true)

-----        

## Weird name, what does it do?
The Gonymizer project is a project that was completed at [SmithRx](https://www.smithrx.com) in hope to simplify the QA process. Gonymizer is 
written in Go lang and is meant to help database administrators and infrastructure folks easily anonymize production
database dumps before loading this data into a QA environment. Currently PostgreSQL 9.6+ is supported with support for
other databases welcome through pull requests.

We have built in support, and examples, for:
* Kubernetes CRONJOB scheduling
* AWS-Lambda Job scheduling
* AWS-S3 Storage (processing and loading)
* CRONJOB BASH scripts

Our API is easy one to follow and we hope others will join in by tying in Gonymizer into their development and staging
environments either directly using the CLI or using the API. We include in our documentation: example configurations,
best practices, Kubernetes CRONJOB examples, examples for AWS-Lambda, and other infrastructure tools. Please see the 
docs directory in this application to see a full how-to guide and where to get started.

## Abbreviations and Definitions:

- **HIPAA**: Health Insurance Portability and Accountability Act of 1996
- **PCI DSS**: Payment Card Industry Data Security Standard 
- **PHI**: Protected Health Information
- **PII**: Personally identifiable information

In this document/codebase, we use them interchangeably.


## Getting Started
If you are a seasoned Go veteran or already have an environment which contains Go>= 1.11 then you can skip to 
the next section.

### OSX

Gonymizer requires that one has complete install of Go >= 1.11. To install Go on OSX you can run the following:
```
brew install go
```

Once this is complete we will need to make sure our Go paths are set correctly in our BASH profile. **NOTE**: You may 
need to change the directories below to match your setup.
```
echo "
export GOPATH=~/go
export GOROOT=/usr/local/Cellar/go/1.11.2/libexec
export GO111MODULE=on
" >> ~/.profile
```

It is recommended to put all Go source code under ~/go. Once this is complete we can attempt to build the application:

```
cd ~/go/src/github.com/smithoss/gonymizer/scripts
./build.sh
```

The build script will build two binaries. One for MacOS on the amd64 architecture as well as a Linux amd64 binary. These
binaries are stored under the Gonymizer/bin directory. Now that we have a built binary we can attempt to download a 
map file using our JSON configuration:

```
./gonymizer-darwin -c ~/conf/map-file.json map
```

## Debian 9.x
Use the following steps to get up and going using Debian 9.x (Ubuntu 18.3 is similar)
1. Install Golang and Git
```
sudo apt-get install go git
```

2. Add go path to profile 
```
echo "
export GOPATH=~/go
export GO111MODULE=on
" >> ~/.bashrc

```

3. Git checkout
```
mkdir -p ~/go/src/github.com/smithoss/
cd ~/go/src/github.com/smithoss/
git clone https://github.com/smithoss/Gonymizer.git gonymizer
```

4. Build the project
```
cd gonymizer/scripts
bash build.sh
```

5. Run the binary
```
cd ../bin
./gonymizer-linux --help
```

## Configuration

Gonymizer has many different configuration settings that can be enabled or disabled using the command line options.
It is recommended that one run `gonymizer --help` or `gonymizer CMD --help` where CMD is one of the commands to see
 which options are available at any given time.

Below we give examples of both the CLI configuration as well as examples on how to create your map file.

### CLI Configuration
Gonymizer was built using the Cobra + Viper Golang libraries to allow for easy configuration however you like it. We 
recommend using a JSON, YAML, or TOML file to configure Gonymizer. Below we will go over an example configuration for
running Gonymizer.

```
{
  "comment":             "Test database used for Go tests",

  "log-level":           "DEBUG",
  "log-format":          "text",
  "database":            "pii_localtest",
  "host":                "localhost",
  "port":                5432,
  "username":            "bob",
  "password":            "Bob's_Password",
  "disable-ssl":         true,
  "dump-file":           "/tmp/dump-db.sql",
  "map-file":            "/tmp/db-map.skeleton.json",
  "exclude-table":       ["upload_history", "download_history"],
  "exclude-table-data":  ["accounts"],
  "schema":              ["public", "company"],
  "exclude-schema":      ["pg_toast", "pg_catagory"],
  "schema-prefix":       "company_"

}
```

`comment`: is used to leave for comments for the reader and is not used by the application.

`log-level`: is the level the application uses to know what should be displayed to the screen. Choices are: FATAL, 
ERROR, WARN, INFO, DEBUG. We use the logrus Golang library for logging so please read the documentation 
[here](https://github.com/sirupsen/logrus) for more information.

`database`: is the master database with PHI and PII that will be used for dumping a SQL dump file from.

`host`: is the hostname for the master database with PHI and PII that will be used for dumping a SQL dump file from.

`port`: is the host port that will be used to connect to the master database with PHI and PII.

`username`: is the username that will be used to connect to the master database with PHI and PII.

`password`: is the password that will be used to connect to the master database with PHI and PII.

`disable-ssl`: is the master database with PHI and PII that will be used for dumping a SQL dump file from.

`dump-file`: is where Gonymizer will store the SQL statements from the `dump` command.

`map-file`: is the file that gonymizer uses to map out which columns need to be anonymized and how. When using the 
`map` command in conjunction with `--map-file`, or in the configuration above, a file is named similarly to the 
`map-file`, but with `skeleton` in the name instead. More on this below in the map section.

`exclude-table`: is list of tables that are not to be included during the pg_dump step of the extraction process. 
This allows us to only focus on tables that are needed for our base application to work. Using this option minimizes 
the size of our dump file and in return decreases the amount of time needed for dumping, processing, and 
reloading. This option operates in the same fashion as pg_dump's `--exclude-table` option.

`exclude-table-data`: allows you to create a list of tables we would like to include in the pg_dump process but do not 
want to include any of the data (table schema only). The usage and advantages are the same as the `exclude-table` 
feature explained above and is identical to pg_dump's `--exclude-table-data` option.

`schema`: is a list of schemas the Gonymizer should dump from the master database. This option must be in the form
of a list if you are using the configuration methods mentioned above.

`exclude-schemas`: is a list of system level schemas that Gonymizer should ignore when adding CREATE SCHEMA statements 
to the dump file. These schemas may still be included in the `--schema` option, for example the `public` schema.

`schema-prefix`: is the prefix used for a schema environment where there is a prefix that matches other schemas. This 
is same as a sharded architecture design which is outside the scope of this article and it is recommended to read
[here](https://en.wikipedia.org/wiki/Shard_(database_architecture)) if you are unfamiliar with this design paradigm. 
For example: *[company_1, company2, company_..., company_n-1, company_n]* would be 
`--schema-prefix=company_ --schemas=company`

*NOTE:* Some arguments are not included here. It is recommended to use `gonymizer --help` and 
`gonymizer [COMMAND] --help` for more information and configuration options.

### Map File Configuration
Once one has created a skeleton map file it is recommended to create a new *true* map file which will be used to let 
gonymizer know which columns need to be anonymized in the database and which columns do not. There are two methods in
which gonymizer map files work (inclusive and exclusive).

**NOTE:** Currently SmithRx is using an *exclusive dump file* which can be found under `map_files/prod_map.json` 

#### Available Fakers and Scramblers
Below is a list of fake data creators and scramblers. This table may not be up to date so please make sure to check 
`processor.go` for a full list.

| Processor Name | Use |
| -------------- |:----|
| AlphaNumbericScrambler | Scrambles strings. If a number is in the string it will replace it with another random number
| FakeStreetAddress | Used to replace a real US address with a fake one
| FakeCity | Used to replace a city column
| FakeEmailAddress | Used to replace e-mail with a fake one
| FakeFirstName | Used to replace a person's first name with a fake first name (non-gender specific)
| FakeLastName | Used to replace a person's last name with a fake last name
| FakePhoneNumber | Used to replace a person's phone number with fake phone number
| FakeState | Used to replace a state (full state name, non-abbreviated)
| FakeUsername | Used to replace a username with a fake one
| FakeZip | Used to replace a real zip code with another zip code
| Identity | Used to notify Gonymizer **not** to anonymize the column (same as leaving the column out of the map file)
| RandomDate | Randomizes Day and Month, but keeps year the same (HIPAA only requires month and day be changed)
| RandomUUID | Randomizes a UUID string, but keep a mapping of the old UUID and map it to the new UUID. If the old is found elsewhere in the database the new UUID will be used instead of creating another one. Useful for UUID primary key mapping (relationships).
| ScrubString | Replaces a string with \*'s. Useful for password hashes.

#### Inclusive Map Files
An *inclusive* map file is a map file which includes every column in every table that is contained in a list of schemas 
that is configurable by using the `--schemas` option. If you are using a sharded/group configuration only one copy of 
the column will be added to the file. An example map file can be found in `map_files/example_db_map.json`.

Once there is an up to date skeleton file one can then walk through the file and modify the "Processors"."Name" field
for any column that needs to be anonymized. This can be done by simply replacing the "Identity" processor with one 
listed in the table above. For example to pick a fake first name for a column labeled `first_name` one would add the 
`FakeFirstName` to the "Processors"."Name" field like so:

```
{
    "TableSchema": "public",
    "TableName": "users",
    "ColumnName": "first_name",
    "DataType": "character varying",
    "ParentSchema": "",
    "ParentTable": "",
    "ParentColumn": "",
    "OrdinalPosition": 6,
    "IsNullable": false,
    "Processors": [
        {
            "Name": "FakeFirstName",
            "Max": 0,
            "Min": 0,
            "Variance": 0,
            "Comment": ""
        }
    ],
    "Comment": ""
}
```

#### Exclusive Map Files
An **exclusive** map file is a map file that contains only the columns that need to be anonymized. This is the only 
difference from the **inclusive** map file method and should make map files smaller and simpler to navigate since they 
will not contain any columns using the "Identity" processor. **It is assumed that all columns that are not listed in 
the map file are considered to be OK to add to the dump file WITHOUT any scrambling or anonymization.** This means that 
the user must add column definitions for every schema change that requires anonymization.

**Pro Tip:** An east way to handle schema changes is to run the `map` command to create a new map file and copy/paste 
the new columns into your map file while adding the proper processors at the same time.

#### Relationship Mapping
Relationship mapping allows the user to define columns that should remain congruent during the processing/anonymization 
step. For example if a user is identified by a unique UUID that is used across multiple tables in the database one may 
select the `RandomUUID` processor which keeps a global variable Go lang string map of `OLD-UUID => NEW-UUID`. The 
global map variable can be found in the processor.go file and can also be stored to disk for back-tracing values to 
debug the application. The only way to enable this type of logging is to edit the generator.go file and add the 
function call the *writeDebugMap* function. Adding this to your run-time is outside of the scope of this documentation 
and it is recommended to **NEVER** use this option when working with real PHI and PII data. If this file is compromised 
and stolen, an attacker will gain full access of the mapping of `(PHI, PII) => (Non-PHI, Non-PII)`.

Currently we only allow for global mapping of the following processors (more may be added later):
* AlphaNumericScrambler
* RandomUUID

They can be found in the processor.go file:
```
var UUIDMap = map[uuid.UUID]uuid.UUID{}
var AlphaNumericMap = map[string]map[string]string{}
```

There are plans to add more globally aware processors in the future, but at this time only 2 are available.

To map a relationship one can do this quite easily by notifying Gonymizer that there is a parent table and column that 
exist that the column should be mapped to. Below is an example where we identify the parent schema, table, and column:

```
{
    "TableSchema": "public",
    "TableName": "credit_scores",
    "ColumnName": "ssn",
    "DataType": "integer",
    "ParentSchema": "public",
    "ParentTable": "user",
    "ParentColumn": "ssn",
    "OrdinalPosition": 6,
    "IsNullable": false,
    "Processors": [
        {
            "Name": "AlphaNumericScrambler",
            "Max": 0,
            "Min": 0,
            "Variance": 0,
            "Comment": ""
        }
    ]
    "Comment": ""
},
```

In the example above we are mapping the social security number (SSN) from the `credit_scores` table to the `users` 
table by simply notifying gonymizer that there exists a map for ssn that is tied to the `users.ssn` table and column. 
Gonymizer will see this and look the value up in the global **AlphaNumericMap** variable mentioned earlier. If the 
original SSN key does not exist in the map the Gonymizer will automatically scramble the SSN and add an entry in the 
 map such that: 
 
 `map["old SSN"]: "new value (new SSN)"`
 
Every time gonymizer checks a value in the SSN column it will look up this value and replace it with the previously 
anonymized SSN. This allows us to map keys between tables.

*Note:* Multiple tables can link back to the user table by simply adding the schema, table, and column names to the 
parent fields in the map file for the specified column.

#### Grouping and Schema Prefix Matching (sharding)
Sharding is a type of database partitioning that separates very large databases the into smaller, faster, more easily 
managed parts called data shards. The word shard means a small part of a whole. Explanation is outside the scope of 
this READ.me and more information can be found at this 
[Wikipedia article](https://en.wikipedia.org/wiki/Shard_(database_architecture\)).

**NOTE:** When working with a database that contains many schemas matching the schema-prefix (shards), one will need to 
make sure that all tables and columns are **identical** across each schema. Manging the DDL for each schema is outside 
the scope of Gonymizer project and should be done by external database administration tools.

## Running Gonymizer

### TL;DR Steps to anonymization (that's a word right?)

1. Create a map file: `gonymizer -c config/prod-conf.json map`
2. Edit dump file to define which columns need to be anonymized.
2. Create a PII encumbered dump file: `gonymizer -c config/prod-conf.json dump`
3. Use the Process command to anonymize the PII dump file: `gonymizer -c config/prod-conf.json process`
4. Use the Load command to load the anonymized database file into the database `gonymizer -c config/staging.json load`


### Detailed Steps

- Step 1: Generate a Map Skeleton (should only need to use the first time or during schema changes)

    This will generate a new skeleton (defined, but empty) config file from scratch:

        ./gonymizer -c config/prod-conf.json map

    If you already have a map file and just need to due to migrations, schema changes, etc (2nd -> nth runs) change
    the path to the real map file. The map command will NOT overwrite your map file, instead it will create a new 
    file with _"skeleton"_ in the name. This will also append new columns to the bottom:

        ./gonymizer -c config/prod-conf.json --map-file=db_mapper.prod_map.json map

    Will output a file named:
        
        db_mapper.prod_map.json.skeleton.json
        

- Step 2: Copy the newly created skeleton file to a new production map file
    
    **Pro Tip:** It is recommended to leave OUT column definitions from your map file that are to be skipped by the 
    gonymizer. This is to keep the map file simple and clean. The gonymizer will skip any column that is not in the 
    map file and continue on. The purpose of the skeleton file is to use it as a base line and to copy/paste your 
    anonymized columns from the skeleton file into your true map file. This map file will be used in the processing 
    step later. See Map Configuration above for more information.
    
        mv db_mapper.prod_map.json.skeleton.json db_mapper.prod_map.json

    Edit every field (removing unneeded columns if going Pro Tip route).  Add processors or Min/Max as necessary.

- Step 3: Generate PHI & PII-encumbered dumpfile

    **CAUTION!!** This dump file will contain PII!  Only do this on secure machines with encrypted block devices only!

        ./gonymizer -c config/prod-config.json dump --dump-file=dump-pii.sql

- Step 4: Generate altered data using the dumpfile built in step 3

    If you've correctly configured db_mapper.j

        ./gonymizer -c config/prod-conf.json --map-file=db_mapper.prod_nap.json\
         --dump-file=dump-pii.sql --s3-file-path=s3://my-bucket-name.s3.us-west-2.amazonaws.com/db-dump-processed.sql process

- Step 5. Use the Load command to load the data into the database to verify that the data is correctly scrambled

    The processed SQL file can simply be imported using PSQL.
    
        ./gonymizer -c config/staging-conf.json --load-file=s3://my-bucket-name.s3.us-west-2.amazonaws.com/db-dump-processed.sql load
        

## Notices and License

Please make sure to read our license agreement here []. We may state throughout our documentation that we are using this 
application to anonymize data for HIPAA requirements, but this is in our own environment and we give NO guarantee this
will be the same for other's uses. Considering everyone's data set is completely different and the configuration of 
this application is very involved we cannot guarantee that this application will guarantee any compliance of any type.
This is the application users responsibility to verify with council that the dataset that is processed by the 
application is indeed HIPAA/PCI/PHI/PII compliant.

**THERE IS ABSOLUTELY NO GUARANTEE THAT USING THIS SOFTWARE WILL COMPLETE A CORRECT ANONYMIZATION OF YOUR DATA SET
FOR COMPLIANCE PURPOSES. PLEASE SEE LICENSE.txt FOR MORE INFORMATION.**

