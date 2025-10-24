# Reference
Except the jupyter notebook image and guide the rest of the images + guides are taken from https://github.com/IBM/presto-iceberg-lab
# Set up an Open Lakehouse

- A MinIO s3 Object Storage as the data storage component
- A Hive metastore server to keep track of table metadata, and a MySQL database to store this metadata
- A single-node Presto cluster as the SQL query engine
- Jupyter notebook 

This section is comprised of the following steps:

- [Set up an Open Lakehouse](#set-up-an-open-lakehouse)
  - [1. Build the minimal Hive metastore image](#1-build-the-minimal-hive-metastore-image)
  - [2. Spin up all containers](#2-spin-up-all-containers)
  - [3. Check that services have started](#3-check-that-services-have-started)
  - [4. Connect to Iceberg](#4-connect-to-iceberg)

## 1. Build the minimal Hive metastore image

In order to use Iceberg with Presto, we have to set up an underlying catalog. Recall that Iceberg is a table format rather than a catalog itself. The Iceberg table format manages *most* of its metadata in metadata files in the underlying storage (in this case MinIO s3 object storage) alongside the raw data files. A small amount of metadata, however, still requires the use of a meta-datastore, which when using Presto can be provided by Hive, Nessie, Glue, Hadoop, or via a REST server implementation. This "Iceberg catalog" is a central place to find the current location of the current metadata pointer for a table. We are using the Hive metastore in this case.

We'll build a minimal Hive metastore image from the Dockerfile included in this repo.

1. Open a terminal locally and run the following commands to build the Hive metastore image:

   ```sh
   cd conf
   docker compose build
   ```

   You will see console output while the image builds. The build may take several minutes to complete. While we wait, let's go over some of the configuration options in the `metastore-site.xml` file that will be passed to the metastore container on startup. The following properties are of particular interest to us:

   - `metastore.thrift.uris`: defines the endpoint of the metastore service that we're using. The hostname supplied corresponds to the `hostname` that is assigned to our metastore container in `docker-compose.yml` (see below section). The port `9083` is the default port for the Hive metastore service. As with any URI, `thrift://` is the protocol by which communication takes place.
   - `javax.jdo.option.ConnectionURL`: defines the URL of the underlying database that supports the metastore service (this is different from the underlying source for our table data, which is MinIO/s3). The hostname is again the hostname of the MySQL database container (again defined in `docker-compose.yml`), and the port is the default MySQL port. We also give a path to a specific database, `metastore_db`, that will act the storage for our metastore
   - `javax.jdo.option.ConnectionUserName` and `javax.jdo.option.ConnectionPassword`: the username and password required to access the underlying MySQL database
   - `fs.s3a.endpoint`: the endpoint that provides the storage for the table data (not the metastore data). The hostname and port given follow the same convention as those mentioned earlier.
   - `fs.s3a.access.key` and `fs.s3a.secret.key`: the username and password required for the metastore to access the underlying table data
   - `fs.s3a.path.style.access`: we set this property to true to indicate that requests will be sent to, for example, `s3.example.com/bucket` instead of `bucket.s3.example.com`

   Once the image has been built, we can move to step 2.

2. Check that the `hive-metastore` image has been successfully created:

   ```sh
   docker image list
   ```

   You should see a `conf-hive-metastore` image in your list of images, similar to this:

   ```sh
    REPOSITORY                       TAG                           IMAGE ID         CREATED          SIZE
    conf-hive-metastore              latest                        28377ad2303e     2 minutes ago    1.14GB
   ```

   This means that the image has been created with the tag `latest`.

## 2. Spin up all containers

Bring up the necessary containers with the following command:

```sh
docker compose up -d
```

This command may also take quite awhile to run, as docker has to pull an image for each container. While we wait for startup to complete, let's go through some of the `docker-compose.yml` file to see how our open lakehouse is set up. This file defines everything about our multi-container application.

First, we define a network: `presto_network`. Each of our containers will communicate across this network.

The next section is the `service` section, which is the bulk of the file. The first service we define is that of the Presto cluster, which we have named `presto-coordinator`. We provide a human-readable `container_name` (also "presto-coordinator") and the Docker `image` that we want this service to be based on, which is the `presto` image with tag `latest` hosted in the `prestodb` DockerHub repository. The value `8080:8080` means that we want to map port 8080 on the Docker host (left side of the colon) to port 8080 in the container (right of the colon).

We also need to supply the Presto container with some necessary configuration files, which we define using the `volume` key. Similar to how we defined the port, we're saying here that we want to map the files that are in the `presto/etc` directory (relative to our current working directory on the command line) to the location is the container corresponding to `/opt/presto-server/etc`, which is the directory that Presto expects to find configuration files. Here are the configuration settings for the Presto server as given in `./presto/etc/config.properties` that we will pass to our server container:

```text
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery-server.enabled=true
discovery.uri=http://localhost:8080
node.environment=test
```

- `coordinator`: defines whether this Presto server acts as a coordinator or not. Use value `true` for a coordinator
- `node-scheduler.include-coordinator`: defines whether the Presto server acts as a worker as well as a coordinator. We use the value `true` to accept worker tasks since we only have one node in our Presto cluster
- `http-server.http.port`: defines the port number for the HTTP server
- `discovery-server.enabled`: defines whether the Presto server should act as a discovery server to register workers
- `discovery.uri`: defines the discovery server's URI, which is itself in this case
- `node.environment`: defines the name of the environment; all Presto nodes in a cluster must have the same environment name

Next we specify any necesssary environment variables. In this case, we give the username and password required to access our MinIO storage. Finally, we state that this container is part of the previouly-created `presto_network`, meaning it will be able to communicate with other services on the network.

Let's do the same for the `hive-metastore` service, which has a few lines we haven't seen yet. The `build` property is what allowed us to build the custom image located in the `hive-metastore` directory in the previous step. We'll specify the image that we just created as the `image` property value. We also give a `hostname` for this container, the value of which we supply in the `metastore-site.xml` configuration file, which itself is mapped to the appropriate location inside the container using the `volumes` property. The last property that we will call out is `depends_on`, which defines dependencies between our service containers. In this case, the `mysql` container will be started before the `hive-metastore` Presto container. This makes sense since the MySQL database needs to be running before the Hive metastore service can start.

You should now have the context you need in order to understand the configuration for the remaining `services` `mysql` and `minio`. These services don't require as much setup as the others. On the last few lines of the file, we define additional `volumes`. These are different from those that we created on the fly in the `services` section in that here we create named volumes that can be persisted even if some containers need to restart.

The output of the `up` command will look like the below when all containers have been started:

```sh
[+] Running 7/7
 ✔ Network conf_presto-network   Created         0.0s
 ✔ Volume "conf_minio-data"      Created         0.0s
 ✔ Volume "conf_mysql-data"      Created         0.0s
 ✔ Container presto-coordinator  Started         50.0s
 ✔ Container mysql               Started         50.0s
 ✔ Container minio               Started         50.0s
 ✔ Container hive-metastore      Started         0.0s
```

## 3. Check that services have started

Let's also check that our relevant services have started.

```sh
docker logs --tail 100 minio
```

If started successfully, the logs for the `minio` container should include something similar to the below:

```sh
Status:         1 Online, 0 Offline.
S3-API: http://172.18.0.2:9090  http://127.0.0.1:9090
Console: http://172.18.0.2:9091 http://127.0.0.1:9091
```

We will be using the console address in the next exercise. Let's check that the Hive metastore is running with the following command:

```sh
docker logs --tail 50 hive-metastore
```

If the metastore service is up and running properly, you should see the below lines somewhere near the bottom of the logs, likely interspersed with other logging information.

```sh
...
Initialization script completed
schemaTool completed
...
2023-11-20 23:21:56: Starting Metastore Server
...
```

If the Hive metastore is up, the MySQL database also must be up because the metastore requires this on startup.

Now, let's check the coordinator node:

```sh
docker logs --tail 100 presto-coordinator
```

If the Presto server is up and running properly, the last lines of the output would like the following:

```sh
2023-11-14T04:03:22.246Z        INFO    main    com.facebook.presto.storage.TempStorageManager  -- Loading temp storage local --
2023-11-14T04:03:22.251Z        INFO    main    com.facebook.presto.storage.TempStorageManager  -- Loaded temp storage local --
2023-11-14T04:03:22.256Z        INFO    main    com.facebook.presto.server.PrestoServer ======== SERVER STARTED ========
```

The Presto server will likely take the longest to set up. If you don't see any errors or the `SERVER STARTED` notice, wait a few minutes and check the logs again.

You can also assess the status of your cluster using the Presto UI at the relevant IP address: `http://<your_ip>:8080`. If you're running everything on your local machine, the address will be `http://localhost:8080`. You should see 1 active worker (which is the coordinator node, in our case) and a green "ready" status in the top right corner, as seen below.

![presto ui](../images/presto-ui.png)

## 4. Connect to Iceberg

Our containers are up and running, but you may be wondering where Iceberg fits into all of this. Presto makes it very easy to get started with Iceberg, with no need to install any additional packages. If we started the Presto CLI right now, we would be able to create tables in Iceberg format - but how? Recall the volume that we passed to the `presto-coordinator` container. This volume includes a directory called `catalog` that was mapped to the `/opt/presto-server/etc/catalog` location in the container along with the other server configuration files. The `catalog` directory is where the Presto server looks to see what underlying data sources should be made available to Presto and how to connect to those sources. Let's take a look at the `iceberg.properties` file that was mapped to the Presto cluster.

```text
connector.name=iceberg
iceberg.catalog.type=hive
hive.metastore.uri=thrift://hive-metastore:9083
hive.s3.path-style-access=true
hive.s3.endpoint=http://minio:9090
hive.s3.aws-access-key=minio
hive.s3.aws-secret-key=minio123
```

This file includes a required `connector.name` property that indicates we're defining properties for an Iceberg connector. It also lists `hive` as the Iceberg catalog type, as we're using the Hive metastore catalog to support our Iceberg tables, and supplies the URI for the Hive metastore.  The remaining configuration options are specific to the Hive metastore and give the details needed in order to access our underlying s3 data source. When Presto starts, it accesses these configuration files in order to determine which connections it can make.

!!! note
      Recall that the `metastore.uri` property is the same value defined earlier in the `metastore-site.xml` file that was used to configure the metastore service.

Leveraging high-performance huge-data analytics is as easy as that! Let's move to the next exercise to set up our data source and start creating some Iceberg tables.

<img src="https://count.asgharlabs.io/count?p=/lab1_presto_iceberg_page">

# Set up the Data Source

In this section, you will prepare the MinIO instance to use with Presto.

This section is comprised of the following steps:

- [Set up the Data Source](#set-up-the-data-source)
  - [1. Create a storage bucket](#1-create-a-storage-bucket)

## 1. Create a storage bucket

As we have already seen, MinIO will act as our underlying data storage for this workshop. MinIO is a high-performance, open-source object storage system. Because it implements the AWS s3 API (i.e., it is "s3-compatible"), we are able to use it to integrate with Presto. These types of object stores use buckets to organize files, giving us a specific location to provide to Presto when we read and write tables.

We'll create our bucket using the MinIO UI. Access the MinIO UI in a browser at the relevant IP address: `http://<your_ip>:8443`. If you're running everything on your local machine, the address will be `http://localhost:8443`.

!!! note
    If you are using a remote Linux host, use the command `curl https://ipinfo.io/ip` to get your public IP address.

!!! note
    Recall that we assigned a port mapping of `8443:9091` in `docker-compose.yml`, meaning that within the Docker network, the MinIO UI exists on port 9091, but it is exposed on port 8443 when accessing it from outside of the container.

You will be prompted for a username and password, which are `minio` and `minio123` respectively, once again as defined in our `docker-compose.yml` for the `minio` service `environment` key. Once you are logged in, you will see a webpage like the below that indicates that there are no available bucket and prompts you to create one. Click "Create a Bucket".

![empty minio](../images/empty-minio.png)

Enter the name `test-bucket` and create the bucket. That's it! You can view the empty bucket in the "Object brower".

Now our s3 object store is ready for use. Let's move to the next section to start creating Iceberg tables in Presto.

<img src="https://count.asgharlabs.io/count?p=/lab2_presto_iceberg_page">

# Exploring Iceberg Tables

In this section, we will create Iceberg tables and explore their structure in the underlying data source. We will also take a look at the hidden tables that Presto provides for Iceberg that give information on the table metadata. We'll also use some of Iceberg's key features - schema evolution and time travel - from Presto to get an idea of how they work.

This section is comprised of the following steps:

- [Exploring Iceberg Tables](#exploring-iceberg-tables)
  - [1. Creating a schema](#1-creating-a-schema)
  - [2. Creating an Iceberg table](#2-creating-an-iceberg-table)
  - [3. Iceberg schema evolution](#3-iceberg-schema-evolution)
  - [4. Iceberg time travel](#4-iceberg-time-travel)

## 1. Creating a schema

First, let's learn how to run the Presto CLI to connect to the coordinator. There are several ways to do that:

1. Download the executable jar from the official repository and run the jar file with a proper JVM. You can see details in this [documentation](http://prestodb.io/docs/current/installation/cli.html#install-the-presto-cli).
2. Use the `presto-cli` that comes with the `prestodb/presto` Docker image

For this lab, since we run everything on Docker containers, we are going to use the second approach. You can run the `presto-cli` inside the coordinator container with the below command:

```sh
$ docker exec -it presto-coordinator presto-cli
presto>
```

!!! note
    Since the `presto-cli` is executed inside the `coordinator` and `localhost:8080` is the default server, there is no need to specify the `--server` argument.

After you run the command, the prompt should change from the shell prompt `$` to the `presto>` CLI prompt. Run the SQL statement `show catalogs` to see a list of currently configured catalogs:

```sh
presto> show catalogs;
 Catalog
---------
 hive
 iceberg
 jmx
 memory
 system
 tpcds
 tpch
(7 rows)

Query 20231122_230131_00021_79xda, FINISHED, 1 node
Splits: 19 total, 19 done (100.00%)
[Latency: client-side: 173ms, server-side: 163ms] [0 rows, 0B] [0 rows/s, 0B/s]
```

These are the catalogs that we specified when launching the coordinator container by using the configurations from the `presto/catalog` directory. The `hive` and `iceberg` catalogs here are expected, but here is a short description of the rest:

- [jmx](http://prestodb.io/docs/current/connector/jmx.html): The JMX connector provides the ability to query JMX
  information from all nodes in a Presto cluster.
- [memory](http://prestodb.io/docs/current/connector/memory.html): The Memory connector stores all data and metadata
  in RAM on workers and both are discarded when Presto restarts.
- [system](http://prestodb.io/docs/current/connector/system.html): The System connector provides information and
  metrics about the currently running Presto cluster.
- [tpcds](http://prestodb.io/docs/current/connector/tpcds.html): The TPCDS connector provides a set of schemas
  to support the TPC Benchmark™ DS (TPC-DS)
- [tpch](http://prestodb.io/docs/current/connector/tpch.html): The TPCH connector provides a set of schemas to
  support the TPC Benchmark™ H (TPC-H).

Now, let's create a schema. A schema is a logical way to organize tables within a catalog. We'll create a schema called "minio" within our "iceberg" catalog. We also want to specify that the tables within this schema are all located in our s3 storage, and more specifically, in the `test-bucket` bucket that we created previously.

```sh
presto> CREATE SCHEMA iceberg.minio with (location = 's3a://test-bucket/');
CREATE SCHEMA
```

We'll be working almost exclusively with the "iceberg" catalog and "minio" schema, so we can employ a `USE` statement to indicate that all the queries we run will be against tables in this catalog/schema combination unless specificed. Otherwise, we would have to use the fully-qualified table name for every statement (i.e., `iceberg.minio.<table_name>`).

```sh
presto> USE iceberg.minio;
USE
presto:minio>
```

You'll notice that the prompt has changed to also include the schema we're working in. Now we're ready to create a table!

## 2. Creating an Iceberg table

When creating a new table, we specify the name and the table schema. A table schema is different than the schema we've been referring to up until now. The table schema defines the column names and types. Let's create a table to represent the books that a (very small) library has in their inventory.

```sh
presto:minio> CREATE TABLE IF NOT EXISTS books (id bigint, title varchar, author varchar) WITH (location = 's3a://test-bucket/minio/books');
CREATE TABLE
```

We now have the table structure, but no data. Let's look into this a little bit. Pull up your MinIO UI and navigate to the `test-bucket/minio/books` directory. Notice that the `minio` and `books` directories were created implicitly with the location property that we passed to `CREATE TABLE`. There should be a single folder at this location called `metadata`. If you go into this directory, you'll see a single metadata file with the extension `.metadata.json`, which stores the table schema information.

Let's add some data to this table:

```sh
presto:minio> INSERT INTO books VALUES (1, 'Pride and Prejudice', 'Jane Austen'), (2, 'To Kill a Mockingbird', 'Harper Lee'), (3, 'The Great Gatsby', 'F. Scott Fitzgerald');
INSERT: 3 rows

Query 20231123_021811_00005_79xda, FINISHED, 1 node
Splits: 35 total, 35 done (100.00%)
[Latency: client-side: 0:01, server-side: 0:01] [0 rows, 0B] [0 rows/s, 0B/s]
```

We can verify our data by running a `SELECT *` statement:

```sh
presto:minio> SELECT * FROM books;
 id |         title         |       author
----+-----------------------+---------------------
  1 | Pride and Prejudice   | Jane Austen
  2 | To Kill a Mockingbird | Harper Lee
  3 | The Great Gatsby      | F. Scott Fitzgerald
(3 rows)
```

If we go back to our MinIO UI now, we can see a new folder, `data` in the `test-bucket/minio/books` path. The `data` folder has a single `.parquet` data file inside. This structure of `data` and `metadata` folders is the default for Iceberg tables.

We can query some of the Iceberg metadata information from Presto. Let's look at the hidden "history" table from Presto. Note that the quotation marks are required here.

```sh
presto:minio> SELECT * FROM "books$history";
       made_current_at       |     snapshot_id     | parent_id | is_current_ancestor
-----------------------------+---------------------+-----------+---------------------
 2023-12-04 03:22:51.654 UTC | 7120201811871583704 | NULL      | true
(1 row)

Query 20231204_032649_00007_8ds9i, FINISHED, 1 node
Splits: 17 total, 17 done (100.00%)
[Latency: client-side: 0:04, server-side: 0:04] [1 rows, 17B] [0 rows/s, 4B/s]
```

This shows us that we have a snapshot that was created at the moment we inserted data. We can get more details about the snapshot with the below query:

```sh
presto:minio> SELECT * FROM "books$snapshots";
        committed_at         |     snapshot_id     | parent_id | operation |                                                manifest_list                                                |                                                                                                           summary
-----------------------------+---------------------+-----------+-----------+-------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 2023-12-04 03:22:51.654 UTC | 7120201811871583704 | NULL      | append    | s3a://test-bucket/minio/books/metadata/snap-7120201811871583704-1-c736f70c-53b0-46bd-93e5-5df38eb0ef62.avro | {changed-partition-count=1, added-data-files=1, total-equality-deletes=0, added-records=3, total-position-deletes=0, added-files-size=579, total-delete-files=0, total-files-size=579, total-records=3, total-data-files=1}
(1 row)
```

This gets us a little more information, such as the type of operation, the manifest list file that this snapshot refers to, as well as a summary of the changes that were made as a result of this operation.

Let's go one level deeper and look at the current manifest list metadata:

```sh
presto:minio> SELECT * FROM "books$manifests";
                                        path                                         | length | partition_spec_id |  added_snapshot_id  | added_data_files_count | existing_data_files_count | deleted_data_files_count | partitions
-------------------------------------------------------------------------------------+--------+-------------------+---------------------+------------------------+---------------------------+--------------------------+------------
 s3a://test-bucket/minio/books/metadata/c736f70c-53b0-46bd-93e5-5df38eb0ef62-m0.avro |   6783 |                 0 | 7120201811871583704 |                      1 |                         0 |                        0 | []
(1 row)
```

As promised, the manifest list table show us a list of the manifest files (or file, in this case) associated with our current state.

Lastly, let's look at what the manifests can tell us. To do so, we call on the `files` hidden table:

```sh
presto:minio> SELECT * FROM "books$files";
 content |                                    file_path                                    | file_format | record_count | file_size_in_bytes |     column_sizes     |  value_counts   | null_value_counts | nan_value_counts |                 lower_bounds                  |               upper_bounds               | key_metadata | split_offsets | equality_ids
---------+---------------------------------------------------------------------------------+-------------+--------------+--------------------+----------------------+-----------------+-------------------+------------------+-----------------------------------------------+------------------------------------------+--------------+---------------+--------------
       0 | s3a://test-bucket/minio/books/data/27b61673-a995-4810-9aa5-b4675b8483ce.parquet | PARQUET     |            3 |                579 | {1=52, 2=124, 3=103} | {1=3, 2=3, 3=3} | {1=0, 2=0, 3=0}   | {}               | {1=1, 2=Pride and Prejud, 3=F. Scott Fitzger} | {1=3, 2=To Kill a Mockio, 3=Jane Austen} | NULL         | NULL          | NULL
(1 row)
```

We have here a path to the data file and some metadata for that file that can help when determining which files need to be accessed for a certain query.

There are other hidden tables as well that you can interrogate. Here is a summary of all hidden tables that Presto can provide:

- `$properties`: General properties of the given table
- `$history`: History of table state changes
- `$snapshots`: Details about the table snapshots
- `$manifests`: Details about the manifest lists of different table snapshots
- `$partitions`: Detailed partition information for the table
- `$files`: Overview of data files in the current snapshot of the table

## 3. Iceberg schema evolution

The Iceberg connector also supports in-place table evolution, aka schema evolution, such as adding, dropping, and renaming columns. This is one of Iceberg's key features. Let's try it. Let's say we want to add a column to indicate whether a book has been checked out. We'll run the following command to do so:

```sh
presto:minio> ALTER TABLE books ADD COLUMN checked_out boolean;
ADD COLUMN
```

At this point, a new `.metadata.json` file is created and can be viewed in the MinIO UI, but, once again, no updates to the other metadata files or the hidden tables take place until data is added. The library comes into the possession of a new book and it is immediately checked out. We can add data for that:

```sh
presto:minio> INSERT INTO books VALUES (4, 'One Hundred Years of Solitude', 'Gabriel Garcia Marquez', true);
INSERT: 1 row

Query 20231123_025430_00013_79xda, FINISHED, 1 node
Splits: 35 total, 35 done (100.00%)
[Latency: client-side: 0:01, server-side: 0:01] [0 rows, 0B] [0 rows/s, 0B/s]
```

At this point, a new snapshot is made current, which we can see by querying the hidden snapshot table:

```sh
presto:minio> SELECT * FROM "books$snapshots";
        committed_at         |     snapshot_id     |      parent_id      | operation |                                                manifest_list                                                |                                                                                                           summary
-----------------------------+---------------------+---------------------+-----------+-------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 2023-12-04 03:22:51.654 UTC | 7120201811871583704 | NULL                | append    | s3a://test-bucket/minio/books/metadata/snap-7120201811871583704-1-c736f70c-53b0-46bd-93e5-5df38eb0ef62.avro | {changed-partition-count=1, added-data-files=1, total-equality-deletes=0, added-records=3, total-position-deletes=0, added-files-size=579, total-delete-files=0, total-files-size=579, total-records=3, total-data-files=1}
 2023-12-04 03:33:37.630 UTC | 5122816232892408908 | 7120201811871583704 | append    | s3a://test-bucket/minio/books/metadata/snap-5122816232892408908-1-973a8dc3-8103-4df7-8324-1fa13a2f1202.avro | {changed-partition-count=1, added-data-files=1, total-equality-deletes=0, added-records=1, total-position-deletes=0, added-files-size=765, total-delete-files=0, total-files-size=1344, total-records=4, total-data-files=2}
(2 rows)
```

The output confirms that we now have a new snapshot, and a new manifest list file representing it.

## 4. Iceberg time travel

Another popular feature of Iceberg is time travel, wherein we can query the table state from a given time or snapshot ID. It's also possible to rollback the state of a table to a previous snapshot using its ID. For the purposes of our example, let's say the person that checked out _One Hundred Days of Solitude_ enjoyed it so much that they bought it from the library, taking it out of the inventory. We want to roll the table state back to before we inserted the latest row. Let's first get our snapshot IDs.

```sh
presto:minio> SELECT snapshot_id, committed_at FROM "books$snapshots" ORDER BY committed_at;
     snapshot_id     |        committed_at
---------------------+-----------------------------
 7120201811871583704 | 2023-12-04 03:22:51.654 UTC
 5122816232892408908 | 2023-12-04 03:33:37.630 UTC
(2 rows)
```

Let's verify that the table is in the expected state at our earliest snapshot ID:

```sh
presto:minio> SELECT * FROM books FOR VERSION AS OF 7120201811871583704;
 id |         title         |       author        | checked_out
----+-----------------------+---------------------+-------------
  1 | Pride and Prejudice   | Jane Austen         | NULL
  2 | To Kill a Mockingbird | Harper Lee          | NULL
  3 | The Great Gatsby      | F. Scott Fitzgerald | NULL
(3 rows)
```

We could also do the same thing using a timestamp or date. If you run this query, make sure you change the timestamp so that it's accurate for the time at which you're following along.

```sh
presto:minio> SELECT * FROM books FOR TIMESTAMP AS OF TIMESTAMP '2023-12-04 03:22:51.700 UTC';
 id |         title         |       author        | checked_out
----+-----------------------+---------------------+-------------
  1 | Pride and Prejudice   | Jane Austen         | NULL
  2 | To Kill a Mockingbird | Harper Lee          | NULL
  3 | The Great Gatsby      | F. Scott Fitzgerald | NULL
(3 rows)
```

Now that we've verified the table state that we want to roll back to, we can call a procedure on the "iceberg" catalog's built-in `system` schema to do so:

```sh
presto:minio> CALL iceberg.system.rollback_to_snapshot('minio', 'books', 7120201811871583704);
CALL
```

Let's verify that the table is back to how it was before:

```sh
presto:minio> SELECT * FROM books;
 id |         title         |       author        | checked_out
----+-----------------------+---------------------+-------------
  1 | Pride and Prejudice   | Jane Austen         | NULL
  2 | To Kill a Mockingbird | Harper Lee          | NULL
  3 | The Great Gatsby      | F. Scott Fitzgerald | NULL
(3 rows)
```

Notice that the table still includes the `checked_out` column. This is to be expected because the snapshot only changes when data files are written to. Removing the column would be another schema evolution operation that only changes the `.metadata.json` file and not the snapshot itself.

You just explored some of Iceberg's key features using Presto! Presto's Iceberg connector has more features than those we've gone over today, such as partitioning and partition column transforms, as well as additional features that are soon-to-come!

<img src="https://count.asgharlabs.io/count?p=/lab3_presto_iceberg_page">


## 2. Creating an Iceberg table via Jupyter Notebook

Open Jupyter notebook using the url printed out in the container's log, something like:

```sh
http://127.0.0.1:8888/lab?token=e5b724f3aeecfafe8f54ef1e73e76b18aa7d7e38b0121958
```
Install s3fs package in order to work with minio file storage
```sh
!pip install s3fs
```

Now import necessary dependencies
```python
import requests
import json
import os
import s3fs
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
import pyspark.sql.functions as F

os.environ["MINIO_KEY"] = "minio"
os.environ["MINIO_SECRET"] = "minio123"
os.environ["MINIO_ENDPOINT"] = "http://minio:9090"
```
Configure Spark Session so that it can work with iceberg catalog

```python
spark = SparkSession.builder \
    .appName("country_data_analysis") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.11.1026,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1") \
    .config("spark.hadoop.fs.s3a.endpoint", os.environ["MINIO_ENDPOINT"]) \
    .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_KEY"]) \
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_SECRET"]) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://wba") \
    .config("spark.sql.defaultCatalog", "spark_catalog") \
    .config("spark.hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.sql.warehouse.dir", "s3a://wba/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()
```

Now you are able to create iceberg tables

```python
spark.sql("CREATE SCHEMA IF NOT EXISTS spark_catalog.ingesta_vut")
spark.sql("CREATE TABLE IF NOT EXISTS spark_catalog.ingesta_vut.estadistica_tabla ( TEST STRING ) USING ICEBERG;")
```

# Additional resources

## Data Lakehouse

* [What is a Data Lakehouse](https://www.ibm.com/topics/data-lakehouse#:~:text=A%20data%20lakehouse%20is%20a,in%20their%20ability%20to%20scale.)
* [A Gentle Introduction to Data Lakehouse](https://towardsdatascience.com/a-gentle-introduction-to-data-lakehouse-fc0f131f90ff)

## Presto

* [Presto](https://prestodb.io/)
* [Presto Documentation](https://prestodb.io/docs/current/)

## Iceberg

* [Iceberg](https://iceberg.apache.org)
* [Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
* [Iceberg Table Spec](https://iceberg.apache.org/spec/)
* [Understanding Iceberg Table Metadata](https://medium.com/snowflake/understanding-iceberg-table-metadata-b1209fbcc7c3)

## MinIO

* [MinIO](https://min.io/)
* [MinIO Documentation](https://min.io/docs/minio/kubernetes/upstream/)
