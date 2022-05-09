ChainPaxos
============

### What is ChainPaxos
Implementation of the ChainPaxos consensus algorithm for high throughput replication.

For details on the internals of the algorithm, refer to the USENIX ATC'22 paper 
(to be published soon) that describes ChainPaxos in detail.

### Contents of the repository

This repository contains:
* The Java implementation of ChainPaxos (in package chainpaxos)
* Java implementation of other consensus protocols, using the same codebase as ChainPaxos,
which were used in the experiments present in the aforementioned paper:
    * [Chain Replication](https://www.usenix.org/legacy/events/osdi04/tech/full_papers/renesse/renesse.pdf) (package chainreplication). Including a version where reads are handled like writes, being propagated through the chain (ChainPaxosMixed); and then version with the local read algorithm presented in the papaer (ChainPaxosDelayed).
    * [Egalitarian Paxos](https://dl.acm.org/doi/abs/10.1145/2517349.2517350) (package epaxos)
    * [Ring Paxos](https://ieeexplore.ieee.org/abstract/document/5544272) and [U-Ring Paxos](https://academic.oup.com/comjnl/article/60/6/866/3058780) (packages ringpaxos and uringpaxos)
    * Multi-Paxos and Multi-Paxos with a distinguished learner (package distinguishedpaxos)
* A simple replicated key-value store application, which can be used to test the protocols (package app).

The folder deploy/server contains all files required to execute the protocols:
* `chain.jar` - a "uber" jar file with the source code of this repository plus all dependencies bundled together.
  If you wish to make changes to the source code, executing "mvn package" in the root directory will build a new jar with your changes.
* `config.properties` - the file that contains all configurations required to execute the protocols. Details on what each field does can be found [here](placeholder).
  Every configuration in this file can be overridden when launching the process, and not all fields are required for all protocols.
* `log4j2.xml` - log4j2 configuration file.
* `log4j.properties` - log4j (version 1) configuration file. Required for the zookeeper client library.

### Running ChainPaxos

In order to test the protocol, at least 3 hosts are required (hosts can be physical machines, docker containers, or virtual machines).
This is required since the protocol binds to the same ports in each host. The hosts should be able to communicate with each other, ideally in a private network without any firewalls or NAT between them.

The only requirement to run ChainPaxos is having Java installed on the hosts. The protocol was developed and tested using OpenJDK 17, however versions 11 or 8 should also work (no guarantees).

Start by cloning the repository and then copying the folder deploy/server to each host.

Then, from the folder where the jar file is, run the following command in each host:

    java -Dlog4j.configurationFile=log4j2.xml \
        -Djava.net.preferIPv4Stack=true \
        -DlogFilename=/tmp/chain_logs \
        -cp chain.jar:. app.HashMapApp \
        interface=<bind interface> \
        algorithm=chain_delayed \
        initial_membership=<server list> \
        initial_state=ACTIVE \
        quorum_size=<quorum size>

Replacing:
* `<bind interface>` with the name of the interface which will be used for the replicas to communicate
* `<server list>` a list of comma separated (without spaces) ip addresses (or names) of the hosts running the replicas. For instance: `192.168.0.1,192.168.0.2,192.168.0.3`
* `<quorum size>` the size of the initial quorum. If running 3 replicas, this number should be 2.


  

For instance, if the hosts have the ip addresses `192.168.0.1`, `192.168.0.2` and `192.168.0.3` in interface `eth0`, you would run the following command:

    java -Dlog4j.configurationFile=log4j2.xml \
        -Djava.net.preferIPv4Stack=true \
        -DlogFilename=/tmp/chain_logs \
        -cp chain.jar:. app.HashMapApp \
        interface=eth0 \
        algorithm=chain_delayed \
        initial_membership=192.168.0.1,192.168.0.2,192.168.0.3 \
        initial_state=ACTIVE \
        quorum_size=2

After a few seconds (defined by the `leader_timeout` configuration), one of the replicas will attempt to become leader and
will show the following output: 

`I am leader now! @ instance 0`

while the other replicas will output that the support the new leader:

`New highest instance leader: iN:0, SN{0:192.168.0.1:50300}`. 

If you see these outputs, then the protocol is running normally, and is executing periodic `no_op` operations while 
it awaits client connections. Changing the log level to debug in the `log4j2.xml` file will allow you to see these `no_op` operations.

---
**NOTE**

All replicas should be launched in a time window lower than the configured `leader_timeout`. 
If a replica takes too long to be launched, then the other replicas may have already removed it from their membership, causing undefined behaviour.

To add new (or removed) replicas to the membership, execute the command with `initial_state=JOINING`.

---
### Testing with clients

In order to test and benchmark the protocol, you can use the YCSB client available [here](https://github.com/pfouto/chain-client).

The file structure is the same, with all required files already available in the folder deploy/client.
The `config.properties` file contains YCSB-specific configuration, with documentation found [here](https://github.com/brianfrankcooper/YCSB/wiki).

To run the client, copy this folder to either one of the hosts that is running a replica of ChainPaxos, or any other host in the same network, and then run the following command:
    
    java -cp chain-client.jar \ 
    site.ycsb.Client -t -s -P config.properties \
    -threads <n_threads> -p fieldlength=<payload> \
    -p hosts=<server list> \
    -p readproportion=<reads_per> -p insertproportion=<writes_per>

Where:
* `n_threads` is the number of client threads to execute, where each thread simulates an individual client.
* `payload` is the length of the payload written in each write operation (and therefore also the size of the payload read)
* `server list` is a list of comma separated (without spaces) ip addresses (or names) of the replicas to connect to.
If more than one address is used, the clients will be distributed uniformly between replicas.
* `reads_per` and `writes_per` are the percentage of read and write operations executed by clients.

For instance, if the replicas are running on the hosts with addresses `192.168.0.1`, `192.168.0.2` and `192.168.0.3`, and you wanted `20` clients to execute 50/50 read and write operations with payloads of 1kb, you would run the following command:

    java -cp chain-client.jar:. site.ycsb.Client -t -s -P config.properties \
    -threads 20 -p fieldlength=1024 \
    -p hosts=192.168.0.1,192.168.0.2,192.168.0.3 \
    -p readproportion=0.5 -p insertproportion=0.5

After which the following message should appear `Connected to all servers!` and then every 10 seconds YCSB should print the current
statistics of the operations being executed. If no changes were made to the configuration, the client should run for 85 seconds and then terminate.

Upon terminating the replicas (by killing the processes with CTRL+C), you should see an output that shows the number of write and read operations executed by the replica
(e.g., `Writes: 3451249, reads: 1176220`). Naturally, the number of writes should be the same in every replica.

---

The implementations in this repository use the [Babel](https://github.com/pfouto/babel-core) framework for building distributed protocols.