ChainPaxos
============

## What is ChainPaxos?
Implementation of the ChainPaxos consensus algorithm for high throughput replication.

For details on the internals of the algorithm, refer to the USENIX ATC'22 paper 
(to be published soon) that describes ChainPaxos in detail.

## Contents of the repository

This repository contains:
* The Java implementation of ChainPaxos (in package chainpaxos), including:
  * a version where reads are handled like writes, being propagated through the chain (ChainPaxosMixed); 
  * and the version with the local read algorithm presented in the paper (ChainPaxosDelayed).
* Java implementations of other consensus protocols, using the same codebase as ChainPaxos,
which were used in the experiments presented in the aforementioned paper:
    * [Chain Replication](https://www.usenix.org/legacy/events/osdi04/tech/full_papers/renesse/renesse.pdf) (package chainreplication);
    * [Egalitarian Paxos](https://dl.acm.org/doi/abs/10.1145/2517349.2517350) (package epaxos);
    * [Ring Paxos](https://ieeexplore.ieee.org/abstract/document/5544272) and [U-Ring Paxos](https://academic.oup.com/comjnl/article/60/6/866/3058780) (packages ringpaxos and uringpaxos);
    * Multi-Paxos and Multi-Paxos with a distinguished learner (package distinguishedpaxos);
* A simple replicated key-value store application, which can be used to test the protocols (package app).

The folder deploy/server contains all files required to execute the protocols:
* `chain.jar` - an "uber" jar file with the source code of this repository plus all dependencies bundled together.
  If you wish to make changes to the source code, executing "mvn package" in the root directory will build a new jar with your changes.
* `config.properties` - the file that contains all configurations required to execute the protocols. Details on what each field does can be found [here](https://github.com/pfouto/chain/wiki/Configuration).
  Every configuration in this file can be overridden when launching the process, and not all fields are required for all protocols.
* `log4j2.xml` - log4j2 configuration file.
* `log4j.properties` - log4j (version 1) configuration file. Required for the zookeeper client library.

## ChainPaxos Client

A client for the key-value store application was implemented over the [YCSB](https://github.com/brianfrankcooper/YCSB) benchmarking tool. It is available [here](https://github.com/pfouto/chain-client). 

## How to run ChainPaxos

This section contains instructions on how to test ChainPaxos by running the replicated key-value store on 3 replicas, 
and then using the YCSB client to execute operations over the application.
Two options are presented:
* [Running everything on a single machine using docker](#Running-ChainPaxos-in-a-single-machine-with-docker) - this only requires having docker installed.
* [Running each replica on a different machine](#Running-ChainPaxos-in-multiple-machines), which is closer to what a production environment would look like - this
requires having Java installed in each machine, and that the machines be able to communicate with each other. The setup is also
slightly more complex.

### Running ChainPaxos in a single machine with docker

To test ChainPaxos with docker, all we need to do is build the docker image and then execute a provided script that runs the test.

First, clone this repository, which includes a Dockerfile, and build the image, naming it `chain-paxos`:

    git clone https://github.com/pfouto/chain/
    cd chain
    docker build . -t chain-paxos
    
Then do the same for the client, naming the image `chain-client`:

    git clone https://github.com/pfouto/chain-client/
    cd chain-client
    docker build . -t chain-client

Finally, execute the script `docker_test.sh`, which is available in the folder to where this repository was cloned.

This script does the following:
* Creates a network named `chain-net`
* Launches 3 docker containers named `chain1`, `chain2` and `chain3` on the created network.
* Launches the replicated key-value store application in each replica, with some pre-defined configurations. Read [configuration page](https://github.com/pfouto/chain-client/wiki/Configuration) and [Running ChainPaxos in multiple machines](#Running-ChainPaxos-in-multiple-machines) for more details on the configurations.
* Waits a few seconds for the replicas to connect to each other and elect a leader (you will see the leader election output on your terminal)
* Launches a new container with the YCSB client, which runs for 45 seconds
  * This client in configured to launch 20 client threads, which are distributed across all replicas, executing 50% read and 50% write operations with a payload size of 100 bytes.
  * Feel free to modify these parameters in the script to see how the benchmark results change.
* After the YCSB client terminates, the script kills the application containers, which display the number of executed reads and writes before terminating.

The script `docker_clean.sh` can be used to clean your docker environment after you are done experimenting. 
It kills and removes any dangling containers used by the previous script (useful if you accidentally stopped the script while the experiment was still running), removes the created network and
removes the previously created images. Note that some intermediate images (namely `openjdk:17-alpine`) may still be present after executing this script.

### Running ChainPaxos in multiple machines

In order to test the protocol in multiple machines, at least 3 hosts are required, since the protocol binds to the same ports in each host.
Hosts can be physical machines, docker containers in a swarm, or virtual machines,
and should be able to communicate with each other, ideally in a private network without any firewalls or NAT between them.

The only requirement to run ChainPaxos is having Java installed on the hosts. The protocol was developed and tested using OpenJDK 17, however versions 11 or 8 should also work, though we cannot guarantee that they will.

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
* `<server list>` with a list of comma separated (without spaces) ip addresses (or names) of the hosts running the replicas. For instance: `192.168.0.1,192.168.0.2,192.168.0.3`
* `<quorum size>` with the size of the initial quorum. If running 3 replicas, this number should be 2.


  

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

This will run the replicated key-value store application, with the configurations present in the file `config.properties`.
Any configurations passed in the previous command (such as `initial_state=...`) override the configurations in this file.
After a few seconds (defined by the `leader_timeout` configuration), one of the replicas will attempt to become leader and
will show the following output: 

`I am leader now! @ instance 0`

while the other replicas will output that they support the new leader:

`New highest instance leader: iN:0, SN{0:192.168.0.1:50300}`. 

If you see these outputs, then the protocol is running normally, and is executing periodic `no_op` operations while 
it awaits client connections. Changing the log level to debug in the `log4j2.xml` file will allow you to see these `no_op` operations.

---
**NOTE**

All replicas should be launched in a time window lower than the configured `leader_timeout`. 
If a replica takes too long to be launched, then the other replicas may have already removed it from their membership, causing undefined behaviour.

To add new (or removed) replicas to the membership, execute the command with `initial_state=JOINING`.

---
#### Testing with clients

In order to test and benchmark the protocol, you can use the YCSB client available [here](https://github.com/pfouto/chain-client).

The file structure is the same, with all required files already available in the folder deploy/client.
The `config.properties` file contains YCSB-specific configuration, with documentation found [here](https://github.com/brianfrankcooper/YCSB/wiki).

To run the client, copy the `deploy/client` folder to either one of the hosts that is running a replica of ChainPaxos, or any other host in the same network, and then run the following command inside that folder:
    
    java -cp chain-client.jar:. site.ycsb.Client -t -s -P config.properties \
    -threads <n_threads> -p fieldlength=<payload> \
    -p hosts=<server list> \
    -p readproportion=<reads_per> -p updateproportion=<writes_per>

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
    -p readproportion=0.5 -p updateproportion=0.5

After which the following message should appear `Connected to all servers!` and then every 10 seconds YCSB should print the current
statistics of the operations being executed. If no changes were made to the configuration, the client should run for 85 seconds and then terminate.

Upon terminating the replicas (by killing the processes with CTRL+C), you should see an output that shows the number of write and read operations executed by the replica
(e.g., `Writes: 3451249, reads: 1176220`). Naturally, the number of writes should be the same in every replica.

---

The implementations in this repository use the [Babel](https://github.com/pfouto/babel-core) framework for building distributed protocols.