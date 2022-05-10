This pages describes the available properties to configure ChainPaxos, as well as the replicated key-value store application.

Every property can be overridden when launching the process, as shown in the instructions on the [README](https://github.com/pfouto/chain/blob/master/README.md).

The available properties are the following:

* **interface**: the network interface which will be used for the replicas to communicate (TCP sockets will be bound to the IP address of this interface)
* **algorithm**: the consensus algorithm to use. Valid options are:
  * chain_delayed - the ChainPaxos protocol, as detailed in the paper
  * chain_mixed - the ChainPaxos protocol, but without using the local read algorithm, instead propagating read operations through the chain.
  * chainrep - our implementation of the Chain Replication protocol.
  * distinguished - our implementation of the Multi Paxos variant that used a distinguished learner.
  * distinguished_piggy - our implementation of the Multi Paxos variant that used a distinguished learner *and* piggybacks *accept* messages with the previous *decision* message
  * multi - our implementation Multi Paxos.
  * epaxos - our implementation of Egalitarian Paxos.
  * esolatedpaxos - a variant of Egalitarian Paxos where operations *never* conflict with each other.
  * ring - our implementation of Ring Paxos
  * ringpiggy - our implementation of RingPaxos which piggybacks accept messages
  * uring - our implementation of U-Ring Paxos

* **consensus_port**: the port to use for the consensus protocol to communicate
* **frontend_peer_port**: the port to use for the frontend to communicate (the frontend is an internal component responsible for redirecting operations to the current leader of the consensus algorithm)
* **app_port**: the port to use for the application to receive client connections.
* **quorum_size**: the size of a quorum (should be # of nodes/2 + 1)
* **leader_timeout**: how long a node waits without hearing from the leader before attempting to become a leader ifself.
* **noop_interval**: interval between executing `no_op` operations, which prevents nodes from suspecting the leader when no application operations are being submitted.
* **join_timeout**: how long a joining replica waits after asking to join the cluster, before restarting the join process.
* **state_transfer_timeout**: how long a recently joined replica waits to receive a state transfer before asking a different node for the state transfer.
* **reconnect_time**: how long a replica waits before attempting to re-connect to another replica after its previous connection failed or was lost.
* **initial_state**: the initial state of a node, should be `ACTIVE` when bootstrapping a cluster, and `JOINING` when adding replicas to the cluster.
* **initial_membership**: if bootstrapping a cluster, this should contain a comma separated list of all replicas in the bootstrap. If joining a cluster, this should contain a list of seed nodes already in the cluster.
* **batch_size**: the size of batches to use (i.e., the number of application operations bundled in each consensus round)
* **batch_interval**: how long to wait before submitting a non-full batch
* **local_batch_size**: the size of batches for local read operations in ChainPaxos
* **local_batch_interval**: how long to wait before submitting a non-full read batch

RingPaxos specific configurations:
* **req_timeout**: the time a replica waits before asking another replica for an old missing decision
* **accept_timeout**: the time the leader waits after not receiving a quorum of responses to an `accept` message before re-sending it. 
* **ring_max_instances**: the maximum number of parallel instances

