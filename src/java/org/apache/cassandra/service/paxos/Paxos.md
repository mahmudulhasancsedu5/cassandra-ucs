# Light-weight transactions and Paxos algorithm

Our implementation of light-weight transactions (LWT) is loosely based on the classic Paxos single-decision algorithm.
It contains a range of modifications that make it different from a direct application of a sequence of independent
decisions.

Below we will describe the basic process (used by both V1 and V2 implementations) with the actions that each participant
takes and sketch a proof why the approach is safe, and then talk about various later improvements that do not change the
correctness of the basic scheme.


## Basic scheme

For each partition with LWT modification, we maintain the following registers:
- The current "promised" ballot number.
- The latest "accepted" proposal together with its ballot number and a "committed" flag.

We define an ordering on the latter by ballot number and committed flag, where "committed" true is interpreted as greater on equal ballot numbers. Two proposals are equal when their ballot numbers and committed flag match.

The basic scheme includes the following stages:

1. A coordinator selects a fresh ballot number (based on time and made unique by including a source node identifier).
2. The coordinator sends a "prepare" message to all replicas responsible for the targeted partition with the given ballot number and a request to read the data required by the operation.
3. By accepting the message, a replica declares that it will not accept any "prepare" or "propose" message with smaller
  ballot number:
   1. If the replica's "promised" register is greater than the number included in the message, the replica rejects it,
    sending back its current "promised" number in a "rejected" message.
   2. Otherwise, the replica stores the ballot number from the message in its "promised" register and replies with a "promise" message which includes the contents of the current "accepted" register together with the requested data from the database.
4. On receipt of "promise"s from a quorum of nodes, the coordinator compiles the "most recently accepted" (MRA) value as the greatest among the accepted values in the promises and then:
   1. If the MRA is not null and its committed flag is not true, there is an in-progress Paxos session that needs to be completed and committed. Controller reproposes the value with the new ballot (i.e. follow the steps below with a proposed value equal to the MRA's) and then restart the process.
   2. If the MRA is not null, its committed flag is true and it is not a match for the "accepted" value of a quorum of promises, controller sends a "commit" message to all replicas whose value did not match and awaits responses until they form a quorum of replicas with a matching "accepted" value.
   3. The controller then creates a proposal which is the result of applying the operation, using a read result from one of the responses that originally matched the MRA (note that this response's "accepted" value must have its committed flag true). It then sends the proposal (a partition update which is empty for reads or failing CAS) as a "propose" message with the value and the current ballot number to all replicas.
5. A replica accepts the proposal if it has not promised that it will not do so:
   1. If its "promised" ballot number is not higher than the proposal's, it sets its "accepted" register to the proposal with its ballot number and a false "committed" flag, updates its "promised" register to the proposal's ballot and sends back an "accepted" message.
   2. Otherwise it rejects the proposal by sending the current "promised" number in a "rejected" message.
   3. On receipt of "accepted" messages from a quorum of nodes, the Paxos session has reached a decision. The coordinator completes it by sending "commit" messages to all replicas, attaching the proposal value and its ballot number.
      1. It can return completion without waiting for receipt of any commit messages.
6. A replica accepts a commit unconditionally, by applying the attached partition update. If the commit's ballot number is greater than the replica's "accepted" ballot number, it sets its "accepted" register to the message's value and ballot with true "committed" flag, and the "promised" register to its ballot number.
7. If at any stage of the process that requires a quorum the quorum is not reached, the process restarts from the beginning using a fresh ballot that is greater than the ballot contained in any "rejected" message received.

We can't directly map multi-instance classic Paxos onto this, but we can follow its correctness proofs to ensure the following propositions:
1. Once a value (possibly empty) has been decided (i.e. accepted by a majority), no earlier proposal can be reproposed.
2. Once a value has been decided, in any further round (i.e. action taken by any proposer) it will either be accepted and/or committed, or it will have been committed in an earlier round and will be witnessed by at least one member of the quorum.

Suppose the value V was decided in the round with ballot number E0.

Proposition 1 is true because new proposals can only be made after a successful promise round with ballot number E > E0. To be successful, it needs a quorum which must intersect in at least one replica with E0's decision quorum. Since that replica accepted that proposal, it cannot have made a promise on E before that and must thus return an accepted value whose ballot number is at least E0. This is true because both "propose" and "commit" actions can only replace the "accepted" value with one with a higher or equal ballot number.

Proposition 2 can be proven true by induction on the following invariant: for any successful ballot number E >= E0, either:
1. For all quorums of replicas, commit V with some ballot number G < E, G >= E0 was witnessed by some replica in the quorum.
2. For all quorums of replicas, the "accepted" value with the highest ballot number among the replicas in the quorum is V with some ballot number G where G <= E, G >= E0.

For round E == E0 the invariant is true because all quorums contain a replica that accepted V with ballot E0, and E0 is the newest ballot number.

Suppose the invariant is true for some round E and examine the behaviour of the algorithm on F = succ(E).
- If 1. was true for E, it remains true for F.
- If the promise pass did not reach a quorum, no accepted values change and hence 2. is still true.
- If the promise reached a quorum, the collected MRA is V with ballot G.
  - If the MRA's committed flag is not true, the value V is reproposed with ballot F.
    - If the proposal reaches no node, no accepted values change and hence 2. is still true.
    - If the proposal reaches a minority of nodes, any quorum that includes one of these nodes will have V with ballot F as their highest "accepted" value. All other quorums will not have changed and still have V as their highest accepted value with an earlier ballot >= E0. In any case, 2. is still true.
    - If the proposal reaches a majority of nodes, all quorums have V with ballot F as the highest "accepted" value and thus satisfy 2.
    - Any "commit" message that is issued after a majority can only change the accepted value's "committed" flag -- all quorums will still satisfy 2.
  - If the MRA's committed flag is true but it is not matched in all responses, a "commit" with this MRA is sent to all replicas. By the reasoning above, 2. is still true regardless how many of them (if any) are received and processed.
  - If the committed MRA matches in all responses (initially or because of commits issued in the last step), then 1. is true for G <= E < F regardless of any further action taken by the coordinator.


Proposition 1 ensures that we can only commit one value in any concurrent operation. Proposition 2 means that any accepted proposal must have started its promise after the previous value was committed to at least one replica in any quorum, and hence must be able to see its effects in its read responses.

## Insubstantial differences with the actual code

Instead of storing an "accepted" proposal with a committed flag, for historical reasons the actual implementation separately stores the latest "accepted" and "committed" values. The coordinator computes most recent values for both after receiving promises, and acts on the higher of the two. In other words, instead of checking the "committed" flag on the most recently accepted, it checks if whether the "committed" value has a higher ballot than the "accepted" one.

When accepting a proposal (which is conditional on having no newer promise or accepted/committed proposal), it stores it in the "accepted" register. When accepting a commit (which is unconditional), it replaces the "commit" register only if the commit has a newer ballot. It will clear the "accepted" value to null if that value does not have a newer ballot. 

Additionally, the "promised" value is not adjusted with accepted proposals and commits. Instead, whenever the code decides whether to promise or accept, it collects the maximum of the promised, accepted and committed ballots.

Version 2 of the Paxos implementation performs reads as described here, by combining them with the "prepare"/"promise" messages. Version 1 runs quorum reads separately after receiving a promise; the read cannot complete if any write reaches consensus after that promise and, if successful, it will in turn invalidate and proposals that it may fail to see.

These differences do not materially change the logic, but make correctness a little harder to prove directly.

## Skipping commit for empty proposals

Since empty proposals make no modifications to the database state, it is not necessary to commit them.

More precisely, in the basic scheme above we can treat the case of an empty partition update as the most-recently accepted value in the coordinator's preparation phase like we would treat a null MRA, skipping phases i and ii. In other words, we can change 4(i) to:

4(i). If the MRA is not null or empty, and its committed flag is not true, there is an in-progress Paxos session that needs to be completed and committed. Controller reproposes the value with the new ballot (i.e. follow the steps below with a proposed value equal to the MRA's) and then restart the process.

With this modified step Proposition 1 is still true, as is Proposition 2 restricted to committing and witnessing non-empty proposals. Their combination still ensures that no update made concurrently with any operation (including a read or a failing CAS) can resurface after that operation is decided, and that any operation will see the results of applying any previous.

## Executing reads concurrently

Since reads are do not modify state, it is safe to execute them concurrently with other reads. To do this we modify the scheme to add a "isWrite" flag to the "prepare" messages, and also record a "promisedWrite" register. The register is updated like "promised" above on promises, acceptances and commits, but not updated when a promise is being made in response for a "prepare" message with a false "isWrite" flag.

