/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;

public class TestVNodeAllocation
{

    private static final double totalTokenRange = Long.MAX_VALUE - (double) Long.MIN_VALUE;
    
    static int nextNodeId = 0;

    private static final class Node implements Comparable<Node>
    {
        int nodeId = nextNodeId++;
        
        public String toString() {
            return Integer.toString(nodeId);
        }

        @Override
        public int compareTo(Node o)
        {
            return Integer.compare(nodeId, o.nodeId);
        }
    }
    
    static class Token implements Comparable<Token>
    {
        long token;

        public Token(long token)
        {
            super();
            this.token = token;
        }

        @Override
        public int compareTo(Token o)
        {
            return Long.compare(token, o.token);
        }

        @Override
        public String toString()
        {
            return String.format("Token[%x]", token);
        }

        @Override
        public int hashCode()
        {
            return Long.hashCode(token);
        }

        @Override
        public boolean equals(Object obj)
        {
            return token == ((Token) obj).token;
        }

        public double size(Token next)
        {
            return next.token - token;  // overflow acceptable and desired
        }

        public Token slice(double slice)
        {
            return new Token(this.token + Math.max(1, Math.round(slice)));  // overflow acceptable and desired
        }
    }
    
    static class Weighted<T extends Comparable<T>> implements Comparable<Weighted<T>> {
        final double weight;
        final T value;

        public Weighted(double weight, T node)
        {
            this.weight = weight;
            this.value = node;
        }

        @Override
        public int compareTo(Weighted<T> o)
        {
            int cmp = Double.compare(o.weight, this.weight);
            if (cmp == 0) {
                cmp = value.compareTo(o.value);
            }
            return cmp;
        }

        @Override
        public String toString()
        {
            return String.format("%s<%s>", value, weight);
        }
    }

    interface ReplicationStrategy {
        /**
         * Returns a list of all replica nodes for given token.
         */
        List<Node> getReplicas(Token token, NavigableMap<Token, Node> sortedTokens);

        /**
         * Returns the token that holds the last replica for the given token.
         */
        Token lastReplicaToken(Token middle, NavigableMap<Token, Node> sortedTokens);

        /**
         * Returns the start of the token span that is replicated in this token.
         * Note: Though this is not trivial to see, the replicated span is always contiguous. A token in the same
         * rack acts as a barrier; if one is not found the token replicates everything up to the replica'th distinct
         * rack seen in front of it.
         */
        Token replicationStart(Token token, Node node, NavigableMap<Token, Node> sortedTokens);

        void addNode(Node n);
        void removeNode(Node n);

        int replicas();
        
        boolean sameRack(Node n1, Node n2);

        
        //void applyToReplicationSpan(Token token, NavigableMap<Token, Node> sortedTokens, Function<Void, Token> func);
    }
    
    static class NoReplicationStrategy implements ReplicationStrategy
    {
        public List<Node> getReplicas(Token token, NavigableMap<Token, Node> sortedTokens)
        {
            return Collections.singletonList(sortedTokens.floorEntry(token).getValue());
        }

        public Token lastReplicaToken(Token token, NavigableMap<Token, Node> sortedTokens)
        {
            return sortedTokens.floorEntry(token).getKey();
        }

        public Token replicationStart(Token token, Node node, NavigableMap<Token, Node> sortedTokens)
        {
            return token;
        }

        public String toString()
        {
            return "No replication";
        }
        
        public void addNode(Node n) {}
        public void removeNode(Node n) {}

        public int replicas()
        {
            return 1;
        }

        public boolean sameRack(Node n1, Node n2)
        {
            return false;
        }
    }
    
    static class SimpleReplicationStrategy implements ReplicationStrategy
    {
        int replicas;

        public SimpleReplicationStrategy(int replicas)
        {
            super();
            this.replicas = replicas;
        }

        public List<Node> getReplicas(Token token, NavigableMap<Token, Node> sortedTokens)
        {
            List<Node> endpoints = new ArrayList<Node>(replicas);

            token = sortedTokens.floorKey(token);
            if (token == null)
                token = sortedTokens.lastKey();
            Iterator<Node> iter = Iterables.concat(sortedTokens.tailMap(token, true).values(), sortedTokens.values()).iterator();
            while (endpoints.size() < replicas)
            {
                // presumably list can't be exhausted before finding all replicas.
                Node ep = iter.next();
                if (!endpoints.contains(ep))
                    endpoints.add(ep);
            }
            return endpoints;
        }

        public Token lastReplicaToken(Token token, NavigableMap<Token, Node> sortedTokens)
        {
            Set<Node> endpoints = new HashSet<Node>(replicas);

            token = sortedTokens.floorKey(token);
            if (token == null)
                token = sortedTokens.lastKey();
            for (Map.Entry<Token, Node> en :
                Iterables.concat(sortedTokens.tailMap(token, true).entrySet(),
                                 sortedTokens.entrySet()))
            {
                Node ep = en.getValue();
                if (!endpoints.contains(ep)){
                    endpoints.add(ep);
                    if (endpoints.size() >= replicas)
                        return en.getKey();
                }
            }
            return token;
        }

        public Token replicationStart(Token token, Node node, NavigableMap<Token, Node> sortedTokens)
        {
            Set<Node> seenNodes = Sets.newHashSet();
            int nodesFound = 0;

            for (Map.Entry<Token, Node> en : Iterables.concat(
                     sortedTokens.headMap(token, false).descendingMap().entrySet(),
                     sortedTokens.descendingMap().entrySet())) {
                Node n = en.getValue();
                // Same rack as investigated node is a break; anything that could replicate in it replicates there.
                if (n == node)
                    break;

                if (seenNodes.add(n))
                {
                    if (++nodesFound == replicas)
                        break;
                }
                token = en.getKey();
            }
            return token;
        }

        public void addNode(Node n) {}
        public void removeNode(Node n) {}

        public String toString()
        {
            return String.format("Simple %d replicas", replicas);
        }

        public int replicas()
        {
            return replicas;
        }

        public boolean sameRack(Node n1, Node n2)
        {
            return false;
        }
    }
    
    static abstract class RackReplicationStrategy implements ReplicationStrategy
    {
        final int replicas;
        final Map<Node, Integer> rackMap;

        public RackReplicationStrategy(int replicas)
        {
            this.replicas = replicas;
            this.rackMap = Maps.newHashMap();
        }

        public List<Node> getReplicas(Token token, NavigableMap<Token, Node> sortedTokens)
        {
            List<Node> endpoints = new ArrayList<Node>(replicas);
            BitSet usedRacks = new BitSet();

            if (sortedTokens.isEmpty())
                return endpoints;

            token = sortedTokens.floorKey(token);
            if (token == null)
                token = sortedTokens.lastKey();
            Iterator<Node> iter = Iterables.concat(sortedTokens.tailMap(token, true).values(), sortedTokens.values()).iterator();
            while (endpoints.size() < replicas)
            {
                // presumably list can't be exhausted before finding all replicas.
                Node ep = iter.next();
                int rack = rackMap.get(ep);
                if (!usedRacks.get(rack))
                {
                    endpoints.add(ep);
                    usedRacks.set(rack);
                }
            }
            return endpoints;
        }

        public Token lastReplicaToken(Token token, NavigableMap<Token, Node> sortedTokens)
        {
            BitSet usedRacks = new BitSet();
            int racksFound = 0;

            token = sortedTokens.floorKey(token);
            if (token == null)
                token = sortedTokens.lastKey();
            for (Map.Entry<Token, Node> en :
                Iterables.concat(sortedTokens.tailMap(token, true).entrySet(),
                                 sortedTokens.entrySet()))
            {
                Node ep = en.getValue();
                int rack = rackMap.get(ep);
                if (!usedRacks.get(rack)){
                    usedRacks.set(rack);
                    if (++racksFound >= replicas)
                        return en.getKey();
                }
            }
            return token;
        }

        public Token replicationStart(Token token, Node node, NavigableMap<Token, Node> sortedTokens)
        {
            // replicated ownership
            int nodeRack = rackMap.get(node);   // node must be already added
            BitSet seenRacks = new BitSet();
            int racksFound = 0;

            for (Map.Entry<Token, Node> en : Iterables.concat(
                     sortedTokens.headMap(token, false).descendingMap().entrySet(),
                     sortedTokens.descendingMap().entrySet())) {
                Node n = en.getValue();
                int nrack = rackMap.get(n);
                // Same rack as investigated node is a break; anything that could replicate in it replicates there.
                if (nrack == nodeRack)
                    break;

                if (!seenRacks.get(nrack))
                {
                    if (++racksFound == replicas)
                        break;
                    seenRacks.set(nrack);
                }
                token = en.getKey();
            }
            return token;
        }

        public String toString()
        {
            Map<Integer, Integer> idToSize = instanceToCount(rackMap);
            Map<Integer, Integer> sizeToCount = Maps.newTreeMap();
            sizeToCount.putAll(instanceToCount(idToSize));
            return String.format("Rack strategy, %d replicas, rack size to count %s", replicas, sizeToCount);
        }

        @Override
        public int replicas()
        {
            return replicas;
        }

        @Override
        public boolean sameRack(Node n1, Node n2)
        {
            return rackMap.get(n1).equals(rackMap.get(n2));
        }

        public void removeNode(Node n) {
            rackMap.remove(n);
        }
    }
    
    static<T> Map<T, Integer> instanceToCount(Map<?, T> map)
    {
        Map<T, Integer> idToCount = Maps.newHashMap();
        for (Map.Entry<?, T> en : map.entrySet()) {
            Integer old = idToCount.get(en.getValue());
            idToCount.put(en.getValue(), old != null ? old + 1 : 1);
        }
        return idToCount;
    }

    static class FixedRackCountReplicationStrategy extends RackReplicationStrategy
    {
        int rackId;
        int rackCount;

        public FixedRackCountReplicationStrategy(int replicas, int rackCount, Collection<Node> nodes)
        {
            super(replicas);
            assert rackCount >= replicas;
            rackId = 0;
            this.rackCount = rackCount;
            for (Node n : nodes)
                addNode(n);
        }

        public void addNode(Node n)
        {
            rackMap.put(n, rackId++ % rackCount);
        }
    }

    static class BalancedRackReplicationStrategy extends RackReplicationStrategy
    {
        int rackId;
        int rackSize;

        public BalancedRackReplicationStrategy(int replicas, int rackSize, Collection<Node> nodes)
        {
            super(replicas);
            assert nodes.size() >= rackSize * replicas;
            rackId = 0;
            this.rackSize = rackSize;
            for (Node n : nodes)
                addNode(n);
        }

        public void addNode(Node n)
        {
            rackMap.put(n, rackId++ / rackSize);
        }
    }
    
    static class UnbalancedRackReplicationStrategy extends RackReplicationStrategy
    {
        int rackId;
        int nextSize;
        int num;
        int minRackSize;
        int maxRackSize;
        
        public UnbalancedRackReplicationStrategy(int replicas, int minRackSize, int maxRackSize, Collection<Node> nodes)
        {
            super(replicas);
            assert nodes.size() >= maxRackSize * replicas;
            rackId = -1;
            nextSize = 0;
            num = 0;
            this.maxRackSize = maxRackSize;
            this.minRackSize = minRackSize;
            
            for (Node n : nodes)
                addNode(n);
        }

        public void addNode(Node n)
        {
            if (++num > nextSize) {
                nextSize = minRackSize + ThreadLocalRandom.current().nextInt(maxRackSize - minRackSize + 1);
                ++rackId;
                num = 0;
            }
            rackMap.put(n, rackId);
        }
    }
    
    
    static class TokenDistributor {
        
        NavigableMap<Token, Node> sortedTokens;
        ReplicationStrategy strategy;
        int perNodeCount;
        
        public TokenDistributor(NavigableMap<Token, Node> sortedTokens, ReplicationStrategy strategy, int perNodeCount)
        {
            this.sortedTokens = new TreeMap<>(sortedTokens);
            this.strategy = strategy;
            this.perNodeCount = perNodeCount;
        }
        
        void addNode(Node newNode)
        {
            throw new AssertionError("Don't call");
        }

        Map<Node, Double> evaluateReplicatedOwnership()
        {
            Map<Node, Double> ownership = Maps.newHashMap();
            Iterator<Token> it = sortedTokens.keySet().iterator();
            Token current = it.next();
            while (it.hasNext()) {
                Token next = it.next();
                addOwnership(current, next, ownership);
                current = next;
            }
            addOwnership(current, sortedTokens.firstKey(), ownership);
            
            // verify ownership
            assert verifyOwnership(ownership);
            
            return ownership;
        }

        private boolean verifyOwnership(Map<Node, Double> ownership)
        {
            for (Map.Entry<Node, Double> en : ownership.entrySet())
            {
                Node n = en.getKey();
                double owns = sortedTokens.entrySet().stream().filter(tn -> tn.getValue() == n).map(Map.Entry::getKey).
                        mapToDouble(this::replicatedTokenOwnership).sum();
                if (Math.abs(owns - en.getValue()) > totalTokenRange * 1e-14)
                {
                    System.out.format("Node %s expected %f got %f\n%s\n%s\n",
                                       n, owns, en.getValue(),
                                       ImmutableList.copyOf(sortedTokens.entrySet().stream().filter(tn -> tn.getValue() == n).map(Map.Entry::getKey).iterator()),
                                       ImmutableList.copyOf(sortedTokens.entrySet().stream().filter(tn -> tn.getValue() == n).map(Map.Entry::getKey).mapToDouble(this::replicatedTokenOwnership).iterator())
                                       );
                    return false;
                }
            }
            return true;
        }

        private void addOwnership(Token current, Token next, Map<Node, Double> ownership)
        {
            double size = current.size(next);
            for (Node n : strategy.getReplicas(current, sortedTokens)) {
                Double v = ownership.get(n);
                ownership.put(n, v != null ? v + size : size);
            }
        }
        
        double tokenSize(Token token)
        {
            return token.size(next(token));
        }

        public Token next(Token token)
        {
            Token next = sortedTokens.higherKey(token);
            if (next == null)
                next = sortedTokens.firstKey();
            return next;
        }
        
        double replicatedTokenOwnership(Token token)
        {
            return strategy.replicationStart(token, sortedTokens.get(token), sortedTokens).size(next(token));
        }

        public int nodeCount()
        {
            return (int) sortedTokens.values().stream().distinct().count();
        }
        
        public Map.Entry<Token, Node> mapEntryFor(Token t)
        {
            Map.Entry<Token, Node> en = sortedTokens.floorEntry(t);
            if (en == null)
                en = sortedTokens.lastEntry();
            return en;
        }
        
        private Node nodeFor(Token t)
        {
            return mapEntryFor(t).getValue();
        }

        public void removeNode(Token t)
        {
            removeNode(nodeFor(t));
        }

        public void removeNode(Node n)
        {
            sortedTokens.entrySet().removeIf(en -> en.getValue() == n);
        }
    }
    
    static class NoReplicationTokenDistributor extends TokenDistributor {
        PriorityQueue<Weighted<Node>> sortedNodes;
        Map<Node, PriorityQueue<Weighted<Token>>> tokensInNode;

        public NoReplicationTokenDistributor(NavigableMap<Token, Node> sortedTokens, int perNodeCount)
        {
            super(sortedTokens, new NoReplicationStrategy(), perNodeCount);
            populateMaps();
        }

        private void populateMaps()
        {
            tokensInNode = Maps.newHashMap();
            for (Map.Entry<Token, Node> en : sortedTokens.entrySet()) {
                Token token = en.getKey();
                Node node = en.getValue();
                addWeightedToken(node, token);
            }
            sortedNodes = Queues.newPriorityQueue();
            tokensInNode.forEach(this::addToSortedNodes);
        }

        private boolean addToSortedNodes(Node node, Collection<Weighted<Token>> set)
        {
            return sortedNodes.add(new Weighted<Node>(set.stream().mapToDouble(wt -> wt.weight).sum(), node));
        }

        private void addWeightedToken(Node node, Token token)
        {
            PriorityQueue<Weighted<Token>> nodeTokens = tokensInNode.get(node);
            if (nodeTokens == null) {
                nodeTokens = Queues.newPriorityQueue();
                tokensInNode.put(node, nodeTokens);
            }
            Weighted<Token> wt = new Weighted<Token>(tokenSize(token), token);
            nodeTokens.add(wt);
        }

        void addNode(Node newNode)
        {
            assert !sortedNodes.isEmpty();
            List<Weighted<Node>> nodes = Lists.newArrayListWithCapacity(perNodeCount);
            double targetAverage = 0;
            double sum = 0;
            int count;
            // Select the nodes we will work with, extract them from sortedNodes and calculate targetAverage.
            for (count = 0; count < perNodeCount; ++count)
            {
                Weighted<Node> wn = sortedNodes.peek();
                if (wn == null)
                    break;
                double average = (sum + wn.weight) / (count + 2); // wn.node and newNode must be counted.
                if (wn.weight <= average)
                    break;  // No point to include later nodes, target can only decrease from here.

                // Node will be used.
                sum += wn.weight;
                targetAverage = average;
                sortedNodes.remove();
                nodes.add(wn);
            }
            
            int nr = 0;
            for (Weighted<Node> n: nodes) {
                // TODO: Any better ways to assign how many tokens to change in each node?
                int tokensToChange = perNodeCount / count + (nr < perNodeCount % count ? 1 : 0);
                
                Queue<Weighted<Token>> nodeTokens = tokensInNode.get(n.value);
                List<Weighted<Token>> tokens = Lists.newArrayListWithCapacity(tokensToChange);
                double workWeight = 0;
                // Extract biggest vnodes and calculate how much weight we can work with.
                for (int i=0; i < tokensToChange; ++i) {
                    Weighted<Token> wt = nodeTokens.remove();
                    tokens.add(wt);
                    workWeight += wt.weight;
                }

                double toTakeOver = n.weight - targetAverage;
                // Split toTakeOver proportionally between the vnodes.
                for (Weighted<Token> wt : tokens)
                {
                    double slice;
                    // TODO: Experiment with limiting the fraction we can take over. Having empty/singleton token ranges
                    // doesn't help anyone.
                    if (toTakeOver < workWeight) {
                        // Spread decrease.
                        slice = wt.weight - (toTakeOver * wt.weight / workWeight);
                    } else {
                        // Effectively take over spans, best we can do.
                        slice = 0;
                    }
                    Token t = wt.value.slice(slice);

                    // Token selected. Now change all data.
                    sortedTokens.put(t, newNode);
                    // This changes nodeTokens.
                    addWeightedToken(n.value, wt.value);
                    addWeightedToken(newNode, t);
                }
                
                addToSortedNodes(n.value, nodeTokens);
                ++nr;
            }
            assert perNodeCount == tokensInNode.get(newNode).size();
            addToSortedNodes(newNode, tokensInNode.get(newNode));
//            System.out.println("Nodes after add: " + sortedNodes);
        }

        @Override
        public int nodeCount()
        {
            return sortedNodes.size();
        }

        @Override
        public void removeNode(Node n)
        {
            super.removeNode(n);
//            populateMaps();
            sortedNodes.removeIf(wn -> wn.value == n);
            Collection<Weighted<Token>> tokens = tokensInNode.remove(n);
            for (Weighted<Token> wt : tokens)
            {
                Map.Entry<Token, Node> prev = mapEntryFor(wt.value);
                Node pn = prev.getValue();
                Token pt = prev.getKey();
                PriorityQueue<Weighted<Token>> nodeTokens = tokensInNode.get(pn);
                boolean removed = nodeTokens.removeIf(rwt -> rwt.value == pt);
                assert removed;
                addWeightedToken(pn, pt);
                sortedNodes.removeIf(wn -> wn.value == pn);
                addToSortedNodes(pn, nodeTokens);
            }
        }
    }
    
    static class ReplicationAwareTokenDistributor extends TokenDistributor
    {
        Map<Token, Token> replicationStart = Maps.newHashMap();
        Map<Node, Double> ownership = Maps.newHashMap();
        Map<Node, List<Token>> tokensInNode = Maps.newHashMap();
        
        public ReplicationAwareTokenDistributor(NavigableMap<Token, Node> sortedTokens, ReplicationStrategy strategy,
                int perNodeCount)
        {
            super(sortedTokens, strategy, perNodeCount);
            for (Map.Entry<Token, Node> en : sortedTokens.entrySet())
            {
                Node n = en.getValue();
                Token t = en.getKey();
                List<Token> nodeTokens = tokensInNode.get(n);
                if (nodeTokens == null)
                {
                    nodeTokens = Lists.newArrayListWithCapacity(perNodeCount);
                    tokensInNode.put(n, nodeTokens);
                    ownership.put(n, 0.0);
                }
                nodeTokens.add(t);
                Token rs = strategy.replicationStart(t, n, sortedTokens);
                replicationStart.put(t, rs);
                ownership.put(n, ownership.get(n) + rs.size(next(t)));
            }
        }

        @Override
        void addNode(Node newNode)
        {
            strategy.addNode(newNode);
            tokensInNode.put(newNode, Lists.newArrayListWithCapacity(perNodeCount));
            ownership.put(newNode, 0.0);
            NavigableMap<Token, Node> sortedTokensCopy = Maps.newTreeMap(sortedTokens);
            
            for (int vn = 0; vn < perNodeCount; ++vn)
            {
                NavigableMap<Double, Token> improvements = Maps.newTreeMap();
                for (Token t : sortedTokens.keySet())
                {
                    // TODO: Can we do better than just picking middle?
                    Token middle = t.slice(tokenSize(t) / 2);
                    improvements.put(evaluateImprovement(middle, newNode, sortedTokensCopy), middle);
                    // Try also 3/4 and 1/4.
    //                middle = t.slice(tokenSize(t) / 4);
    //                improvements.put(evaluateImprovement(middle, newNode), middle);
    //                middle = t.slice(tokenSize(t) * 3 / 4);
    //                improvements.put(evaluateImprovement(middle, newNode), middle);
                }

                // Greedy scheme. Pick best.
                Token middle = improvements.lastEntry().getValue();
                //adjustData(tt, newNode);
                sortedTokensCopy.put(middle, newNode);
                Token lastReplica = strategy.lastReplicaToken(middle, sortedTokens);
                for (Map.Entry<Token, Node> en : Iterables.concat(sortedTokens.tailMap(middle, false).entrySet(),
                                                                  sortedTokens.entrySet()))
                {
                    Token t = en.getKey();
                    Node n = en.getValue();
                    Token rs = strategy.replicationStart(t, n, sortedTokensCopy);
                    Token rsOld = replicationStart.get(t);
                    double oldOwnership = ownership.get(n);
                    double newOwnership = oldOwnership + rs.size(t) - rsOld.size(t);
                    ownership.put(n, newOwnership);
                    replicationStart.put(t, rs);
                    
                    if (t == lastReplica)
                        break;
                }

                // Adjust sliced token.
                Entry<Token, Node> en = mapEntryFor(middle);
                Token t = en.getKey();
                Node n = en.getValue();
                double oldOwnership = ownership.get(n);
                double newOwnership = oldOwnership + t.size(middle) - t.size(next(middle));
                ownership.put(n, newOwnership);

                // TODO: Control ownership of newNode.
                sortedTokens.put(middle, newNode);
                tokensInNode.get(newNode).add(middle);
                Token rs = strategy.replicationStart(middle, newNode, sortedTokensCopy);
                replicationStart.put(middle, rs);
                ownership.put(newNode, ownership.get(newNode) + rs.size(next(middle)));
            }
            System.out.println(ownership);
        }
        
        // Returns a measure for the improvement that inserting this token will yield. Higher is better.
        // The exact measure used currently is decrease in std deviation.
        double evaluateImprovement(Token middle, Node newNode, NavigableMap<Token, Node> sortedTokensCopy)
        {
            // First, check who's affected by split.
            Token lastReplica = strategy.lastReplicaToken(middle, sortedTokens);
            
            double improvement = 0;
            sortedTokensCopy.put(middle, newNode);
            double optimalOwnership = totalTokenRange * strategy.replicas() / (nodeCount() + 1);
            for (Map.Entry<Token, Node> en : Iterables.concat(sortedTokens.tailMap(middle, false).entrySet(),
                                                              sortedTokens.entrySet()))
            {
                Token t = en.getKey();
                Node n = en.getValue();
                Token rs = strategy.replicationStart(t, n, sortedTokensCopy);
                Token rsOld = replicationStart.get(t);
                double oldOwnership = ownership.get(n);
                double newOwnership = oldOwnership + rs.size(t) - rsOld.size(t);
                improvement += sq(oldOwnership - optimalOwnership) - sq(newOwnership - optimalOwnership);
                
                if (t == lastReplica)
                    break;
            }
            sortedTokensCopy.remove(middle);
            
            // Also calculate change to currently owning token.
            Entry<Token, Node> en = mapEntryFor(middle);
            Token t = en.getKey();
            double oldOwnership = ownership.get(en.getValue());
            double newOwnership = oldOwnership + t.size(middle) - t.size(next(middle));
            improvement += sq(oldOwnership - optimalOwnership) - sq(newOwnership - optimalOwnership);
            return improvement;
        }

        @Override
        public void removeNode(Node n)
        {
            super.removeNode(n);
            strategy.removeNode(n);
            ownership.remove(n);
            // more
        }
        
    }
    
    private static void oneNodePerfectDistribution(Map<Token, Node> map, int perNodeCount)
    {
        System.out.println("\nOne node init.");
        Node node = new Node();
        long inc = - (Long.MIN_VALUE / (perNodeCount / 2));
        long start = Long.MIN_VALUE;
        for (int i = 0 ; i < perNodeCount ; i++)
        {
            map.put(new Token(start), node);
            start += inc;
        }
    }

    public static double sq(double d)
    {
        return d*d;
    }

    private static void random(Map<Token, Node> map, int nodeCount, int perNodeCount, boolean localRandom)
    {
        System.out.format("\nRandom generation of %d nodes with %d tokens each%s\n", nodeCount, perNodeCount, (localRandom ? ", locally random" : ""));
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        long inc = -(Long.MIN_VALUE / (perNodeCount / 2));
        for (int i = 0 ; i < nodeCount ; i++)
        {
            Node node = new Node();
            for (int j = 0 ; j < perNodeCount ; j++)
            {
                long nextToken;
                if (localRandom) nextToken = Long.MIN_VALUE + j * inc + rand.nextLong(inc);
                else nextToken = rand.nextLong();
                map.put(new Token(nextToken), node);
            }
        }
    }

    private static void printDistribution(TokenDistributor t)
    {
        Map<Node, Double> ownership = t.evaluateReplicatedOwnership();
        int size = t.nodeCount();
        double inverseAverage = size / (totalTokenRange * t.strategy.replicas());
        System.out.format("Size %d   max %.2f min %.2f   %s\n",
                          size,
                          ownership.values().stream().mapToDouble(x->x).max().getAsDouble() * inverseAverage,
                          ownership.values().stream().mapToDouble(x->x).min().getAsDouble() * inverseAverage,
                          t.strategy);
    }

    private static void printDistribution(TokenDistributor t, ReplicationStrategy rs)
    {
        printDistribution(new TokenDistributor(t.sortedTokens, rs, t.perNodeCount));
    }

    public static void main(String[] args)
    {
        final int targetClusterSize = 10;
        int perNodeCount = 4;
        NavigableMap<Token, Node> tokenMap = Maps.newTreeMap();

        boolean locallyRandom = false;
        random(tokenMap, targetClusterSize, perNodeCount, locallyRandom);

        TokenDistributor t = new ReplicationAwareTokenDistributor(tokenMap, new SimpleReplicationStrategy(3), perNodeCount);
        test(t, targetClusterSize);

        test(t, targetClusterSize + 1);

        test(t, targetClusterSize + 2);

        test(t, targetClusterSize + 3);

        test(t, targetClusterSize + 4);

        test(t, targetClusterSize + 5);

        
//        TokenDistributor t = new NoReplicationTokenDistributor(tokenMap, perNodeCount);
//        test(t, targetClusterSize);
//
//        test(t, targetClusterSize + 1);
//
//        test(t, targetClusterSize * 101 / 100);
//
//        test(t, targetClusterSize * 26 / 25);
//
//        test(t, targetClusterSize * 5 / 4);
//
//        tokenMap.clear();
//        oneNodePerfectDistribution(tokenMap, perNodeCount);
//        t = new NoReplicationTokenDistributor(tokenMap, perNodeCount);
//        test(t, targetClusterSize);
//        testLoseAndReplace(t, 1);
//        testLoseAndReplace(t, targetClusterSize / 100);
//        testLoseAndReplace(t, targetClusterSize / 25);
//        testLoseAndReplace(t, targetClusterSize / 4);
    }

    private static void testLoseAndReplace(TokenDistributor t, int howMany)
    {
        int fullCount = t.nodeCount();
        System.out.format("Losing %d nodes\n", howMany);
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        for (int i=0; i<howMany; ++i)
            t.removeNode(new Token(rand.nextLong()));
        test(t, t.nodeCount());
        
        test(t, fullCount);
    }

    public static void test(TokenDistributor t, int targetClusterSize)
    {
        int size = t.nodeCount();
        if (size < targetClusterSize)
            System.out.format("Adding %d node(s) using %s\n", targetClusterSize - size, t.getClass().getSimpleName());
        while (size < targetClusterSize)
        {
            t.addNode(new Node());
            ++size;
        }
        printDistribution(t);
        if (!(t.strategy instanceof NoReplicationStrategy))
            printDistribution(t, new NoReplicationStrategy());
        Set<Node> nodes = Sets.newHashSet(t.sortedTokens.values());
        printDistribution(t, new SimpleReplicationStrategy(3));
//        printDistribution(t, new SimpleReplicationStrategy(5));
//        printDistribution(t, new SimpleReplicationStrategy(17));
//        printDistribution(t, new BalancedRackReplicationStrategy(3, 4, nodes));
//        printDistribution(t, new BalancedRackReplicationStrategy(3, 8, nodes));
        printDistribution(t, new FixedRackCountReplicationStrategy(3, 3, nodes));
//        printDistribution(t, new FixedRackCountReplicationStrategy(3, 5, nodes));
//        printDistribution(t, new BalancedRackReplicationStrategy(3, 16, nodes));
//        printDistribution(t, new BalancedRackReplicationStrategy(3, 64, nodes));
//        printDistribution(t, new BalancedRackReplicationStrategy(5, 16, nodes));
//        printDistribution(t, new BalancedRackReplicationStrategy(17, 16, nodes));
//        printDistribution(t, new UnbalancedRackReplicationStrategy(3, 16, 32, nodes));
//        printDistribution(t, new UnbalancedRackReplicationStrategy(3, 8, 16, nodes));
//        printDistribution(t, new UnbalancedRackReplicationStrategy(3, 2, 6, nodes));
    }
}
