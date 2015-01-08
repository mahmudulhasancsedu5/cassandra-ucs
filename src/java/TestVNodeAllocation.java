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
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;

public class TestVNodeAllocation
{

    private static final double totalTokenRange = 1.0 + Long.MAX_VALUE - (double) Long.MIN_VALUE;
    
    static int nextNodeId = 0;

    static final class Node implements Comparable<Node>
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
            return String.format("Token[%016x]", token);
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
            long v = next.token - token;  // overflow acceptable and desired
            return v > 0 ? v : (v + totalTokenRange);
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
        int debug;
        
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

        protected boolean verifyOwnership(Map<Node, Double> ownership)
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

        double replicatedTokenOwnership(Token token, NavigableMap<Token, Node> sortedTokens)
        {
            Token next = sortedTokens.higherKey(token);
            if (next == null)
                next = sortedTokens.firstKey();
            return strategy.replicationStart(token, sortedTokens.get(token), sortedTokens).size(next);
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
            strategy.removeNode(n);
        }
        
        public String toString()
        {
            return getClass().getSimpleName();
        }

        protected double evaluateTokenImprovement(Token middle, Node newNode, NavigableMap<Token, Node> sortedTokensWithNew, double optimalOwnership)
        {
            // First, check who's affected by split.
            sortedTokensWithNew.put(middle, newNode);
            Token lr1 = strategy.lastReplicaToken(middle, sortedTokens);
            Token lr2 = strategy.lastReplicaToken(middle, sortedTokensWithNew);
            Token lastReplica = max(lr1, lr2, middle);
            Iterable<Map.Entry<Token, Node>> affectedTokens = Iterables.concat(sortedTokens.tailMap(middle, false).entrySet(),
                                                              sortedTokens.entrySet());
            
            double improvement = 0;
            for (Map.Entry<Token, Node> en : affectedTokens)
            {
                Token t = en.getKey();
                Node n = en.getValue();
                Token nt = next(t);
                Token rs = strategy.replicationStart(t, n, sortedTokensWithNew);
                Token rsOld = strategy.replicationStart(t, n, sortedTokens);
                improvement += sq(rsOld.size(nt) - optimalOwnership) - sq(rs.size(nt) - optimalOwnership);
                
                if (t == lastReplica)
                    break;
            }
            sortedTokensWithNew.remove(middle);
            
            // Also calculate change to currently owning token.
            Entry<Token, Node> en = mapEntryFor(middle);
            Token t = en.getKey();
            Node n = en.getValue();
            Token nt = next(t);
            Token rs = strategy.replicationStart(t, n, sortedTokens);
            improvement += sq(rs.size(nt) - optimalOwnership) - sq(rs.size(middle) - optimalOwnership);
            
            rs = strategy.replicationStart(middle, newNode, sortedTokens);
            improvement -= sq(rs.size(nt) - optimalOwnership);
            return improvement;
        }

        protected void evaluateNodeChange(Token middle, Node newNode, NavigableMap<Token, Node> sortedTokensWithNew, Map<Node, Double> nodeToWeight)
        {
            // First, check who's affected by split.
            sortedTokensWithNew.put(middle, newNode);
            Token lr1 = strategy.lastReplicaToken(middle, sortedTokens);
            Token lr2 = strategy.lastReplicaToken(middle, sortedTokensWithNew);
            Token lastReplica = max(lr1, lr2, middle);
            // node to new weight
            
            for (Map.Entry<Token, Node> en : Iterables.concat(sortedTokens.tailMap(middle, false).entrySet(),
                                                              sortedTokens.entrySet()))
            {
                Token t = en.getKey();
                Node n = en.getValue();
                Token end = next(t);
                Token rs = strategy.replicationStart(t, n, sortedTokensWithNew);
                Token rsOld = strategy.replicationStart(t, n, sortedTokens);
                addToWeight(nodeToWeight, nodeToWeight, n, rs.size(end) - rsOld.size(end));
        
                if (t == lastReplica)
                    break;
            }
            sortedTokensWithNew.remove(middle);
            
            // Also calculate change to currently owning token.
            Entry<Token, Node> en = mapEntryFor(middle);
            Token t = en.getKey();
            Node n = en.getValue();
            addToWeight(nodeToWeight, nodeToWeight, n, t.size(middle) - t.size(next(middle)));
            
            Token rs = strategy.replicationStart(middle, newNode, sortedTokensWithNew);
            addToWeight(nodeToWeight, nodeToWeight, newNode, rs.size(next(middle)));
        }
        
        protected double evaluateNodeImprovement(Token middle, Node newNode, NavigableMap<Token, Node> sortedTokensWithNew, Map<Node, Double> ownership, Map<Token, Token> replicationStart,
                double optimalOwnership, double assignedTokenShare)
        {
            return evaluateNodeImprovement(middle, newNode, sortedTokensWithNew, ownership, replicationStart, optimalOwnership, assignedTokenShare, Maps.newHashMap());            
        }
        protected double evaluateNodeImprovement(Token middle, Node newNode, NavigableMap<Token, Node> sortedTokensWithNew, Map<Node, Double> ownership, Map<Token, Token> replicationStart,
                double optimalOwnership, double assignedTokenShare, Map<Node, Double> nodeToWeight)
        {
            // First, check who's affected by split.
            sortedTokensWithNew.put(middle, newNode);
            Token lr1 = strategy.lastReplicaToken(middle, sortedTokens);
            Token lr2 = strategy.lastReplicaToken(middle, sortedTokensWithNew);
            Token lastReplica = max(lr1, lr2, middle);
            // node to new weight
            
            for (Map.Entry<Token, Node> en : Iterables.concat(sortedTokens.tailMap(middle, false).entrySet(),
                                                              sortedTokens.entrySet()))
            {
                Token t = en.getKey();
                Node n = en.getValue();
                Token end = next(t);
                Token rs = strategy.replicationStart(t, n, sortedTokensWithNew);
                Token rsOld = replicationStart.get(t);
                addToWeight(nodeToWeight, ownership, n, rs.size(end) - rsOld.size(end));
        
                if (t == lastReplica)
                    break;
            }
            sortedTokensWithNew.remove(middle);
            
            // Also calculate change to currently owning token.
            Entry<Token, Node> en = mapEntryFor(middle);
            Token t = en.getKey();
            Node n = en.getValue();
            addToWeight(nodeToWeight, ownership, n, t.size(middle) - t.size(next(middle)));
            
            Token rs = strategy.replicationStart(middle, newNode, sortedTokensWithNew);
            addToWeight(nodeToWeight, ownership, newNode, rs.size(next(middle)) - assignedTokenShare);
            return nodeToWeight.entrySet().stream().mapToDouble(
                enn -> sq(ownership.get(enn.getKey()) - optimalOwnership) - sq(enn.getValue() - optimalOwnership)).sum();
        }

        protected double evaluateTokenImprovement(Token middle, Node newNode, NavigableMap<Token, Node> sortedTokensWithNew, Map<Token, Token> replicationStart, double optimalOwnership)
        {
            // First, check who's affected by split.
            sortedTokensWithNew.put(middle, newNode);
            Token lr1 = strategy.lastReplicaToken(middle, sortedTokens);
            Token lr2 = strategy.lastReplicaToken(middle, sortedTokensWithNew);
            Token lastReplica = max(lr1, lr2, middle);
            Iterable<Map.Entry<Token, Node>> affectedTokens = Iterables.concat(sortedTokens.tailMap(middle, false).entrySet(),
                                                              sortedTokens.entrySet());
            
            double improvement = 0;
            for (Map.Entry<Token, Node> en : affectedTokens)
            {
                Token t = en.getKey();
                Node n = en.getValue();
                Token nt = next(t);
                Token rs = strategy.replicationStart(t, n, sortedTokensWithNew);
                Token rsOld = replicationStart.get(t);
                improvement += sq(rsOld.size(nt) - optimalOwnership) - sq(rs.size(nt) - optimalOwnership);
                
                if (t == lastReplica)
                    break;
            }
            sortedTokensWithNew.remove(middle);
            
            // Also calculate change to currently owning token.
            Entry<Token, Node> en = mapEntryFor(middle);
            Token t = en.getKey();
            Token nt = next(t);
            Token rs = replicationStart.get(t);
            improvement += sq(rs.size(nt) - optimalOwnership) - sq(rs.size(middle) - optimalOwnership);
            
            rs = strategy.replicationStart(middle, newNode, sortedTokens);
            improvement -= sq(rs.size(nt) - optimalOwnership);
            return improvement;
        }

        
        // nodeMult or tokenMult could be 0, otherwise they could be combined.
        protected double evaluateCombinedImprovement(Token middle, Node newNode,
                NavigableMap<Token, Node> sortedTokensWithNew, Map<Node, Double> ownership, Map<Token, Token> replicationStart,
                double optimalOwnership, double assignedTokenShare, double nodeMult, double newNodeMult, double tokenMult, double nonReplicatedTokenMult)
        {
            // First, check who's affected by split.
            sortedTokensWithNew.put(middle, newNode);
            Token lr1 = strategy.lastReplicaToken(middle, sortedTokens);
            Token lr2 = strategy.lastReplicaToken(middle, sortedTokensWithNew);
            Token lastReplica = max(lr1, lr2, middle);
            // node to new weight
            Map<Node, Double> nodeToWeight = Maps.newHashMap();
            
            double tokenImprovement = 0;
            for (Map.Entry<Token, Node> en : Iterables.concat(sortedTokens.tailMap(middle, false).entrySet(),
                                                              sortedTokens.entrySet()))
            {
                Token token = en.getKey();
                Node node = en.getValue();
                Token nextToken = next(token);
                Token rsNew = strategy.replicationStart(token, node, sortedTokensWithNew);
                Token rsOld = replicationStart.get(token);
                double osz = rsOld.size(nextToken);
                double nsz = rsNew.size(nextToken);
                tokenImprovement += sq(osz - assignedTokenShare) - sq(nsz - assignedTokenShare);
                addToWeight(nodeToWeight, ownership, node, nsz - osz);
        
                if (token == lastReplica)
                    break;
            }
            sortedTokensWithNew.remove(middle);
            
            // Also calculate change to currently owning token.
            Entry<Token, Node> en = mapEntryFor(middle);
            Token token = en.getKey();
            Node node = en.getValue();
            Token nextToken = next(token);
            Token rsOld = replicationStart.get(token);
            double osz = rsOld.size(nextToken);
            double nsz = rsOld.size(middle);
            tokenImprovement += sq(osz - assignedTokenShare) - sq(nsz - assignedTokenShare);
            addToWeight(nodeToWeight, ownership, node, nsz - osz);
            double nonReplOptimal = assignedTokenShare / strategy.replicas();
            double nonRepl = sq(token.size(nextToken) - nonReplOptimal) - sq(token.size(middle) - nonReplOptimal);
            
            Token rsNew = strategy.replicationStart(middle, newNode, sortedTokensWithNew);
            osz = assignedTokenShare;
            nsz = rsNew.size(nextToken);
            tokenImprovement += sq(osz - assignedTokenShare) - sq(nsz - assignedTokenShare);
            addToWeight(nodeToWeight, ownership, newNode, nsz - osz);
            nonRepl -= sq(middle.size(nextToken) - nonReplOptimal);

            double nodeImprovement = nodeToWeight.entrySet().stream().mapToDouble(
                enn -> (sq(ownership.get(enn.getKey()) - optimalOwnership) - sq(enn.getValue() - optimalOwnership)) * (enn.getKey() == newNode ? newNodeMult : nodeMult)).sum();

            return nodeImprovement + tokenImprovement * tokenMult + nonRepl * nonReplicatedTokenMult;
        }

        public void addToWeight(Map<Node, Double> nodeToWeight, Map<Node, Double> fallback, Node n, double weightChange)
        {
            Double v = nodeToWeight.get(n);
            if (v == null)
                v = fallback.get(n);
            if (v == null) {
                v = 0.0;
                fallback.put(n, v);
            }
            nodeToWeight.put(n, v + weightChange);
        }

        public void printChangeStat(Token middle, Node newNode, NavigableMap<Token, Node> sortedTokensWithNew, double nopt)
        {
            double topt = nopt/perNodeCount;
            sortedTokensWithNew.put(middle, newNode);
            StringBuilder afft = new StringBuilder();
            Token lr1 = strategy.lastReplicaToken(middle, sortedTokens);
            Token lr2 = strategy.lastReplicaToken(middle, sortedTokensWithNew);
            Token lastReplica = max(lr1, lr2, middle);
            for (Token t = mapEntryFor(middle).getKey(); ; t = next(t)) {
                double oldd = replicatedTokenOwnership(t, sortedTokens);
                double neww = replicatedTokenOwnership(t, sortedTokensWithNew);
                afft.append(String.format("%s:%.3f->%.3f(%.5f),",
                                          t,
                                          oldd / topt,
                                          neww / topt,
                                          sq(oldd / topt - 1) - sq(neww / topt - 1)));
                if (t == lastReplica)
                    break;
            }
            double neww = replicatedTokenOwnership(middle, sortedTokensWithNew);
            System.out.format("change token %s->%.3f(%.5f) affects %s\n",
                              middle, neww / topt, -sq(neww / topt - 1),
                              afft);
            sortedTokensWithNew.remove(middle);

            double improv = evaluateTokenImprovement(middle, newNode, sortedTokensWithNew, topt);
            List<Double> tokenOwnership = Lists.newArrayList(sortedTokens.keySet().stream().mapToDouble(this::replicatedTokenOwnership).iterator());
            double dev = tokenOwnership.stream().mapToDouble(x -> sq(x / topt - 1)).sum();
            DoubleSummaryStatistics stat = tokenOwnership.stream().mapToDouble(x -> x / topt).summaryStatistics();
            System.out.format(" expected token impr %.5f on %.5f max %.2f min %.2f\n", improv / sq(topt), dev, stat.getMax(), stat.getMin());
            
            Map<Node, Double> ownership = evaluateReplicatedOwnership();
            int newNodeCount = (int) sortedTokens.values().stream().filter(v -> v == newNode).count();
            Double v = ownership.get(newNode);
            if (v == null) v = 0.0;
            ownership.put(newNode, v + (perNodeCount - newNodeCount) * topt);
            Map<Token, Token> replicationStart = Maps.newHashMap();
            for (Map.Entry<Token, Node> en : sortedTokens.entrySet())
            {
                replicationStart.put(en.getKey(), strategy.replicationStart(en.getKey(), en.getValue(), sortedTokens));
            }
            Map<Node, Double> nodeToWeight = Maps.newHashMap();
            improv = evaluateNodeImprovement(middle, newNode, sortedTokensWithNew, ownership, replicationStart, nopt, topt, nodeToWeight);
            System.out.print(" affects ");
            for (Map.Entry<Node, Double> en: nodeToWeight.entrySet())
            {
                Node n = en.getKey();
                neww = en.getValue();
                double oldd = ownership.get(n);
                System.out.format("node %s:%.3f->%.3f(%.5f),",
                                  n,
                                  oldd / nopt,
                                  neww / nopt,
                                  sq(oldd / nopt - 1) - sq(neww / nopt - 1));
            }
            dev = ownership.values().stream().mapToDouble(x -> sq(x / nopt - 1)).sum();
            stat = ownership.values().stream().mapToDouble(x -> x / nopt).summaryStatistics();
            System.out.format("\n expected node impr %.5f on %.5f max %.2f min %.2f\n", improv / sq(nopt), dev, stat.getMax(), stat.getMin());
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
    
    static class ReplicationAwareTokenDistributorRec extends ReplicationAwareTokenDistributor
    {
        public ReplicationAwareTokenDistributorRec(NavigableMap<Token, Node> sortedTokens, ReplicationStrategy strategy,
                int perNodeCount)
        {
            super(sortedTokens, strategy, perNodeCount);
            bestSelection = new Token[perNodeCount];
            current = new Token[perNodeCount];
            choices = Lists.newArrayList();
        }
        
        double bestImprov;
        Token[] bestSelection;
        Token[] current;
        List<Token> choices;

        @Override
        void addNode(Node newNode)
        {
            ++nodeCount;
            ownership = evaluateReplicatedOwnership();  // FIXME
            strategy.addNode(newNode);
            optimalOwnership = totalTokenRange * strategy.replicas() / (nodeCount() + 1);
            sortedTokensWithNew = Maps.newTreeMap(sortedTokens);
            ownership.put(newNode, 0.0);
            bestImprov = Double.NEGATIVE_INFINITY;
            choices.clear();
            for (Token t : sortedTokens.keySet())
            {
                int c = 2;
                for (int i=1; i<c; ++i)
                {
                    Token middle = t.slice(tokenSize(t)*i / c);
                    choices.add(middle);
                }
            }

            addNodeRecursive(newNode, 0, 0, 0.0);
            for (Token t: bestSelection)
            {
                if (debug > 1)
                    printChangeStat(t, newNode, sortedTokensWithNew, optimalOwnership);
                sortedTokens.put(t, newNode);
            }
            sortedTokensWithNew = null;
        }
            

        void addNodeRecursive(Node newNode, int vn, int firstChoice, double improv)
        {
            if (vn == perNodeCount)
            {
                double newNodeSize = ownership.get(newNode);
                improv -= sq(newNodeSize - optimalOwnership);
                if (improv > bestImprov) {
                    bestImprov = improv;
                    System.arraycopy(current, 0, bestSelection, 0, vn);
                }
                return;
            }

            for (int i=firstChoice; i<choices.size(); ++i)
            {
                Token middle = choices.get(i);
//                printChangeStat(middle, newNode, sortedTokensWithNew, optimalOwnership);
                Map<Node, Double> ownershipChange = Maps.newHashMap();
                evaluateNodeChange(middle, newNode, sortedTokensWithNew, ownershipChange);
                double impr = 0;
                for (Map.Entry<Node, Double> en: ownershipChange.entrySet())
                {
                    Node n = en.getKey();
                    if (n == newNode)
                        continue;
                    double ov = ownership.get(n);
                    double nv = en.getValue() + ov;
                    impr += sq(ov - optimalOwnership) - sq(nv - optimalOwnership);
                }
                // trim
                if (impr < 0)
                    continue;
                
                for (Map.Entry<Node, Double> en: ownershipChange.entrySet())
                {
                    Node n = en.getKey();
                    double ov = ownership.get(n);
                    double nv = en.getValue() + ov;
                    ownership.put(n, nv);
                }
                sortedTokens.put(middle, newNode);
                sortedTokensWithNew.put(middle, newNode);
                current[vn] = middle;
                // recurse
                addNodeRecursive(newNode, vn + 1, i + 1, improv + impr);
                
                sortedTokens.remove(middle);
                sortedTokensWithNew.remove(middle);
                for (Map.Entry<Node, Double> en: ownershipChange.entrySet())
                {
                    Node n = en.getKey();
                    double ov = ownership.get(n);
                    double nv = ov - en.getValue();
                    ownership.put(n, nv);
                }
            }
            
            
         }

    }
    

    static class GrowingWeightTokenDistributor extends ConfigurableTokenDistributor
    {
        public GrowingWeightTokenDistributor(NavigableMap<Token, Node> sortedTokens, ReplicationStrategy strategy,
                int perNodeCount, double nodeWeight, double tokenWeight, double nonReplWeight)
        {
            super(sortedTokens, strategy, perNodeCount, nodeWeight, tokenWeight, nonReplWeight);
        }

//        @Override
//        public double calcOptimalOwnership()
//        {
//            return totalTokenRange * strategy.replicas() / (nodeCount() + nodeCount() / perNodeCount);
//        }

        @Override
        double evaluateImprovement(Token middle, Node newNode, double assignedTokenShare, int vn)
        {
            return evaluateCombinedImprovement(middle, newNode, sortedTokensWithNew, ownership, replicationStart, optimalOwnership, assignedTokenShare, nodeWeight, nodeWeight, 0, tokenWeight);
//            return evaluateCombinedImprovement(middle, newNode, sortedTokensWithNew, ownership, replicationStart, optimalOwnership, assignedTokenShare, nodeWeight, (nodeWeight * (vn + 1)) / perNodeCount, tokenWeight);
        }
    }
    
    static class FasterConfigurableTokenDistributor extends ConfigurableTokenDistributor
    {

        public FasterConfigurableTokenDistributor(NavigableMap<Token, Node> sortedTokens, ReplicationStrategy strategy,
                int perNodeCount, double nodeWeight, double tokenWeight, double nonReplWeight)
        {
            super(sortedTokens, strategy, perNodeCount, nodeWeight, tokenWeight, nonReplWeight);
        }
        
        void addNode(Node newNode)
        {
            ++nodeCount;
            if (replicationStart == null)
                replicationStart = createReplicationStartMap(sortedTokens);
            ownership = evaluateReplicatedOwnership();  // FIXME
            strategy.addNode(newNode);
            optimalOwnership = calcOptimalOwnership();
            ownership.put(newNode, optimalOwnership);
            double assignedTokenShare = optimalOwnership / perNodeCount;
            sortedTokensWithNew = Maps.newTreeMap(sortedTokens);
            PriorityQueue<Weighted<Token>> improvements = new PriorityQueue<>(sortedTokens.size());
            for (Token t : sortedTokens.keySet())
            {
                Token middle = t.slice(tokenSize(t) / 2);
                double impr = evaluateImprovement(middle, newNode, assignedTokenShare, 0);
                improvements.add(new Weighted<>(impr, middle));
            }
            Token bestToken = improvements.remove().value;
            
            for (int vn = 0; vn < perNodeCount; ++vn)
            {
//                printChangeStat(bestToken, newNode, sortedTokensWithNew, optimalOwnership);

                adjustData(bestToken, newNode, assignedTokenShare);
                if (vn == perNodeCount - 1)
                    return;
                
                for (;;)
                {
                    bestToken = improvements.remove().value;
                    double impr = evaluateImprovement(bestToken, newNode, assignedTokenShare, vn);
                    double nextImpr = improvements.peek().weight;
                    
                    if (impr >= nextImpr)
                        break;
                    improvements.add(new Weighted<>(impr, bestToken));
                }

            }
            assert verifyReplicationStarts();
            sortedTokensWithNew = null;
            ownership = null;
        }
    }

    static class ConfigurableTokenDistributor extends ReplicationAwareTokenDistributor
    {
        final double nodeWeight;
        final double tokenWeight;
        final double nonReplWeight;
        public ConfigurableTokenDistributor(NavigableMap<Token, Node> sortedTokens, ReplicationStrategy strategy,
                int perNodeCount, double nodeWeight, double tokenWeight, double nonReplWeight)
        {
            super(sortedTokens, strategy, perNodeCount);
            this.nodeWeight = nodeWeight;
            this.tokenWeight = tokenWeight;
            this.nonReplWeight = nonReplWeight;
        }

        @Override
        double evaluateImprovement(Token middle, Node newNode, double assignedTokenShare, int vn)
        {
            return evaluateCombinedImprovement(middle, newNode, sortedTokensWithNew, ownership, replicationStart, optimalOwnership, assignedTokenShare, nodeWeight, nodeWeight, tokenWeight, nonReplWeight);
        }

        @Override
        public String toString()
        {
            return super.toString() + " [node=" + nodeWeight + ", token=" + tokenWeight + ", nonRepl=" + nonReplWeight + "]";
        }


    }

    static class ReplicationAwareTokenDistributor extends TokenDistributor
    {
        Map<Token, Token> replicationStart;
        Map<Node, Double> ownership;
        NavigableMap<Token, Node> sortedTokensWithNew;
        double optimalOwnership;
        int nodeCount;
        
        public ReplicationAwareTokenDistributor(NavigableMap<Token, Node> sortedTokens, ReplicationStrategy strategy,
                int perNodeCount)
        {
            super(sortedTokens, strategy, perNodeCount);
            nodeCount = super.nodeCount();
        }

        void addNode(Node newNode)
        {
            ++nodeCount;
            if (replicationStart == null)
              replicationStart = createReplicationStartMap(sortedTokens);
            ownership = evaluateReplicatedOwnership();  // FIXME
            strategy.addNode(newNode);
            optimalOwnership = calcOptimalOwnership();
            ownership.put(newNode, optimalOwnership);
            double assignedTokenShare = optimalOwnership / perNodeCount;
            sortedTokensWithNew = Maps.newTreeMap(sortedTokens);
            List<Token> choices = Lists.newArrayList();
            for (Token t : sortedTokens.keySet())
            {
                choices.add(t.slice(tokenSize(t) / 2));
            }
            
            for (int vn = 0; vn < perNodeCount; ++vn)
            {
                Token bestToken = null;
                double bestImprovement = Double.NEGATIVE_INFINITY;
                for (Token middle : choices)
                {
                    if (debug >= 5) printChangeStat(middle, newNode, sortedTokensWithNew, optimalOwnership);
                    double impr = evaluateImprovement(middle, newNode, assignedTokenShare, vn);
                    if (impr > bestImprovement)
                    {
                        bestToken = middle;
                        bestImprovement = impr;
                    }
                }

                Token middle = bestToken;

                if (debug >= 3) {
                    System.out.print("Selected ");printChangeStat(bestToken, newNode, sortedTokensWithNew, optimalOwnership);
                }

                adjustData(middle, newNode, assignedTokenShare);
                choices.remove(middle);
            }
//            assert verifyOwnership(ownership);
//            assert verifyReplicationStarts();
            sortedTokensWithNew = null;
            ownership = null;
        }

        public Map<Token, Token> createReplicationStartMap(NavigableMap<Token, Node> sortedTokens)
        {
            Map<Token, Token> replicationStart = Maps.newHashMap();
            for (Map.Entry<Token, Node> en : sortedTokens.entrySet())
            {
                Node n = en.getValue();
                Token t = en.getKey();
                Token rs = strategy.replicationStart(t, n, sortedTokens);
                replicationStart.put(t, rs);
            }
            return replicationStart;
        }

        public double calcOptimalOwnership()
        {
            return totalTokenRange * strategy.replicas() / nodeCount;
        }

        public void adjustData(Token middle, Node newNode, double assignedTokenShare)
        {
            sortedTokensWithNew.put(middle, newNode);
            Token lr1 = strategy.lastReplicaToken(middle, sortedTokens);
            Token lr2 = strategy.lastReplicaToken(middle, sortedTokensWithNew);
            Token lastReplica = max(lr1, lr2, middle);
            for (Map.Entry<Token, Node> en : Iterables.concat(sortedTokens.tailMap(middle, false).entrySet(),
                                                              sortedTokens.entrySet()))
            {
                Token t = en.getKey();
                Node n = en.getValue();
                Token end = next(t);
                Token rs = strategy.replicationStart(t, n, sortedTokensWithNew);
                Token rsOld = replicationStart.get(t);
                double oldOwnership = ownership.get(n);
                double newOwnership = oldOwnership + rs.size(end) - rsOld.size(end);
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

            sortedTokens.put(middle, newNode);
            Token rs = strategy.replicationStart(middle, newNode, sortedTokensWithNew);
            replicationStart.put(middle, rs);
            ownership.put(newNode, ownership.get(newNode) + rs.size(next(middle)) - assignedTokenShare);
        }

        public boolean verifyReplicationStarts()
        {
            Token t;
            Node n;
            Token rs;
            boolean success = true;
            for (Map.Entry<Token, Node> ven: sortedTokens.entrySet()) {
                n = ven.getValue();
                t = ven.getKey();
                rs = strategy.replicationStart(t, n, sortedTokens);
                Token rss = replicationStart.get(t);
                if (rs != rss) {
                    System.out.format("Problem repl start of %s: %s vs. %s\n%s\n",
                                      t, rs, rss, sortedTokens);
                    success = false;
                }
            }
            return success;
        }
        
        double evaluateImprovement(Token middle, Node newNode, double assignedTokenShare, int vn)
        {
            return evaluateNodeImprovement(middle, newNode, sortedTokensWithNew, ownership, replicationStart, optimalOwnership, assignedTokenShare);
        }
        
        @Override
        public int nodeCount()
        {
            return nodeCount;
        }

        @Override
        public void removeNode(Node n)
        {
            replicationStart = null;
            super.removeNode(n);
            --nodeCount;
        }
        
    }

    static class LargestTokenBalancingTokenDistributor extends TokenBalancingTokenDistributor
    {
        

        
        public LargestTokenBalancingTokenDistributor(NavigableMap<Token, Node> sortedTokens, ReplicationStrategy strategy,
                int perNodeCount)
        {
            super(sortedTokens, strategy, perNodeCount);
        }

        void addNode(Node newNode)
        {
            ++nodeCount;
            strategy.addNode(newNode);
            optimalOwnership = calcOptimalOwnership();
            sortedTokensWithNew = Maps.newTreeMap(sortedTokens);

            for (int vn = 0; vn < perNodeCount; ++vn)
            {
                double bestSize = Double.NEGATIVE_INFINITY;
                Token bestToken = null;
                for (Token t : sortedTokens.keySet())
                {
                    double size = replicatedTokenOwnership(t);
                    if (size > bestSize) {
                        bestToken = t;
                        bestSize = size;
                    }
                }
                
                Token end = bestToken;
                Token start = strategy.replicationStart(end, sortedTokens.get(bestToken), sortedTokens);
                Iterable<Token> affectingSpan;
                if (start.compareTo(end) <= 0)
                    affectingSpan = sortedTokens.subMap(start, true, end, true).keySet();
                else
                    affectingSpan = Iterables.concat(sortedTokens.tailMap(start, true).keySet(), sortedTokens.headMap(end, true).keySet());

                double bestImprovement = Double.NEGATIVE_INFINITY;
                bestToken = null;
                for (Token t : affectingSpan)
                {
                    // TODO: Can we do better than just picking middle?
                    Token middle = t.slice(tokenSize(t) / 2);
                    double impr = evaluateImprovement(middle, newNode);
                    if (impr > bestImprovement)
                    {
                        bestToken = middle;
                        bestImprovement = impr;
                    }
                }
//                printChangeStat(bestToken, newNode, sortedTokensWithNew, optimalOwnership * perNodeCount);

                // Greedy scheme. Pick best.
                Token middle = bestToken;
                sortedTokensWithNew.put(middle, newNode);
                sortedTokens.put(middle, newNode);
                
//                assert replicatedTokenOwnership(end) < bestSize;
            }
            // release copy.
            sortedTokensWithNew = null;
        }
        
    }
    
    static class TokenBalancingTokenDistributor extends TokenDistributor
    {
        double optimalOwnership;
        NavigableMap<Token, Node> sortedTokensWithNew;
        int nodeCount;
        
        public TokenBalancingTokenDistributor(NavigableMap<Token, Node> sortedTokens, ReplicationStrategy strategy,
                int perNodeCount)
        {
            super(sortedTokens, strategy, perNodeCount);
            nodeCount = super.nodeCount();
        }
        
        public double calcOptimalOwnership()
        {
            return totalTokenRange * strategy.replicas() / (perNodeCount * nodeCount);
        }

        @Override
        void addNode(Node newNode)
        {
            ++nodeCount;
            strategy.addNode(newNode);
            optimalOwnership = calcOptimalOwnership();
            sortedTokensWithNew = Maps.newTreeMap(sortedTokens);

            List<Token> choices = Lists.newArrayList();
            for (Token t : sortedTokens.keySet())
            {
                choices.add(t.slice(tokenSize(t) / 2));
            }

            for (int vn = 0; vn < perNodeCount; ++vn)
            {
                double bestImprovement = Double.NEGATIVE_INFINITY;
                Token bestToken = null;
                for (Token middle : choices)
                {
                    double impr = evaluateImprovement(middle, newNode);
                    if (impr > bestImprovement)
                    {
                        bestToken = middle;
                        bestImprovement = impr;
                    }
                }

                // Greedy scheme. Pick best.
                Token middle = bestToken;
                sortedTokensWithNew.put(middle, newNode);
                sortedTokens.put(middle, newNode);
                choices.remove(middle);
            }
            // release copy.
            sortedTokensWithNew = null;
        }
        
        @Override
        public int nodeCount()
        {
            return nodeCount;
        }

        @Override
        public void removeNode(Node n)
        {
            super.removeNode(n);
            --nodeCount;
        }

        protected double evaluateImprovement(Token middle, Node newNode)
        {
            return evaluateTokenImprovement(middle, newNode, sortedTokensWithNew, optimalOwnership);
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

    private static Token max(Token t1, Token t2, Token m)
    {
        return m.size(t1) >= m.size(t2) ? t1 : t2;
    }

    public static double sq(double d)
    {
        return d*d;
    }

    private static void perfectDistribution(Map<Token, Node> map, int nodeCount, int perNodeCount)
    {
        System.out.format("\nPerfect init for %d nodes with %d tokens each.\n", nodeCount, perNodeCount);
        for (int i=0; i<nodeCount; ++i)
        {
            Node node = new Node();
            double inc = totalTokenRange / perNodeCount;
            double start = Long.MIN_VALUE + inc / nodeCount * i;
            for (int j = 0 ; j < perNodeCount ; j++)
            {
                map.put(new Token((long) start), node);
                start += inc;
            }
        }
    }

    private static void random(Map<Token, Node> map, int nodeCount, int perNodeCount, boolean localRandom)
    {
        System.out.format("\nRandom generation of %d nodes with %d tokens each%s\n", nodeCount, perNodeCount, (localRandom ? ", locally random" : ""));
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        long inc = -(Long.MIN_VALUE / perNodeCount) * 2;
        for (int i = 0 ; i < nodeCount ; i++)
        {
            Node node = new Node();
            for (int j = 0 ; j < perNodeCount ; j++)
            {
                long nextToken;
                if (localRandom && perNodeCount > 1) nextToken = Long.MIN_VALUE + j * inc + rand.nextLong(inc);
                else nextToken = rand.nextLong();
                map.put(new Token(nextToken), node);
            }
        }
    }
    
    private static String stats(Collection<Double> data, double normalization, int countPerInc)
    {
        double mul = normalization * countPerInc;
        DoubleSummaryStatistics stat = data.stream().mapToDouble(x->x * mul).summaryStatistics();
        double avg = stat.getAverage();
        double dev = data.stream().mapToDouble(x->sq(x * mul - avg)).sum();
        long sz = stat.getCount();
        double stdDev = Math.sqrt(dev / (sz - 1));
        double nextAvg = stat.getSum() / (sz + countPerInc);
        double nextDev = (data.stream().mapToDouble(x->sq(x * mul - nextAvg)).sum() * sq(sz + countPerInc)) / sq(sz);
        return String.format("max %.2f min %.2f stddev %.5f sq %.5f next %.5f",
                             stat.getMax(),
                             stat.getMin(),
                             stdDev,
                             dev,
                             nextDev);
        
    }

    private static void printDistribution(TokenDistributor t)
    {
        Map<Node, Double> ownership = t.evaluateReplicatedOwnership();
        int size = t.nodeCount();
        double inverseAverage = size / (totalTokenRange * t.strategy.replicas());
        List<Double> tokenOwnership = Lists.newArrayList(t.sortedTokens.keySet().stream().mapToDouble(t::replicatedTokenOwnership).iterator());
        System.out.format("Size %d   node %s  token %s   %s\n",
                          size,
                          stats(ownership.values(), inverseAverage, 1),
                          stats(tokenOwnership, inverseAverage, t.perNodeCount),
                          t.strategy);
//        if (size % 25 == 0) {
//          System.out.println(Arrays.toString(t.sortedTokens.keySet().stream().mapToDouble(t::replicatedTokenOwnership).toArray()));
//          System.out.println(t.sortedTokens);
//        }
    }

    private static void printDistribution(TokenDistributor t, ReplicationStrategy rs)
    {
        printDistribution(new TokenDistributor(t.sortedTokens, rs, t.perNodeCount));
    }


    public static void main1(String[] args)
    {
      final int targetClusterSize = 4;
      int howmany = 30;
      int perNodeCount = 3;
      int rf = 3;
      NavigableMap<Token, Node> tokenMap;
      double maxTokenSize;
      do {
          tokenMap = Maps.newTreeMap();
          Node A = new Node();
          Node B = new Node();
          Node C = new Node();
          Node D = new Node();
          tokenMap.put(new Token(0x90L<<56), A);
          tokenMap.put(new Token(0x91L<<56), B);
          tokenMap.put(new Token(0x92L<<56), B);
          tokenMap.put(new Token(0x93L<<56), B);
          tokenMap.put(new Token(0xc0L<<56), C);
          tokenMap.put(new Token(0xd0L<<56), C);
          tokenMap.put(new Token(0xE0L<<56), A);
          tokenMap.put(new Token(0xf0L<<56), C);
          tokenMap.put(new Token(0x00L<<56), A);
//          tokenMap.put(new Token(0x00L<<56), C);
//          tokenMap.put(new Token(0x09L<<56), A);
//          tokenMap.put(new Token(0x10L<<56), A);
//          tokenMap.put(new Token(0x90L<<56), B);
//          tokenMap.put(new Token(0x94L<<56), C);
//          tokenMap.put(new Token(0x98L<<56), D);
//          tokenMap.put(new Token(0x9dL<<56), B);
//          tokenMap.put(new Token(0xE0L<<56), D);
//          tokenMap.put(new Token(0xF8L<<56), D);
          
//          int others = 4;
//          long split = 0xc0L << 56;
//          long inc = (-split) / (rf * others);
//          for (int i=0;i<others; ++i) {
//              Node n = new Node();
//              for (int j=0;j<rf; ++j)
//                  tokenMap.put(new Token(split + inc * (i + j*others)), n);
//          }
    
    //      perfectDistribution(tokenMap, targetClusterSize, perNodeCount);
//          boolean locallyRandom = false;
//          random(tokenMap, targetClusterSize, perNodeCount, locallyRandom);
    
          TokenDistributor t =
              new ConfigurableTokenDistributor(tokenMap, new SimpleReplicationStrategy(rf), perNodeCount, 1, 0, 0);
    
          for (int i=t.nodeCount(); i<=targetClusterSize + howmany; ++i)
              t.addNode(new Node());
          maxTokenSize = t.sortedTokens.keySet().stream().mapToDouble(t::replicatedTokenOwnership).max().getAsDouble()*perNodeCount*t.nodeCount() / (totalTokenRange * t.strategy.replicas());
          System.out.println("Max token size " + maxTokenSize);
      } while (false);//maxTokenSize < 10);
      System.out.println("Initial state " + tokenMap);
      
      TokenDistributor t =
//              new ConfigurableTokenDistributor(tokenMap, new SimpleReplicationStrategy(rf), perNodeCount, 1, 0, 0);
              new ReplicationAwareTokenDistributorRec(tokenMap, new SimpleReplicationStrategy(rf), perNodeCount);
      for (int i=t.nodeCount(); i<=targetClusterSize + howmany; ++i) {
          test(t, i);
      }
      
      TokenDistributor td =
//              new ConfigurableTokenDistributor(tokenMap, new SimpleReplicationStrategy(rf), perNodeCount, 1, 0, 0);
              new ReplicationAwareTokenDistributorRec(tokenMap, new SimpleReplicationStrategy(rf), perNodeCount);
      td.debug = 5;
      for (int i=td.nodeCount(); i<=targetClusterSize + howmany; ++i) {
          System.out.println("\ncURRENT state " + td.sortedTokens);
          test(td, i);
      }
      
        
    }
        
    public static void main(String[] args)
    {
        for (int perNodeCount = 1; perNodeCount <= 256; perNodeCount *= 4)
        {
        final int targetClusterSize = 500;
//        int perNodeCount = 4;
        NavigableMap<Token, Node> tokenMap = Maps.newTreeMap();
//        Node A = new Node();
//        Node B = new Node();
//        Node C = new Node();
//        tokenMap.put(new Token(0x93L<<56), A);
//        tokenMap.put(new Token(0x94L<<56), B);
//        tokenMap.put(new Token(0x95L<<56), B);
//        tokenMap.put(new Token(0xA4L<<56), B);
//        tokenMap.put(new Token(0xB8L<<56), C);
//        tokenMap.put(new Token(0xE1L<<56), C);
//        tokenMap.put(new Token(0xE2L<<56), A);
//        tokenMap.put(new Token(0x23L<<56), C);
//        tokenMap.put(new Token(0x34L<<56), A);

//        perfectDistribution(tokenMap, targetClusterSize, perNodeCount);
        boolean locallyRandom = true;
        random(tokenMap, targetClusterSize, perNodeCount, locallyRandom);

        Set<Node> nodes = Sets.newTreeSet(tokenMap.values());
        TokenDistributor[] t = {
//            new TokenBalancingTokenDistributor(tokenMap, new SimpleReplicationStrategy(3), perNodeCount),
//            new ConfigurableTokenDistributor(tokenMap, new SimpleReplicationStrategy(3), perNodeCount, 1, 0, 0),
//            new FasterConfigurableTokenDistributor(tokenMap, new SimpleReplicationStrategy(3), perNodeCount, 0, 0, 1),
//            new FasterConfigurableTokenDistributor(tokenMap, new SimpleReplicationStrategy(3), perNodeCount, 0, 1, 1),
//            new FasterConfigurableTokenDistributor(tokenMap, new SimpleReplicationStrategy(3), perNodeCount, 0, 1, 0),
            new FasterConfigurableTokenDistributor(tokenMap, new SimpleReplicationStrategy(3), perNodeCount, 1, 1, 0),
//            new FasterConfigurableTokenDistributor(tokenMap, new SimpleReplicationStrategy(3), perNodeCount, 1, 0, 0),
//            new FasterConfigurableTokenDistributor(tokenMap, new SimpleReplicationStrategy(3), perNodeCount, 1, 0, 1),
//            new FasterConfigurableTokenDistributor(tokenMap, new SimpleReplicationStrategy(3), perNodeCount, 1, 1, 1),
//            new GrowingWeightTokenDistributor(tokenMap, new SimpleReplicationStrategy(3), perNodeCount, 1, 1),
//            new FasterConfigurableTokenDistributor(tokenMap, new SimpleReplicationStrategy(3), perNodeCount, 1, 2),
//            new FasterConfigurableTokenDistributor(tokenMap, new SimpleReplicationStrategy(3), perNodeCount, 2, 1),
//            new FasterConfigurableTokenDistributor(tokenMap, new SimpleReplicationStrategy(3), perNodeCount, 3, 1),
//            new FasterConfigurableTokenDistributor(tokenMap, new SimpleReplicationStrategy(3), perNodeCount, 1, 0),
//            new FasterConfigurableTokenDistributor(tokenMap, new FixedRackCountReplicationStrategy(3, 3, nodes), perNodeCount, 0, 1),
//            new FasterConfigurableTokenDistributor(tokenMap, new FixedRackCountReplicationStrategy(3, 3, nodes), perNodeCount, 1, 2),
//            new FasterConfigurableTokenDistributor(tokenMap, new FixedRackCountReplicationStrategy(3, 3, nodes), perNodeCount, 1, 1),
//            new FasterConfigurableTokenDistributor(tokenMap, new FixedRackCountReplicationStrategy(3, 3, nodes), perNodeCount, 2, 1),
//            new FasterConfigurableTokenDistributor(tokenMap, new FixedRackCountReplicationStrategy(3, 3, nodes), perNodeCount, 1, 0),
//            new LargestTokenBalancingTokenDistributor(tokenMap, new SimpleReplicationStrategy(3), perNodeCount),
//            new TokenBalancingTokenDistributor(tokenMap, new FixedRackCountReplicationStrategy(3, 3, nodes), perNodeCount),
//            new LargestTokenBalancingTokenDistributor(tokenMap, new FixedRackCountReplicationStrategy(3, 3, nodes), perNodeCount),
//            new TokenBalancingTokenDistributor(tokenMap, new FixedRackCountReplicationStrategy(3, 5, nodes), perNodeCount),
//            new LargestTokenBalancingTokenDistributor(tokenMap, new FixedRackCountReplicationStrategy(3, 5, nodes), perNodeCount),

//            new ReplicationAwareTokenDistributorRec(tokenMap, new SimpleReplicationStrategy(3), perNodeCount),
//          new ConfigurableTokenDistributor(tokenMap, new SimpleReplicationStrategy(3), perNodeCount, 1, 0),
//          new ConfigurableTokenDistributor(tokenMap, new SimpleReplicationStrategy(3), perNodeCount, 1, 1),
//          new ConfigurableTokenDistributor(tokenMap, new SimpleReplicationStrategy(3), perNodeCount, 0, 1),
//          new GrowingWeightTokenDistributor(tokenMap, new SimpleReplicationStrategy(3), perNodeCount, 1, 0),
//          new GrowingWeightTokenDistributor(tokenMap, new SimpleReplicationStrategy(3), perNodeCount, 1, 1),

//            new ReplicationAwareTokenDistributor2(tokenMap, new SimpleReplicationStrategy(3), perNodeCount),
//            new ConfigurableTokenDistributor(tokenMap, new SimpleReplicationStrategy(3), perNodeCount, 2, 1),
//            new ConfigurableTokenDistributor(tokenMap, new SimpleReplicationStrategy(3), perNodeCount, 5, 1),
//            new ReplicationAwareTokenDistributor3(tokenMap, new SimpleReplicationStrategy(3), perNodeCount),
//            new ReplicationAwareTokenDistributor(tokenMap, new FixedRackCountReplicationStrategy(3, 3, nodes), perNodeCount),
//            new ReplicationAwareTokenDistributor2(tokenMap, new FixedRackCountReplicationStrategy(3, 3, nodes), perNodeCount),
//            new ReplicationAwareTokenDistributorLarge(tokenMap, new FixedRackCountReplicationStrategy(3, 3, nodes), perNodeCount),
//          new ReplicationAwareTokenDistributor(tokenMap, new FixedRackCountReplicationStrategy(3, 5, nodes), perNodeCount),
//            new NoReplicationTokenDistributor(tokenMap, perNodeCount)
        };
        if (nodes.size() < targetClusterSize)
            test(t, nodes.size());

//        for (int i=nodes.size(); i<=targetClusterSize + 1000; i+=1)
//            test(t, i);
        
//
//        test(t, targetClusterSize * 5 / 4);
        
//        TokenDistributor t = new NoReplicationTokenDistributor(tokenMap, perNodeCount);
        test(t, targetClusterSize);
//
        test(t, targetClusterSize + 1);

        test(t, targetClusterSize * 101 / 100);

        test(t, targetClusterSize * 26 / 25);

        test(t, targetClusterSize * 5 / 4);

        test(t, targetClusterSize * 2);

        testLoseAndReplace(t, 1);
        testLoseAndReplace(t, targetClusterSize / 100);
        testLoseAndReplace(t, targetClusterSize / 25);
        testLoseAndReplace(t, targetClusterSize / 4);
        }
    }

    private static void testLoseAndReplace(TokenDistributor[] ts, int howMany)
    {
        // TODO Auto-generated method stub
        for (TokenDistributor t: ts)
            testLoseAndReplace(t, howMany);
    }

    private static void test(TokenDistributor[] ts, int targetClusterSize)
    {
        System.out.println(targetClusterSize);
        for (TokenDistributor t: ts)
            test(t, targetClusterSize);
    }

    private static void testLoseAndReplace(TokenDistributor t, int howMany)
    {
        int fullCount = t.nodeCount();
        System.out.format("Losing %d nodes\n", howMany);
        Random rand = new Random(howMany);
        for (int i=0; i<howMany; ++i)
            t.removeNode(new Token(rand.nextLong()));
        test(t, t.nodeCount());
        
        test(t, fullCount);
    }

    public static void test(TokenDistributor t, int targetClusterSize)
    {
        int size = t.nodeCount();
        if (size < targetClusterSize)
            System.out.format("Adding %d node(s) using %s\n", targetClusterSize - size, t.toString());
        while (size < targetClusterSize)
        {
//            System.out.println(t.sortedTokens);
            t.addNode(new Node());
            ++size;
        }
        printDistribution(t);
//        if (!(t.strategy instanceof NoReplicationStrategy))
//            printDistribution(t, new NoReplicationStrategy());
//        Set<Node> nodes = Sets.newTreeSet(t.sortedTokens.values());
//        printDistribution(t, new SimpleReplicationStrategy(3));
//        printDistribution(t, new SimpleReplicationStrategy(5));
//        printDistribution(t, new SimpleReplicationStrategy(17));
//        printDistribution(t, new BalancedRackReplicationStrategy(3, 4, nodes));
//        printDistribution(t, new BalancedRackReplicationStrategy(3, 8, nodes));
//        printDistribution(t, new FixedRackCountReplicationStrategy(3, 3, nodes));
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
