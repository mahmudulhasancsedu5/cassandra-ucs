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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
    
    static class Weighted<T> implements Comparable<Weighted<T>> {
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

        /**
         * Returns a rack identifier. getRack(a) == getRack(b) iff a and b are on the same rack.
         * @return Some hashable object.
         */
        Object getRack(Node node);
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

        public Object getRack(Node node)
        {
            return node;
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

        public Node getRack(Node node)
        {
            // The node is the rack.
            return node;
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
                // For simlicity assuming list can't be exhausted before finding all replicas.
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

        public Integer getRack(Node node)
        {
            return rackMap.get(node);
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

    /**
     * Rack strategy spreading nodes into a fixed number of racks.
     */
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

    /**
     * Rack strategy with a fixed number of nodes per rack.
     */
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
            
            // verify ownership is the same as what's calculated using the replication start method.
            assert verifyOwnership(ownership);
            
            return ownership;
        }

        private void addOwnership(Token current, Token next, Map<Node, Double> ownership)
        {
            double size = current.size(next);
            for (Node n : strategy.getReplicas(current, sortedTokens)) {
                Double v = ownership.get(n);
                ownership.put(n, v != null ? v + size : size);
            }
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

        public boolean verifyReplicationStartMap(Map<Token, Token> replicationStart)
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

        protected double evaluateCombinedImprovement(Token middle, Node newNode,
                NavigableMap<Token, Node> sortedTokensWithNew, Map<Node, Double> ownership, Map<Token, Token> replicationStart,
                double nodeOptimal, double tokenOptimal, double nodeMult, double newNodeMult, double tokenMult)
        {
            sortedTokensWithNew.put(middle, newNode);
            // First, check which tokens are affected by the split.
            Token lr1 = strategy.lastReplicaToken(middle, sortedTokens);
            // The split can cause some tokens to be replicated further than they used to.
            Token lr2 = strategy.lastReplicaToken(middle, sortedTokensWithNew);
            Token lastReplica = max(lr1, lr2, middle);

            // We can't directly calculate node improvement as a node may have multiple affected tokens. Store new size in a map.
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
                tokenImprovement += sq(osz - tokenOptimal) - sq(nsz - tokenOptimal);
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
            tokenImprovement += sq(osz - tokenOptimal) - sq(nsz - tokenOptimal);
            addToWeight(nodeToWeight, ownership, node, nsz - osz);
            
            // Calculate the size of the newly added token.
            Token rsNew = strategy.replicationStart(middle, newNode, sortedTokensWithNew);
            osz = tokenOptimal;
            nsz = rsNew.size(nextToken);
            tokenImprovement += sq(osz - tokenOptimal) - sq(nsz - tokenOptimal);
            addToWeight(nodeToWeight, ownership, newNode, nsz - osz);

            // Evaluate the node-level improvement from the nodeToWeight map.
            double nodeImprovement = nodeToWeight.entrySet().stream().mapToDouble(
                enn -> (sq(ownership.get(enn.getKey()) - nodeOptimal) - sq(enn.getValue() - nodeOptimal)) * (enn.getKey() == newNode ? newNodeMult : nodeMult)).sum();

            return nodeImprovement + tokenImprovement * tokenMult;
        }

        public static void addToWeight(Map<Node, Double> nodeToWeight, Map<Node, Double> fallback, Node n, double weightChange)
        {
            Double v = nodeToWeight.get(n);
            if (v == null)
                v = fallback.get(n);
            nodeToWeight.put(n, v + weightChange);
        }

        // Debug function tracing the results of the above process.
        public double printChangeStat(Token middle, Node newNode, NavigableMap<Token, Node> sortedTokensWithNew, double nopt, int vn)
        {
            double topt = nopt/perNodeCount;
            sortedTokensWithNew.put(middle, newNode);
            StringBuilder afft = new StringBuilder();
            Token lr1 = strategy.lastReplicaToken(middle, sortedTokens);
            Token lr2 = strategy.lastReplicaToken(middle, sortedTokensWithNew);
            Token lastReplica = max(lr1, lr2, middle);
            Map<Node, Double> nodeToWeight = Maps.newHashMap();
            Map<Node, Double> ownership = evaluateReplicatedOwnership();
            Double v = ownership.get(newNode);
            if (v == null) v = 0.0;
            ownership.put(newNode, v + (perNodeCount - vn) * topt);
            double tokenImprovement = 0;
            for (Token t = mapEntryFor(middle).getKey(); ; t = next(t)) {
                double osz = replicatedTokenOwnership(t, sortedTokens);
                double nsz = replicatedTokenOwnership(t, sortedTokensWithNew);
                afft.append(String.format("%s:%.3f->%.3f(%.5f),",
                                          t,
                                          osz / topt,
                                          nsz / topt,
                                          sq(osz / topt - 1) - sq(nsz / topt - 1)));
                tokenImprovement += sq(osz - topt) - sq(nsz - topt);
                addToWeight(nodeToWeight, ownership, nodeFor(t), nsz - osz);
                if (t == lastReplica)
                    break;
            }
            double nsz = replicatedTokenOwnership(middle, sortedTokensWithNew);
            System.out.format("change token %s->%.3f(%.5f) affects %s\n",
                              middle, nsz / topt, -sq(nsz / topt - 1),
                              afft);
            tokenImprovement -= sq(nsz - topt);
            addToWeight(nodeToWeight, ownership, newNode, nsz - topt);
            sortedTokensWithNew.remove(middle);

            List<Double> tokenOwnership = Lists.newArrayList(sortedTokens.keySet().stream().mapToDouble(this::replicatedTokenOwnership).iterator());
            double dev = tokenOwnership.stream().mapToDouble(x -> sq(x / topt - 1)).sum();
            DoubleSummaryStatistics stat = tokenOwnership.stream().mapToDouble(x -> x / topt).summaryStatistics();
            System.out.format(" expected token impr %.5f on %.5f max %.2f min %.2f\n", tokenImprovement / sq(topt), dev, stat.getMax(), stat.getMin());
            
            System.out.print(" affects ");
            for (Map.Entry<Node, Double> en: nodeToWeight.entrySet())
            {
                Node n = en.getKey();
                nsz = en.getValue();
                double oldd = ownership.get(n);
                System.out.format("node %s:%.3f->%.3f(%.5f),",
                                  n,
                                  oldd / nopt,
                                  nsz / nopt,
                                  sq(oldd / nopt - 1) - sq(nsz / nopt - 1));
            }
            dev = ownership.values().stream().mapToDouble(x -> sq(x / nopt - 1)).sum();
            stat = ownership.values().stream().mapToDouble(x -> x / nopt).summaryStatistics();
            double nodeImprovement = nodeToWeight.entrySet().stream().mapToDouble(
                enn -> (sq(ownership.get(enn.getKey()) - nopt) - sq(enn.getValue() - nopt)) * (enn.getKey() == newNode ? (1.0 + vn) / perNodeCount : 1.0)).sum();
            System.out.format("\n expected node impr %.5f on %.5f max %.2f min %.2f\n", nodeImprovement / sq(nopt), dev, stat.getMax(), stat.getMin());
            return tokenImprovement + nodeImprovement;
        }
    }
    
    static RackInfo getRack(Node node, Map<Object, RackInfo> rackMap, ReplicationStrategy strategy)
    {
        Object rackClass = strategy.getRack(node);
        RackInfo ri = rackMap.get(rackClass);
        if (ri == null)
            rackMap.put(rackClass, ri = new RackInfo(rackClass));
        return ri;
    }
    
    /**
     * Unique rack object that one or more NodeInfo objects link to.
     */
    static class RackInfo {
        /** Rack identifier given by ReplicationStrategy.getRack(Node). */
        final Object rack;

        /**
         * Seen marker. When non-null, the rack is already seen in replication walks.
         * Also points to previous seen rack to enable walking the seen racks and clearing the seen markers.
         */
        RackInfo prevSeen = null;
        /** Same marker/chain used by populateTokenInfo. */
        RackInfo prevPopulate = null;

        /** Value used as terminator for seen chains. */
        static RackInfo TERMINATOR = new RackInfo(null);

        public RackInfo(Object rack)
        {
            this.rack = rack;
        }

        public String toString()
        {
            return rack.toString() + (prevSeen != null ? "*" : ""); 
        }
    }

    /**
     * Node information created and used by ReplicationAwareTokenDistributor. Contained vnodes all point to the same
     * instance.
     */
    static class NodeInfo {
        final Node node;
        final RackInfo rack;
        double ownership;

        /** During evaluateImprovement this is used to form a chain of nodes affected by the candidate insertion. */
        NodeInfo prevUsed;
        /** During evaluateImprovement this holds the ownership after the candidate insertion. */
        double adjustedOwnership;

        private NodeInfo(Node node, RackInfo rack)
        {
            this.node = node;
            this.rack = rack;
        }
        
        public NodeInfo(Node node, double ownership, Map<Object, RackInfo> rackMap, ReplicationStrategy strategy)
        {
            this(node, getRack(node, rackMap, strategy));
            this.ownership = ownership;
        }
        
        public String toString()
        {
            return String.format("%s%s(%.2e)%s", node, rack.prevSeen != null ? "*" : "", ownership, prevUsed != null ? "prev " + (prevUsed == this ? "this" : prevUsed.toString()) : ""); 
        }
    }

    static class CircularList<T extends CircularList<T>> {
        T prev;
        T next;
        
        /**
         * Inserts this after node in the circular list which starts at head. Returns the new head of the list, which
         * only changes if head was null.
         */
        @SuppressWarnings("unchecked")
        T insertAfter(T head, T node) {
            if (head == null) {
                return prev = next = (T) this;
            }
            assert node != null;
            assert node.next != null;
            prev = node;
            next = node.next;
            prev.next = (T) this;
            next.prev = (T) this;
            return head;
        }
        
        /** 
         * Removes this from the list that starts at head. Returns the new head of the list, which only changes if the
         * head was removed.
         */
        T removeFrom(T head) {
            next.prev = prev;
            prev.next = next;
            return this == head ? (this == next ? null : next) : head;
        }
    }
    
    static class BaseTokenInfo<T extends BaseTokenInfo<T>> extends CircularList<T> {
        final Token token;
        final NodeInfo owningNode;

        /**
         * Start of the replication span for the vnode, i.e. the end of the first token of the RF'th rack seen before
         * the token. The replicated ownership of the node is the range between replicationStart and next.token.
         */
        Token replicationStart;
        /**
         * RF minus one boundary, i.e. the end of the first token of the RF-1'th rack seen before the token.
         * Used to determine replicationStart after insertion of new token.
         */
        Token rfm1Token;
        /**
         * Whether token can be expanded (and only expanded) by adding new node that ends at replicationStart.
         */
        boolean expandable;
        /**
         * Current replicated ownership. This number is reflected in the owning node's ownership.
         */
        double replicatedOwnership = 0;
        
        public BaseTokenInfo(Token token, NodeInfo owningNode)
        {
            this.token = token;
            this.owningNode = owningNode;
        }

        public String toString()
        {
            return String.format("%s(%s)%s", token, owningNode, expandable ? "=" : ""); 
        }
        
        /**
         * Previous node in the token ring. For existing tokens this is prev, for candidates it's the host.
         */
        TokenInfo prevInRing()
        {
            return null;
        }
    }
    
    /**
     * TokenInfo about existing tokens/vnodes.
     */
    static class TokenInfo extends BaseTokenInfo<TokenInfo> {
        public TokenInfo(Token token, NodeInfo owningNode)
        {
            super(token, owningNode);
        }
        
        TokenInfo prevInRing()
        {
            return prev;
        }
    }
    
    /**
     * TokenInfo about candidate new tokens/vnodes.
     */
    static class CandidateInfo extends BaseTokenInfo<CandidateInfo> {
        final TokenInfo host;

        public CandidateInfo(Token token, TokenInfo host, NodeInfo owningNode)
        {
            super(token, owningNode);
            this.host = host;
        }
        
        TokenInfo prevInRing()
        {
            return host;
        }
    }
    
    static void dumpTokens(String lead, BaseTokenInfo<?> tokens)
    {
        BaseTokenInfo<?> token = tokens;
        do {
            System.out.format("%s%s: rs %s rfm1 %s size %.2e\n", lead, token, token.replicationStart, token.rfm1Token, token.replicatedOwnership);
            token = token.next;
        } while (token != null && token != tokens);
    }
    
    static class ReplicationAwareTokenDistributor2 extends ReplicationAwareTokenDistributor
    {
        public ReplicationAwareTokenDistributor2(NavigableMap<Token, Node> sortedTokens, ReplicationStrategy strategy,
                int perNodeCount)
        {
            super(sortedTokens, strategy, perNodeCount);
        }

        double evaluateToken(double ownership, double optOwnership)
        {
//            double lo = optOwnership * 0.9;
//            if (ownership < optOwnership * 1.1)
//                return sq(lo - ownership);
//            double hi = optOwnership * 1.6;
//            //if (ownership > opt)
//                return sq(ownership - hi);
            
            return sq(ownership - optOwnership);
        }
        
        double evaluateImprovement(CandidateInfo candidate, double optNodeOwnership, double optTokenOwnership, int vn)
        {
            double change = 0;
            TokenInfo host = candidate.host;
            TokenInfo next = host.next;
            Token cend = next.token;

            // Reflect change in ownership of the splitting token (candidate).
            double oldOwnership = candidate.replicatedOwnership;
            double newOwnership = candidate.replicationStart.size(next.token);
            change += evaluateToken(newOwnership, optTokenOwnership) - evaluateToken(oldOwnership, optTokenOwnership);
            NodeInfo newNode = candidate.owningNode;
            newNode.adjustedOwnership = newNode.ownership + newOwnership - oldOwnership;
            
            // Reflect change of ownership in the token being split (host).
            oldOwnership = host.replicatedOwnership;
            newOwnership = host.replicationStart.size(candidate.token);
            change += evaluateToken(newOwnership, optTokenOwnership) - evaluateToken(oldOwnership, optTokenOwnership);
            NodeInfo hostNode = host.owningNode;
            hostNode.adjustedOwnership = hostNode.ownership + newOwnership - oldOwnership;

            // Form a chain of nodes affected by the insertion to be able to qualify change of node ownership.
            // A node may be affected more than once.
            assert newNode.prevUsed == null;
            newNode.prevUsed = newNode;   // end marker
            RackInfo newRack = newNode.rack;
            assert hostNode.prevUsed == null;
            hostNode.prevUsed = newNode;
            RackInfo hostRack = hostNode.rack;
            NodeInfo nodesChain = hostNode;

            // Loop through all vnodes that replicate candidate or host and update their ownership.
            int seenOld = 0;
            int seenNew = 0;
            int rf = strategy.replicas() - 1;
          evLoop:
            for (TokenInfo curr = next; seenOld < rf || seenNew < rf; curr = next)
            {
                next = curr.next;
                NodeInfo currNode = curr.owningNode;
                RackInfo rack = currNode.rack;
                if (rack.prevSeen != null)
                    continue evLoop;    // If both have already seen this rack, nothing can change within it.
                if (rack != newRack)
                    seenNew += 1;
                if (rack != hostRack)
                    seenOld += 1;
                rack.prevSeen = rack;   // Just mark it as seen, we are not forming a chain of racks.
                
                if (currNode.prevUsed == null)
                {
                    currNode.adjustedOwnership = currNode.ownership;
                    currNode.prevUsed = nodesChain;
                    nodesChain = currNode;
                }

                Token rs;
                if (curr.expandable) // Sees same or new rack before rf-1.
                {
                    if (cend != curr.replicationStart)
                        continue evLoop;
                    // Replication expands to start of candidate.
                    rs = candidate.token;
                } else {
                    if (rack == newRack)
                    {
                        if (!preceeds(curr.replicationStart, cend, next.token))
                            continue evLoop; // no changes, another newRack is closer
                        // Replication shrinks to end of candidate.
                        rs = cend;
                    } else {
                        if (preceeds(curr.rfm1Token, cend, next.token))
                            // Candidate is closer than one-before last.
                            rs = curr.rfm1Token;
                        else if (preceeds(cend, curr.rfm1Token, next.token))
                            // Candidate is the replication boundary.
                            rs = cend;
                        else
                            // Candidate replaces one-before last. The latter becomes the replication boundary.
                            rs = candidate.token;
                    }
                }

                // Calculate ownership adjustments.
                oldOwnership = curr.replicatedOwnership;
                newOwnership = rs.size(next.token);
                change += evaluateToken(newOwnership, optTokenOwnership) - evaluateToken(oldOwnership, optTokenOwnership);
                currNode.adjustedOwnership += newOwnership - oldOwnership;
            }

            // Now loop through the nodes chain and add the node-level changes. Also clear the racks' seen marks.
            for (;;) {
                newOwnership = nodesChain.adjustedOwnership;
                oldOwnership = nodesChain.ownership;
                double diff = sq(newOwnership - optNodeOwnership) - sq(oldOwnership - optNodeOwnership);
                NodeInfo prev = nodesChain.prevUsed;
                nodesChain.prevUsed = null;
                nodesChain.rack.prevSeen = null;
                if (nodesChain != newNode)
                    change += diff;
                else 
                {
                    change += (diff * (vn + 1)) / perNodeCount;
                    break;
                }
                nodesChain = prev;
            }

            return -change;
        }
        
        public double calcOptimalOwnership()
        {
            return totalTokenRange * strategy.replicas() / (nodeCount + nodeCount / perNodeCount);
        }

    }

    static class ReplicationAwareTokenDistributor extends TokenDistributor
    {
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
            strategy.addNode(newNode);
            double optNodeOwnership = calcOptimalOwnership();
            double optTokenOwnership = optNodeOwnership / perNodeCount;
            Map<Object, RackInfo> racks = Maps.newHashMap();
            NodeInfo newNodeInfo = new NodeInfo(newNode, optNodeOwnership, racks, strategy);
            TokenInfo tokens = createTokenInfos(createNodeInfos(racks), newNodeInfo.rack);

            CandidateInfo candidates = createCandidates(tokens, newNodeInfo, totalTokenRange * strategy.replicas() / (nodeCount * perNodeCount));
            assert verifyTokenInfo(tokens);
            
            // Evaluate the expected improvements from all candidates and form a priority queue.
            PriorityQueue<Weighted<CandidateInfo>> improvements = new PriorityQueue<>(sortedTokens.size());
            CandidateInfo candidate = candidates;
            do
            {
                double impr = evaluateImprovement(candidate, optNodeOwnership, optTokenOwnership, 0);
                improvements.add(new Weighted<>(impr, candidate));
                candidate = candidate.next;
            } while (candidate != candidates);
            CandidateInfo bestToken = improvements.remove().value;
            candidates = bestToken.removeFrom(candidates);
            
            for (int vn = 0; vn < perNodeCount; ++vn)
            {
                // Use the token with the best improvement.
                adjustData(bestToken);
                sortedTokens.put(bestToken.token, newNode);
                if (vn == perNodeCount - 1)
                    return;
                
                for (;;)
                {
                    // Get the next candidate in the queue. Its improvement may have changed (esp. if multiple tokens
                    // were good suggestions because they could improve the same problem)-- evaluate it again to check
                    // if it is still a good candidate.
                    bestToken = improvements.remove().value;
                    candidates = bestToken.removeFrom(candidates);
                    double impr = evaluateImprovement(bestToken, optNodeOwnership, optTokenOwnership, vn);
                    double nextImpr = improvements.peek().weight;
                    
                    // If it is better than the next in the queue, it is good enough. This is a heuristic that doesn't
                    // get the best results, but works well enough and on average cuts search time by a factor of O(vnodes).
                    if (impr >= nextImpr)
                        break;
                    improvements.add(new Weighted<>(impr, bestToken));
                }
            }
            
            // Verify adjustData didn't do something wrong.
            assert verifyTokenInfo(tokens);
        }

        Map<Node, NodeInfo> createNodeInfos(Map<Object, RackInfo> racks)
        {
            Map<Node, NodeInfo> map = Maps.newHashMap();
            for (Node n: sortedTokens.values())
                if (!map.containsKey(n))
                    map.put(n, new NodeInfo(n, 0, racks, strategy));
            return map;
        }

        TokenInfo createTokenInfos(Map<Node, NodeInfo> nodes, RackInfo newNodeRack)
        {
            TokenInfo prev = null;
            TokenInfo first = null;
            for (Map.Entry<Token, Node> en: sortedTokens.entrySet())
            {
                Token t = en.getKey();
                NodeInfo ni = nodes.get(en.getValue());
                TokenInfo ti = new TokenInfo(t, ni);
                first = ti.insertAfter(first, prev);
                prev = ti;
            }

            TokenInfo curr = first;
            do {
                populateTokenInfoAndAdjustNode(curr, newNodeRack);
                curr = curr.next;
            } while (curr != first);

            return first;
        }

        CandidateInfo createCandidates(TokenInfo tokens, NodeInfo newNodeInfo, double optTokenOwnership)
        {
            TokenInfo curr = tokens;
            CandidateInfo first = null;
            CandidateInfo prev = null;
            do {
                CandidateInfo candidate = new CandidateInfo(curr.token.slice(curr.token.size(curr.next.token) / 2), curr, newNodeInfo);
                first = candidate.insertAfter(first, prev);
                
                candidate.replicatedOwnership = optTokenOwnership;
                populateCandidate(candidate);
                
                prev = candidate;
                curr = curr.next;
            } while (curr != tokens);
            prev.next = first;
            return first;
        }

        private void populateCandidate(CandidateInfo candidate)
        {
            // Only finding replication start would do.
            populateTokenInfo(candidate, candidate.owningNode.rack);
        }

        boolean verifyTokenInfo(TokenInfo tokens)
        {
            Map<Token, Token> replicationStart = Maps.newHashMap();
            Map<Node, Double> ownership = Maps.newHashMap();
            TokenInfo token = tokens;
            do {
                replicationStart.put(token.token, token.replicationStart);
                NodeInfo ni = token.owningNode;
                ownership.put(ni.node, ni.ownership);
                token = token.next;
            } while (token != tokens);
            return verifyReplicationStartMap(replicationStart) && verifyOwnership(ownership);
        }

        double printChangeStat(CandidateInfo candidate, double optNodeOwnership, int vn)
        {
            return printChangeStat(candidate.token, candidate.owningNode.node, new TreeMap<>(sortedTokens), optNodeOwnership, vn);
        }

        public void adjustData(CandidateInfo candidate)
        {
            // This process is less efficient than it could be (loops through each vnodes's replication span instead
            // of recalculating replicationStart, rfm1 and expandable from existing data + new token data in an O(1)
            // case analysis similar to evaluateImprovement). This is fine as the method does not dominate processing
            // time.
            
            // Put the accepted candidate in the token list.
            TokenInfo host = candidate.host;
            TokenInfo next = host.next;
            TokenInfo candidateToken = new TokenInfo(candidate.token, candidate.owningNode);
            candidateToken.replicatedOwnership = candidate.replicatedOwnership;
            candidateToken.insertAfter(host, host);   // List is not empty so this won't need to change head of list.

            // Update data for both candidate and host.
            NodeInfo newNode = candidateToken.owningNode;
            RackInfo newRack = newNode.rack;
            populateTokenInfoAndAdjustNode(candidateToken, newRack);
            NodeInfo hostNode = host.owningNode;
            RackInfo hostRack = hostNode.rack;
            populateTokenInfoAndAdjustNode(host, newRack);

            RackInfo rackChain = RackInfo.TERMINATOR;

            // Loop through all vnodes that replicate new token or host and update their data.
            // Also update candidate data for nodes in the replication span.
            int seenOld = 0;
            int seenNew = 0;
            int rf = strategy.replicas() - 1;
          evLoop:
            for (TokenInfo curr = next; seenOld < rf || seenNew < rf; curr = next)
            {
                candidate = candidate.next;
                populateCandidate(candidate);

                next = curr.next;
                NodeInfo currNode = curr.owningNode;
                RackInfo rack = currNode.rack;
                if (rack.prevSeen != null)
                    continue evLoop;    // If both have already seen this rack, nothing can change within it.
                if (rack != newRack)
                    seenNew += 1;
                if (rack != hostRack)
                    seenOld += 1;
                rack.prevSeen = rackChain;
                rackChain = rack;
                
                populateTokenInfoAndAdjustNode(curr, newRack);
            }
            
            // Clean rack seen markers.
            while (rackChain != RackInfo.TERMINATOR) {
                RackInfo prev = rackChain.prevSeen;
                rackChain.prevSeen = null;
                rackChain = prev;
            }
        }
        
        private Token populateTokenInfo(BaseTokenInfo<?> token, RackInfo newNodeRack)
        {
            RackInfo rackChain = RackInfo.TERMINATOR;
            RackInfo tokenRack = token.owningNode.rack;
            int seenRacks = 0;
            boolean expandable = false;
            int rf = strategy.replicas();
            // Replication start = the end of a token from the RF'th different rack seen before the token.
            Token rs = token.token;
            // The end of a token from the RF-1'th different rack seen before the token.
            Token rfm1 = rs;
            for (TokenInfo curr = token.prevInRing();;rs = curr.token, curr = curr.prev) {
                RackInfo ri = curr.owningNode.rack;
                if (ri.prevPopulate != null)
                    continue; // Rack is already seen.
                // Mark the rack as seen. Also forms a chain that can be used to clear the marks when we are done.
                // We use prevPopulate instead of prevSeen as this may be called within adjustData which also needs
                // rack seen markers.
                ri.prevPopulate = rackChain;
                rackChain = ri;
                if (++seenRacks == rf)
                    break;

                rfm1 = rs;
                // Another instance of the same rack is a replication boundary.
                if (ri == tokenRack)
                {
                    // Inserting a token that ends at this boundary will increase replication coverage,
                    // but only if the inserted node is not in the same rack.
                    expandable = tokenRack != newNodeRack;
                    break;
                }
                // An instance of the new rack in the replication span also means that we can expand coverage
                // by inserting a token ending at replicationStart.
                if (ri == newNodeRack)
                    expandable = true;
            }
            token.rfm1Token = rfm1;
            token.replicationStart = rs;
            token.expandable = expandable;

            // Clean rack seen markers.
            while (rackChain != RackInfo.TERMINATOR)
            {
                RackInfo prev = rackChain.prevPopulate;
                rackChain.prevPopulate  = null;
                rackChain = prev;
            }
            return rs;
        }
        
        private void populateTokenInfoAndAdjustNode(TokenInfo candidate, RackInfo newNodeRack)
        {
            Token rs = populateTokenInfo(candidate, newNodeRack);
            double newOwnership = rs.size(candidate.next.token);
            double oldOwnership = candidate.replicatedOwnership;
            candidate.replicatedOwnership = newOwnership;
            candidate.owningNode.ownership += newOwnership - oldOwnership;
        }

        double evaluateImprovement(CandidateInfo candidate, double optNodeOwnership, double optTokenOwnership, int vn)
        {
            double change = 0;
            TokenInfo host = candidate.host;
            TokenInfo next = host.next;
            Token cend = next.token;

            // Reflect change in ownership of the splitting token (candidate).
            double oldOwnership = candidate.replicatedOwnership;
            double newOwnership = candidate.replicationStart.size(next.token);
            change += sq(newOwnership - optTokenOwnership) - sq(oldOwnership - optTokenOwnership);
            NodeInfo newNode = candidate.owningNode;
            newNode.adjustedOwnership = newNode.ownership + newOwnership - oldOwnership;
            
            // Reflect change of ownership in the token being split (host).
            oldOwnership = host.replicatedOwnership;
            newOwnership = host.replicationStart.size(candidate.token);
            change += sq(newOwnership - optTokenOwnership) - sq(oldOwnership - optTokenOwnership);
            NodeInfo hostNode = host.owningNode;
            hostNode.adjustedOwnership = hostNode.ownership + newOwnership - oldOwnership;

            // Form a chain of nodes affected by the insertion to be able to qualify change of node ownership.
            // A node may be affected more than once.
            assert newNode.prevUsed == null;
            newNode.prevUsed = newNode;   // end marker
            RackInfo newRack = newNode.rack;
            assert hostNode.prevUsed == null;
            hostNode.prevUsed = newNode;
            RackInfo hostRack = hostNode.rack;
            NodeInfo nodesChain = hostNode;

            // Loop through all vnodes that replicate candidate or host and update their ownership.
            int seenOld = 0;
            int seenNew = 0;
            int rf = strategy.replicas() - 1;
          evLoop:
            for (TokenInfo curr = next; seenOld < rf || seenNew < rf; curr = next)
            {
                next = curr.next;
                NodeInfo currNode = curr.owningNode;
                RackInfo rack = currNode.rack;
                if (rack.prevSeen != null)
                    continue evLoop;    // If both have already seen this rack, nothing can change within it.
                if (rack != newRack)
                    seenNew += 1;
                if (rack != hostRack)
                    seenOld += 1;
                rack.prevSeen = rack;   // Just mark it as seen, we are not forming a chain of racks.
                
                if (currNode.prevUsed == null)
                {
                    currNode.adjustedOwnership = currNode.ownership;
                    currNode.prevUsed = nodesChain;
                    nodesChain = currNode;
                }

                Token rs;
                if (curr.expandable) // Sees same or new rack before rf-1.
                {
                    if (cend != curr.replicationStart)
                        continue evLoop;
                    // Replication expands to start of candidate.
                    rs = candidate.token;
                } else {
                    if (rack == newRack)
                    {
                        if (!preceeds(curr.replicationStart, cend, next.token))
                            continue evLoop; // no changes, another newRack is closer
                        // Replication shrinks to end of candidate.
                        rs = cend;
                    } else {
                        if (preceeds(curr.rfm1Token, cend, next.token))
                            // Candidate is closer than one-before last.
                            rs = curr.rfm1Token;
                        else if (preceeds(cend, curr.rfm1Token, next.token))
                            // Candidate is the replication boundary.
                            rs = cend;
                        else
                            // Candidate replaces one-before last. The latter becomes the replication boundary.
                            rs = candidate.token;
                    }
                }

                // Calculate ownership adjustments.
                oldOwnership = curr.replicatedOwnership;
                newOwnership = rs.size(next.token);
                change += sq(newOwnership - optTokenOwnership) - sq(oldOwnership - optTokenOwnership);
                currNode.adjustedOwnership += newOwnership - oldOwnership;
            }

            // Now loop through the nodes chain and add the node-level changes. Also clear the racks' seen marks.
            for (;;) {
                newOwnership = nodesChain.adjustedOwnership;
                oldOwnership = nodesChain.ownership;
                double diff = sq(newOwnership - optNodeOwnership) - sq(oldOwnership - optNodeOwnership);
                NodeInfo prev = nodesChain.prevUsed;
                nodesChain.prevUsed = null;
                nodesChain.rack.prevSeen = null;
                if (nodesChain != newNode)
                    change += diff;
                else 
                {
                    change += (diff * (vn + 1)) / perNodeCount;
                    break;
                }
                nodesChain = prev;
            }

            return -change;
        }
        
        public double calcOptimalOwnership()
        {
            return totalTokenRange * strategy.replicas() / nodeCount;
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
        
    }
    
    static class ReplicationAwareTokenDistributorTryAll extends ReplicationAwareTokenDistributor
    {

        public ReplicationAwareTokenDistributorTryAll(NavigableMap<Token, Node> sortedTokens, ReplicationStrategy strategy,
                int perNodeCount)
        {
            super(sortedTokens, strategy, perNodeCount);
        }
        
        void addNode(Node newNode)
        {
            ++nodeCount;
            strategy.addNode(newNode);
            double optNodeOwnership = calcOptimalOwnership();
            double optTokenOwnership = optNodeOwnership / perNodeCount;
            Map<Object, RackInfo> racks = Maps.newHashMap();
            NodeInfo newNodeInfo = new NodeInfo(newNode, optNodeOwnership, racks, strategy);
            TokenInfo tokens = createTokenInfos(createNodeInfos(racks), newNodeInfo.rack);

            CandidateInfo candidates = createCandidates(tokens, newNodeInfo, optTokenOwnership);
            if (debug >= 3)
            {
                dumpTokens("E", tokens);
                dumpTokens("C", candidates);
            }
            assert verifyTokenInfo(tokens);

            // For each vnode to add, try all choices and pick the one that gives best improvement.
            for (int vn = 0; vn < perNodeCount; ++vn)
            {
                CandidateInfo bestToken = null;
                double bestImprovement = Double.NEGATIVE_INFINITY;
                CandidateInfo candidate = candidates;
                do
                {
                    if (debug >= 5)
                    {
                        double expImpr = printChangeStat(candidate, optNodeOwnership, vn);
                        double impr = evaluateImprovement(candidate, optNodeOwnership, optTokenOwnership, vn);
                        if (Math.abs((impr / expImpr) - 1) > 0.000001)
                        {
                            System.out.format("Evaluation wrong: %.6f vs %.6f\n", impr/sq(optNodeOwnership), expImpr/sq(optNodeOwnership));
                        }
                    }
                    double impr = evaluateImprovement(candidate, optNodeOwnership, optTokenOwnership, vn);
                    if (impr > bestImprovement)
                    {
                        bestToken = candidate;
                        bestImprovement = impr;
                    }
                    candidate = candidate.next;
                } while (candidate != candidates);

                if (debug >= 3) {
                    System.out.print("Selected ");printChangeStat(bestToken, optNodeOwnership, vn);
                }

                adjustData(bestToken);
                candidates = bestToken.removeFrom(candidates);
                sortedTokens.put(bestToken.token, newNode);
                if (debug >= 5)
                {
                    dumpTokens("E", tokens);
                    dumpTokens("C", candidates);
                }
            }
            
            // Verify adjustData didn't do something wrong.
            assert verifyTokenInfo(tokens);
        }

    }

    private static Token max(Token t1, Token t2, Token m)
    {
        return m.size(t1) >= m.size(t2) ? t1 : t2;
    }
    
    private static boolean preceeds(Token t1, Token t2, Token towards)
    {
        return t1.size(towards) > t2.size(towards);
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
        return String.format("max %.2f min %.2f max:min %.2f stddev %.5f sq %.5f next %.5f",
                             stat.getMax(),
                             stat.getMin(),
                             stat.getMax() / stat.getMin(),
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
    }

    public static void main1(String[] args)
    {
        int perNodeCount = 256;
//        for (perNodeCount = 1; perNodeCount <= 1024; perNodeCount *= 2)
        {
            final int targetClusterSize = 1000;
            int rf = 3;
            NavigableMap<Token, Node> tokenMap = Maps.newTreeMap();
    
    //        perfectDistribution(tokenMap, targetClusterSize, perNodeCount);
            boolean locallyRandom = false;
            random(tokenMap, targetClusterSize, perNodeCount, locallyRandom);
    
            Set<Node> nodes = Sets.newTreeSet(tokenMap.values());
            TokenDistributor[] t = {
//                new ReplicationAwareTokenDistributorTryAll(tokenMap, new SimpleReplicationStrategy(rf), perNodeCount),
                new ReplicationAwareTokenDistributor(tokenMap, new SimpleReplicationStrategy(rf), perNodeCount),
            };
//            t[0].debug = 5;
            if (nodes.size() < targetClusterSize)
                test(t, nodes.size());
    
            test(t, targetClusterSize);
    
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

    public static void main(String[] args)
    {
        int perNodeCount = 16;
//        for (perNodeCount = 1; perNodeCount <= 256; perNodeCount *= 4)
        {
            final int targetClusterSize = 500;
            int rf = 3;
            int initialSize = rf;
            NavigableMap<Token, Node> tokenMap = Maps.newTreeMap();
    
    //        perfectDistribution(tokenMap, targetClusterSize, perNodeCount);
            boolean locallyRandom = false;
            random(tokenMap, initialSize, perNodeCount, locallyRandom);
    
            TokenDistributor[] t = {
//                    new ReplicationAwareTokenDistributorTryAll(tokenMap, new SimpleReplicationStrategy(rf), perNodeCount),
                    new ReplicationAwareTokenDistributor(tokenMap, new SimpleReplicationStrategy(rf), perNodeCount),
                    new ReplicationAwareTokenDistributor2(tokenMap, new SimpleReplicationStrategy(rf), perNodeCount),
            };
            t[1].debug = 5;
            for (int i=initialSize; i<=targetClusterSize; i+=1)
                test(t, i);
            
//            testLoseAndReplace(t, targetClusterSize / 4);
        }
    }

    private static void testLoseAndReplace(TokenDistributor[] ts, int howMany)
    {
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
        if (size < targetClusterSize) {
            System.out.format("Adding %d node(s) using %s...", targetClusterSize - size, t.toString());
            long time = System.currentTimeMillis();
            while (size < targetClusterSize)
            {
                t.addNode(new Node());
                ++size;
            }
            System.out.format(" Done in %.3fs\n", (System.currentTimeMillis() - time) / 1000.0);
        }
        printDistribution(t);
    }
}
