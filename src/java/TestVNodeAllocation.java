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
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
    
    interface ReplicationStrategy {
        List<Node> getReplicas(Token token, NavigableMap<Token, Node> sortedTokens);

        double replicas();
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

            if (sortedTokens.isEmpty())
                return endpoints;

            // Add the token at the index by default
            assert sortedTokens.get(token) != null;
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

        public String toString()
        {
            return String.format("Simple %d replicas", replicas);
        }

        @Override
        public double replicas()
        {
            return replicas;
        }
    }
    
    static class RackReplicationStrategy implements ReplicationStrategy
    {
        final int replicas;
        final Map<Node, Integer> rackMap;

        public RackReplicationStrategy(int replicas, Map<Node, Integer> rackMap)
        {
            this.replicas = replicas;
            this.rackMap = rackMap;
        }

        public List<Node> getReplicas(Token token, NavigableMap<Token, Node> sortedTokens)
        {
            List<Node> endpoints = new ArrayList<Node>(replicas);
            BitSet usedRacks = new BitSet();

            if (sortedTokens.isEmpty())
                return endpoints;

            // Add the token at the index by default
            assert sortedTokens.get(token) != null;
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

        public String toString()
        {
            Map<Integer, Integer> idToSize = instanceToCount(rackMap);
            Map<Integer, Integer> sizeToCount = Maps.newTreeMap();
            sizeToCount.putAll(instanceToCount(idToSize));
            return String.format("Rack strategy, %d replicas, rack size to count %s", replicas, sizeToCount);
        }

        @Override
        public double replicas()
        {
            return replicas;
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

    static class BalancedRackReplicationStrategy extends RackReplicationStrategy
    {
        public BalancedRackReplicationStrategy(int replicas, int rackSize, Collection<Node> nodes)
        {
            super(replicas, Maps.newHashMap());
            assert nodes.size() >= rackSize * replicas;
            int rackId = 0;
            for (Node n : nodes)
                rackMap.put(n, rackId++ / rackSize);
        }
    }
    
    static class UnbalancedRackReplicationStrategy extends RackReplicationStrategy
    {
        public UnbalancedRackReplicationStrategy(int replicas, int minRackSize, int maxRackSize, Collection<Node> nodes)
        {
            super(replicas, Maps.newHashMap());
//            assert nodes.size() >= rackSize * replicas;
            int rackId = -1;
            int nextSize = 0;
            int num = 0;
            Random rnd = ThreadLocalRandom.current();
            
            for (Node n : nodes) {
                if (++num > nextSize) {
                    nextSize = minRackSize + rnd.nextInt(maxRackSize - minRackSize + 1);
                    ++rackId;
                    num = 0;
                }
                rackMap.put(n, rackId);
            }
        }
    }
    
    
    static class TokenDistributor {
        
        NavigableMap<Token, Node> sortedTokens;
        ReplicationStrategy strategy;
        int perNodeCount;
        
        public TokenDistributor(NavigableMap<Token, Node> sortedTokens, ReplicationStrategy strategy, int perNodeCount)
        {
            this.sortedTokens = sortedTokens;
            this.strategy = strategy;
            this.perNodeCount = perNodeCount;
        }
        
        void addNode(Node newNode)
        {}

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

        double tokenSize(Token token)
        {
            Token next = sortedTokens.higherKey(token);
            if (next == null)
                next = sortedTokens.firstKey();
            return token.size(next);
        }

        public int nodeCount()
        {
            return (int) sortedTokens.values().stream().distinct().count();
        }
    }
    
    static class Weighted<T extends Comparable<T>> implements Comparable<Weighted<T>>{
        final double weight;
        final T node;

        public Weighted(double weight, T node)
        {
            this.weight = weight;
            this.node = node;
        }

        @Override
        public int compareTo(Weighted<T> o)
        {
            int cmp = Double.compare(o.weight, this.weight);
            if (cmp == 0) {
                cmp = node.compareTo(o.node);
            }
            return cmp;
        }

        @Override
        public String toString()
        {
            return String.format("%s<%s>", node, weight);
        }
    }
    
    static class ReplicationAgnosticTokenDistributor extends TokenDistributor {
        SortedSet<Weighted<Node>> sortedNodes;
        final Map<Node, SortedSet<Weighted<Token>>> tokensInNode;
        
        
        public ReplicationAgnosticTokenDistributor(NavigableMap<Token, Node> sortedTokens, ReplicationStrategy strategy, int perNodeCount)
        {
            super(sortedTokens, strategy, perNodeCount);
            tokensInNode = Maps.newHashMap();
            sortedNodes = Sets.newTreeSet();
            for (Map.Entry<Token, Node> en : sortedTokens.entrySet()) {
                Token token = en.getKey();
                Node node = en.getValue();
                addWeightedToken(node, token);
            }
            tokensInNode.forEach(this::addToSortedNodes);
            
            Map<Node, Double> ownership = evaluateReplicatedOwnership();
            sortedNodes = new TreeSet<>();
            ownership.forEach((n, w) -> sortedNodes.add(new Weighted<>(w, n)));
        }

        private boolean addToSortedNodes(Node node, SortedSet<Weighted<Token>> set)
        {
            return sortedNodes.add(new Weighted<Node>(set.stream().mapToDouble(wt -> wt.weight).sum(), node));
        }

        private void addWeightedToken(Node node, Token token)
        {
            SortedSet<Weighted<Token>> nodeTokens = tokensInNode.get(node);
            if (nodeTokens == null) {
                nodeTokens = Sets.newTreeSet();
                tokensInNode.put(node, nodeTokens);
            }
            Weighted<Token> wt = new Weighted<Token>(tokenSize(token), token);
            nodeTokens.add(wt);
        }

        void addNode(Node newNode)
        {
            int applyTo = perNodeCount;
            double targetAverage;
            for (;;) {
                // FIXME: There's a one-pass version of this.
                final double target = sortedNodes.stream().limit(applyTo).mapToDouble(n -> n.weight).sum() / (applyTo + 1);
                int countAbove = (int) sortedNodes.stream().limit(applyTo).filter(n -> (n.weight > target)).count();
                targetAverage = target;
                if (countAbove == applyTo)
                    break;
                applyTo = countAbove;
            }
            // Now we want to insert tokens so that the largest nodes (+ new node) become targetAverage size.
            List<Weighted<Node>> nodes = Lists.newArrayListWithCapacity(applyTo);
            for (int i=0; i<applyTo; ++i) {
                Weighted<Node> wn = sortedNodes.first();
                sortedNodes.remove(wn);
                nodes.add(wn);
            }
            
            int nr = 0;
            for (Weighted<Node> n: nodes) {
                int tokensToChange = perNodeCount / applyTo + (nr < perNodeCount % applyTo ? 1 : 0);
                
                SortedSet<Weighted<Token>> nodeTokens = tokensInNode.get(n.node);
                List<Weighted<Token>> tokens = Lists.newArrayListWithCapacity(tokensToChange);
                double workWeight = 0;
                for (int i=0; i < tokensToChange; ++i) {
                    Weighted<Token> wt = nodeTokens.first();
                    nodeTokens.remove(wt);
                    tokens.add(wt);
                    workWeight += wt.weight;
                }
                double toTakeOver = n.weight - targetAverage;
                while (tokensToChange > 0) {
                    Weighted<Token> wt = tokens.get(tokensToChange - 1);
                    double slice;
                    if (toTakeOver < workWeight) {
                        // Spread decrease.
                        slice = wt.weight - (toTakeOver * wt.weight / workWeight);
                    } else {
                        // Effectively take over spans, best we can do.
                        slice = 0;
                    }
                    Token t = wt.node.slice(slice);
                    // Token selected. Now change all data.

                    sortedTokens.put(t, newNode);
                    // This changes nodeTokens.
                    addWeightedToken(n.node, wt.node);
                    addWeightedToken(newNode, t);
                    --tokensToChange;
                }
                
                addToSortedNodes(n.node, nodeTokens);
                ++nr;
            }
            addToSortedNodes(newNode, tokensInNode.get(newNode));
//            System.out.println("Nodes after add: " + sortedNodes);
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
        final int targetClusterSize = 1000;
        int perNodeCount = 32;
        NavigableMap<Token, Node> tokenMap = Maps.newTreeMap();
        ReplicationStrategy rs = new SimpleReplicationStrategy(1);
        boolean locallyRandom = false;
        random(tokenMap, targetClusterSize, perNodeCount, locallyRandom);
        TokenDistributor t = new ReplicationAgnosticTokenDistributor(tokenMap, rs, perNodeCount);
        test(t, targetClusterSize);

        tokenMap.clear();
        oneNodePerfectDistribution(tokenMap, perNodeCount);
        t = new ReplicationAgnosticTokenDistributor(tokenMap, rs, perNodeCount);
        test(t, targetClusterSize);

        tokenMap.clear();
        random(tokenMap, targetClusterSize/2, perNodeCount, locallyRandom);
        t = new ReplicationAgnosticTokenDistributor(tokenMap, rs, perNodeCount);
        test(t, targetClusterSize);

        tokenMap.clear();
        random(tokenMap, targetClusterSize*19/20, perNodeCount, locallyRandom);
        t = new ReplicationAgnosticTokenDistributor(tokenMap, rs, perNodeCount);
        test(t, targetClusterSize);
    }

    public static void test(TokenDistributor t, int targetClusterSize)
    {
        int size = t.nodeCount();
        while (size < targetClusterSize)
        {
            t.addNode(new Node());
            ++size;
        }
        printDistribution(t);
        Set<Node> nodes = Sets.newHashSet(t.sortedTokens.values());
        printDistribution(t, new SimpleReplicationStrategy(3));
//        printDistribution(t, new SimpleReplicationStrategy(5));
//        printDistribution(t, new SimpleReplicationStrategy(17));
//        printDistribution(t, new BalancedRackReplicationStrategy(3, 4, nodes));
        printDistribution(t, new BalancedRackReplicationStrategy(3, 8, nodes));
//        printDistribution(t, new BalancedRackReplicationStrategy(3, 16, nodes));
//        printDistribution(t, new BalancedRackReplicationStrategy(3, 64, nodes));
//        printDistribution(t, new BalancedRackReplicationStrategy(5, 16, nodes));
//        printDistribution(t, new BalancedRackReplicationStrategy(17, 16, nodes));
//        printDistribution(t, new UnbalancedRackReplicationStrategy(3, 16, 32, nodes));
//        printDistribution(t, new UnbalancedRackReplicationStrategy(3, 8, 16, nodes));
//        printDistribution(t, new UnbalancedRackReplicationStrategy(3, 2, 6, nodes));
    }

    private static final ThreadLocalRandom rand = ThreadLocalRandom.current();

    private static double stdev(double[] vals)
    {
        double sumsquares = 0, sum = 0, mean = 0, stdev = 0;
        for (double value : vals)
        {
            sumsquares += value * value;
            sum += value;
        }
        mean = sum / vals.length;
        stdev = Math.sqrt((sumsquares / vals.length) - (mean * mean));
        return stdev;
    }
    private static double avg(double[] vals)
    {
        double sum = 0;
        for (double value : vals)
            sum += value;
        double mean = sum / vals.length;
        return mean;
    }
    private static double max(double[] vals)
    {
        double max = -Double.MAX_VALUE;
        for (double value : vals)
            max = Math.max(max, value);
        return max;
    }
    private static double min(double[] vals)
    {
        double min = Double.MAX_VALUE;
        for (double value : vals)
            min = Math.min(min, value);
        return min;
    }
}
