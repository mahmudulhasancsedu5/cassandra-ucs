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
package org.apache.cassandra.dht.tokenallocator;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.tokenallocator.TokenAllocator.ReplicationStrategy;
import org.apache.cassandra.dht.tokenallocator.ReplicationAwareTokenAllocator;

public class ReplicationAwareTokenAllocatorTest
{
    interface TestReplicationStrategy extends ReplicationStrategy<Unit> {
        void addUnit(Unit n);
        void removeUnit(Unit n);

        /**
         * Returns a list of all replica units for given token.
         */
        List<Unit> getReplicas(Token token, NavigableMap<Token, Unit> sortedTokens);

        /**
         * Returns the start of the token span that is replicated in this token.
         * Note: Though this is not trivial to see, the replicated span is always contiguous. A token in the same
         * group acts as a barrier; if one is not found the token replicates everything up to the replica'th distinct
         * group seen in front of it.
         */
        Token replicationStart(Token token, Unit unit, NavigableMap<Token, Unit> sortedTokens);
    }
    
    static class NoReplicationStrategy implements TestReplicationStrategy
    {
        public List<Unit> getReplicas(Token token, NavigableMap<Token, Unit> sortedTokens)
        {
            return Collections.singletonList(sortedTokens.floorEntry(token).getValue());
        }

        public Token lastReplicaToken(Token token, NavigableMap<Token, Unit> sortedTokens)
        {
            return sortedTokens.floorEntry(token).getKey();
        }

        public Token replicationStart(Token token, Unit unit, NavigableMap<Token, Unit> sortedTokens)
        {
            return token;
        }

        public String toString()
        {
            return "No replication";
        }
        
        public void addUnit(Unit n) {}
        public void removeUnit(Unit n) {}

        public int replicas()
        {
            return 1;
        }

        public boolean sameGroup(Unit n1, Unit n2)
        {
            return false;
        }

        public Object getGroup(Unit unit)
        {
            return unit;
        }
    }
    
    static class SimpleReplicationStrategy implements TestReplicationStrategy
    {
        int replicas;

        public SimpleReplicationStrategy(int replicas)
        {
            super();
            this.replicas = replicas;
        }

        public List<Unit> getReplicas(Token token, NavigableMap<Token, Unit> sortedTokens)
        {
            List<Unit> endpoints = new ArrayList<Unit>(replicas);

            token = sortedTokens.floorKey(token);
            if (token == null)
                token = sortedTokens.lastKey();
            Iterator<Unit> iter = Iterables.concat(sortedTokens.tailMap(token, true).values(), sortedTokens.values()).iterator();
            while (endpoints.size() < replicas)
            {
                // presumably list can't be exhausted before finding all replicas.
                Unit ep = iter.next();
                if (!endpoints.contains(ep))
                    endpoints.add(ep);
            }
            return endpoints;
        }

        public Token lastReplicaToken(Token token, NavigableMap<Token, Unit> sortedTokens)
        {
            Set<Unit> endpoints = new HashSet<Unit>(replicas);

            token = sortedTokens.floorKey(token);
            if (token == null)
                token = sortedTokens.lastKey();
            for (Map.Entry<Token, Unit> en :
                Iterables.concat(sortedTokens.tailMap(token, true).entrySet(),
                                 sortedTokens.entrySet()))
            {
                Unit ep = en.getValue();
                if (!endpoints.contains(ep)){
                    endpoints.add(ep);
                    if (endpoints.size() >= replicas)
                        return en.getKey();
                }
            }
            return token;
        }

        public Token replicationStart(Token token, Unit unit, NavigableMap<Token, Unit> sortedTokens)
        {
            Set<Unit> seenUnits = Sets.newHashSet();
            int unitsFound = 0;

            for (Map.Entry<Token, Unit> en : Iterables.concat(
                     sortedTokens.headMap(token, false).descendingMap().entrySet(),
                     sortedTokens.descendingMap().entrySet())) {
                Unit n = en.getValue();
                // Same group as investigated unit is a break; anything that could replicate in it replicates there.
                if (n == unit)
                    break;

                if (seenUnits.add(n))
                {
                    if (++unitsFound == replicas)
                        break;
                }
                token = en.getKey();
            }
            return token;
        }

        public void addUnit(Unit n) {}
        public void removeUnit(Unit n) {}

        public String toString()
        {
            return String.format("Simple %d replicas", replicas);
        }

        public int replicas()
        {
            return replicas;
        }

        public boolean sameGroup(Unit n1, Unit n2)
        {
            return false;
        }

        public Unit getGroup(Unit unit)
        {
            // The unit is the group.
            return unit;
        }
    }
    
    static abstract class GroupReplicationStrategy implements TestReplicationStrategy
    {
        final int replicas;
        final Map<Unit, Integer> groupMap;

        public GroupReplicationStrategy(int replicas)
        {
            this.replicas = replicas;
            this.groupMap = Maps.newHashMap();
        }

        public List<Unit> getReplicas(Token token, NavigableMap<Token, Unit> sortedTokens)
        {
            List<Unit> endpoints = new ArrayList<Unit>(replicas);
            BitSet usedGroups = new BitSet();

            if (sortedTokens.isEmpty())
                return endpoints;

            token = sortedTokens.floorKey(token);
            if (token == null)
                token = sortedTokens.lastKey();
            Iterator<Unit> iter = Iterables.concat(sortedTokens.tailMap(token, true).values(), sortedTokens.values()).iterator();
            while (endpoints.size() < replicas)
            {
                // For simlicity assuming list can't be exhausted before finding all replicas.
                Unit ep = iter.next();
                int group = groupMap.get(ep);
                if (!usedGroups.get(group))
                {
                    endpoints.add(ep);
                    usedGroups.set(group);
                }
            }
            return endpoints;
        }

        public Token lastReplicaToken(Token token, NavigableMap<Token, Unit> sortedTokens)
        {
            BitSet usedGroups = new BitSet();
            int groupsFound = 0;

            token = sortedTokens.floorKey(token);
            if (token == null)
                token = sortedTokens.lastKey();
            for (Map.Entry<Token, Unit> en :
                Iterables.concat(sortedTokens.tailMap(token, true).entrySet(),
                                 sortedTokens.entrySet()))
            {
                Unit ep = en.getValue();
                int group = groupMap.get(ep);
                if (!usedGroups.get(group)){
                    usedGroups.set(group);
                    if (++groupsFound >= replicas)
                        return en.getKey();
                }
            }
            return token;
        }

        public Token replicationStart(Token token, Unit unit, NavigableMap<Token, Unit> sortedTokens)
        {
            // replicated ownership
            int unitGroup = groupMap.get(unit);   // unit must be already added
            BitSet seenGroups = new BitSet();
            int groupsFound = 0;

            for (Map.Entry<Token, Unit> en : Iterables.concat(
                     sortedTokens.headMap(token, false).descendingMap().entrySet(),
                     sortedTokens.descendingMap().entrySet())) {
                Unit n = en.getValue();
                int ngroup = groupMap.get(n);
                // Same group as investigated unit is a break; anything that could replicate in it replicates there.
                if (ngroup == unitGroup)
                    break;

                if (!seenGroups.get(ngroup))
                {
                    if (++groupsFound == replicas)
                        break;
                    seenGroups.set(ngroup);
                }
                token = en.getKey();
            }
            return token;
        }

        public String toString()
        {
            Map<Integer, Integer> idToSize = instanceToCount(groupMap);
            Map<Integer, Integer> sizeToCount = Maps.newTreeMap();
            sizeToCount.putAll(instanceToCount(idToSize));
            return String.format("Group strategy, %d replicas, group size to count %s", replicas, sizeToCount);
        }

        @Override
        public int replicas()
        {
            return replicas;
        }

        public boolean sameGroup(Unit n1, Unit n2)
        {
            return groupMap.get(n1).equals(groupMap.get(n2));
        }

        public void removeUnit(Unit n) {
            groupMap.remove(n);
        }

        public Integer getGroup(Unit unit)
        {
            return groupMap.get(unit);
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
     * Group strategy spreading units into a fixed number of groups.
     */
    static class FixedGroupCountReplicationStrategy extends GroupReplicationStrategy
    {
        int groupId;
        int groupCount;

        public FixedGroupCountReplicationStrategy(int replicas, int groupCount, Collection<Unit> units)
        {
            super(replicas);
            assert groupCount >= replicas;
            groupId = 0;
            this.groupCount = groupCount;
            for (Unit n : units)
                addUnit(n);
        }

        public void addUnit(Unit n)
        {
            groupMap.put(n, groupId++ % groupCount);
        }
    }

    /**
     * Group strategy with a fixed number of units per group.
     */
    static class BalancedGroupReplicationStrategy extends GroupReplicationStrategy
    {
        int groupId;
        int groupSize;

        public BalancedGroupReplicationStrategy(int replicas, int groupSize, Collection<Unit> units)
        {
            super(replicas);
            assert units.size() >= groupSize * replicas;
            groupId = 0;
            this.groupSize = groupSize;
            for (Unit n : units)
                addUnit(n);
        }

        public void addUnit(Unit n)
        {
            groupMap.put(n, groupId++ / groupSize);
        }
    }
    
    static class UnbalancedGroupReplicationStrategy extends GroupReplicationStrategy
    {
        int groupId;
        int nextSize;
        int num;
        int minGroupSize;
        int maxGroupSize;
        
        public UnbalancedGroupReplicationStrategy(int replicas, int minGroupSize, int maxGroupSize, Collection<Unit> units)
        {
            super(replicas);
            assert units.size() >= maxGroupSize * replicas;
            groupId = -1;
            nextSize = 0;
            num = 0;
            this.maxGroupSize = maxGroupSize;
            this.minGroupSize = minGroupSize;
            
            for (Unit n : units)
                addUnit(n);
        }

        public void addUnit(Unit n)
        {
            if (++num > nextSize) {
                nextSize = minGroupSize + ThreadLocalRandom.current().nextInt(maxGroupSize - minGroupSize + 1);
                ++groupId;
                num = 0;
            }
            groupMap.put(n, groupId);
        }
    }
    
    static IPartitioner partitioner = new Murmur3Partitioner();
    
    private static void random(Map<Token, Unit> map, int unitCount, int perUnitCount)
    {
        System.out.format("\nRandom generation of %d units with %d tokens each\n", unitCount, perUnitCount);
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        for (int i = 0 ; i < unitCount ; i++)
        {
            Unit unit = new Unit();
            int tokens = tokenCount(perUnitCount, rand);
            for (int j = 0 ; j < tokens ; j++)
            {
                map.put(partitioner.getRandomToken(), unit);
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

    private static double sq(double d)
    {
        return d*d;
    }

    static Map<Unit, Double> evaluateReplicatedOwnership(ReplicationAwareTokenAllocator<Unit> t)
    {
        Map<Unit, Double> ownership = Maps.newHashMap();
        Iterator<Token> it = t.sortedTokens.keySet().iterator();
        Token current = it.next();
        while (it.hasNext()) {
            Token next = it.next();
            addOwnership(t, current, next, ownership);
            current = next;
        }
        addOwnership(t, current, t.sortedTokens.firstKey(), ownership);

        return ownership;
    }

    private static void addOwnership(ReplicationAwareTokenAllocator<Unit> t, Token current, Token next, Map<Unit, Double> ownership)
    {
        TestReplicationStrategy ts = (TestReplicationStrategy) t.strategy;
        double size = current.size(next);
        for (Unit n : ts.getReplicas(current, t.sortedTokens)) {
            Double v = ownership.get(n);
            ownership.put(n, v != null ? v + size : size);
        }
    }

    private static double replicatedTokenOwnership(Token token, NavigableMap<Token, Unit> sortedTokens, ReplicationStrategy<Unit> strategy)
    {
        TestReplicationStrategy ts = (TestReplicationStrategy) strategy;
        Token next = sortedTokens.higherKey(token);
        if (next == null)
            next = sortedTokens.firstKey();
        return ts.replicationStart(token, sortedTokens.get(token), sortedTokens).size(next);
    }

    private static void printDistribution(ReplicationAwareTokenAllocator<Unit> t)
    {
        Map<Unit, Double> ownership = evaluateReplicatedOwnership(t);
        ownership.replaceAll((n, w) -> w / t.unitToTokens.get(n).size());
        int size = t.sortedTokens.size();
        double inverseAverage = 1.0 * size / t.strategy.replicas();
        List<Double> tokenOwnership = Lists.newArrayList(
                t.sortedTokens.keySet().stream().mapToDouble(
                        tok -> replicatedTokenOwnership(tok, t.sortedTokens, t.strategy)).iterator());
        System.out.format("Size %d(%d)   unit %s  token %s   %s\n",
                          t.unitCount(), size,
                          stats(ownership.values(), inverseAverage, 1),
                          stats(tokenOwnership, inverseAverage, 1),
                          t.strategy);
    }
    
    // TODO: Turn this file into a test, testing:
    // -- different vnode counts
    // -- fixed pernodecount as well as changing
    // -- simple strategy + rack strategy, rack sizes 4 to 64
    // -- rf 1 through 5
    // -- starting from 0 nodes (auto-randoming), starting from 500 nodes
    
    public static void main(String[] args)
    {
        int perUnitCount = 16;
        for (perUnitCount = 1; perUnitCount <= 1024; perUnitCount *= 4)
        {
            final int targetClusterSize = 500;
            int rf = 3;
            NavigableMap<Token, Unit> tokenMap = Maps.newTreeMap();
    
            random(tokenMap, targetClusterSize, perUnitCount);
    
            Set<Unit> units = Sets.newTreeSet(tokenMap.values());

            ReplicationAwareTokenAllocator<Unit> t = new ReplicationAwareTokenAllocator<>(tokenMap, new SimpleReplicationStrategy(rf), partitioner);
            if (units.size() < targetClusterSize)
                test(t, units.size(), perUnitCount);
    
            test(t, targetClusterSize, perUnitCount);
    
            test(t, targetClusterSize + 1, perUnitCount);
    
            test(t, targetClusterSize * 101 / 100, perUnitCount);
    
            test(t, targetClusterSize * 26 / 25, perUnitCount);
    
            test(t, targetClusterSize * 5 / 4, perUnitCount);
    
            test(t, targetClusterSize * 2, perUnitCount);
    
            testLoseAndReplace(t, 1, perUnitCount);
            testLoseAndReplace(t, targetClusterSize / 100, perUnitCount);
            testLoseAndReplace(t, targetClusterSize / 25, perUnitCount);
            testLoseAndReplace(t, targetClusterSize / 4, perUnitCount);
        }
    }

    public static void main2(String[] args)
    {
        int perUnitCount = 16;
        for (perUnitCount = 4; perUnitCount <= 256; perUnitCount *= 4)
        {
            final int targetClusterSize = 500;
            int rf = 3;
            int initialSize = rf;
            NavigableMap<Token, Unit> tokenMap = Maps.newTreeMap();
    
            random(tokenMap, initialSize, perUnitCount);
    
            ReplicationAwareTokenAllocator<Unit> t = new ReplicationAwareTokenAllocator<>(tokenMap, new SimpleReplicationStrategy(rf), partitioner);
            for (int i=initialSize; i<=targetClusterSize; i+=1)
                test(t, i, perUnitCount);
            
            testLoseAndReplace(t, targetClusterSize / 4, perUnitCount);
        }
    }
    
    static int tokenCount(int perUnitCount, Random rand)
    {
        if (perUnitCount == 1) return 1;
//        return perUnitCount;
//        return rand.nextInt(perUnitCount * 2) + 1;
//        return rand.nextInt(perUnitCount) + (perUnitCount+1)/2;
        return rand.nextInt(perUnitCount * 3 / 2) + (perUnitCount+3)/4;
    }

    private static void testLoseAndReplace(ReplicationAwareTokenAllocator<Unit> t, int howMany, int perUnitCount)
    {
        int fullCount = t.unitCount();
        System.out.format("Losing %d units\n", howMany);
        for (int i=0; i<howMany; ++i) {
            Unit u = t.unitFor(partitioner.getRandomToken());
            t.removeUnit(u);
            ((TestReplicationStrategy) t.strategy).removeUnit(u);
        }
        test(t, t.unitCount(), perUnitCount);
        
        test(t, fullCount, perUnitCount);
    }

    public static void test(ReplicationAwareTokenAllocator<Unit> t, int targetClusterSize, int perUnitCount)
    {
        int size = t.unitCount();
        Random rand = new Random(targetClusterSize + perUnitCount);
        if (size < targetClusterSize) {
            System.out.format("Adding %d unit(s) using %s...", targetClusterSize - size, t.toString());
            long time = System.currentTimeMillis();
            while (size < targetClusterSize)
            {
                int tokens = tokenCount(perUnitCount, rand);
                Unit unit = new Unit();
                ((TestReplicationStrategy) t.strategy).addUnit(unit);
                t.addUnit(unit, tokens);
                ++size;
            }
            System.out.format(" Done in %.3fs\n", (System.currentTimeMillis() - time) / 1000.0);
        }
        printDistribution(t);
    }



    static int nextUnitId = 0;

    static final class Unit implements Comparable<Unit>
    {
        int unitId = nextUnitId++;
        
        public String toString() {
            return Integer.toString(unitId);
        }

        @Override
        public int compareTo(Unit o)
        {
            return Integer.compare(unitId, o.unitId);
        }
    }
}
