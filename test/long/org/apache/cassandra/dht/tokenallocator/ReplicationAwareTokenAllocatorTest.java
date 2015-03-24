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

import junit.framework.Assert;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.commons.math.stat.descriptive.SummaryStatistics;

import org.junit.Test;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;

public class ReplicationAwareTokenAllocatorTest
{
    private static final int MAX_VNODE_COUNT = 256;

    private static final int TARGET_CLUSTER_SIZE = 500;

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
            return String.format("%s strategy, %d replicas, group size to count %s", getClass().getSimpleName(), replicas, sizeToCount);
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
    
    private static<T> Map<T, Integer> instanceToCount(Map<?, T> map)
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

        public FixedGroupCountReplicationStrategy(int replicas, int groupCount)
        {
            super(replicas);
            assert groupCount >= replicas;
            groupId = 0;
            this.groupCount = groupCount;
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

        public BalancedGroupReplicationStrategy(int replicas, int groupSize)
        {
            super(replicas);
            groupId = 0;
            this.groupSize = groupSize;
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
        
        public UnbalancedGroupReplicationStrategy(int replicas, int minGroupSize, int maxGroupSize)
        {
            super(replicas);
            groupId = -1;
            nextSize = 0;
            num = 0;
            this.maxGroupSize = maxGroupSize;
            this.minGroupSize = minGroupSize;
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
    
    static Map<Unit, Double> evaluateReplicatedOwnership(ReplicationAwareTokenAllocator<Unit> t)
    {
        Map<Unit, Double> ownership = Maps.newHashMap();
        Iterator<Token> it = t.sortedTokens.keySet().iterator();
        if (!it.hasNext())
            return ownership;
        
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

    static interface TokenCount {
        int tokenCount(int perUnitCount, Random rand);

        double spreadExpectation();
    }
    
    static TokenCount fixedTokenCount = new TokenCount()
    {
        public int tokenCount(int perUnitCount, Random rand)
        {
            return perUnitCount;
        }

        public double spreadExpectation()
        {
            return 10;  // High tolerance to avoid flakiness.
        }
    };
    
    static TokenCount varyingTokenCount = new TokenCount()
    {
        public int tokenCount(int perUnitCount, Random rand)
        {
            if (perUnitCount == 1) return 1;
            // 25 to 175%
            return rand.nextInt(perUnitCount * 3 / 2) + (perUnitCount+3)/4;
        }

        public double spreadExpectation()
        {
            return 15;  // High tolerance to avoid flakiness.
        }
    };
    
    IPartitioner partitioner = new Murmur3Partitioner();
    
    private void random(Map<Token, Unit> map, TestReplicationStrategy rs, int unitCount, TokenCount tc, int perUnitCount)
    {
        System.out.format("\nRandom generation of %d units with %d tokens each\n", unitCount, perUnitCount);
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        for (int i = 0 ; i < unitCount ; i++)
        {
            Unit unit = new Unit();
            rs.addUnit(unit);
            int tokens = tc.tokenCount(perUnitCount, rand);
            for (int j = 0 ; j < tokens ; j++)
            {
                map.put(partitioner.getRandomToken(), unit);
            }
        }
    }
    
    @Test
    public void testExistingCluster()
    {
        for (int rf = 1; rf <= 5; ++rf)
            for (int perUnitCount = 1; perUnitCount <= MAX_VNODE_COUNT; perUnitCount *= 4)
            {
                testExistingCluster(perUnitCount, fixedTokenCount, new SimpleReplicationStrategy(rf));
                testExistingCluster(perUnitCount, varyingTokenCount, new SimpleReplicationStrategy(rf));
                if (rf == 1) continue;  // Replication strategy doesn't matter for RF = 1.
                for (int groupSize = 4; groupSize <= 64 && groupSize * rf * 4 < TARGET_CLUSTER_SIZE; groupSize *= 4)
                {
                    testExistingCluster(perUnitCount, fixedTokenCount, new BalancedGroupReplicationStrategy(rf, groupSize));
                    testExistingCluster(perUnitCount, varyingTokenCount, new UnbalancedGroupReplicationStrategy(rf, groupSize / 2, groupSize * 2));
                }
                testExistingCluster(perUnitCount, fixedTokenCount, new FixedGroupCountReplicationStrategy(rf, rf * 2));
            }
    }
    
    @Test
    public void testNewCluster()
    {
        for (int rf = 1; rf <= 5; ++rf)
            for (int perUnitCount = 1; perUnitCount <= MAX_VNODE_COUNT; perUnitCount *= 4)
            {
                testNewCluster(perUnitCount, fixedTokenCount, new SimpleReplicationStrategy(rf));
                testNewCluster(perUnitCount, varyingTokenCount, new SimpleReplicationStrategy(rf));
                if (rf == 1) continue;  // Replication strategy doesn't matter for RF = 1.
                for (int groupSize = 4; groupSize <= 64 && groupSize * rf * 4 < TARGET_CLUSTER_SIZE; groupSize *= 4)
                {
                    testNewCluster(perUnitCount, fixedTokenCount, new BalancedGroupReplicationStrategy(rf, groupSize));
                    testNewCluster(perUnitCount, varyingTokenCount, new UnbalancedGroupReplicationStrategy(rf, groupSize / 2, groupSize * 2));
                }
                testNewCluster(perUnitCount, fixedTokenCount, new FixedGroupCountReplicationStrategy(rf, rf * 2));
            }
    }

    public void testExistingCluster(int perUnitCount, TokenCount tc, TestReplicationStrategy rs)
    {
        System.out.println("Testing existing cluster, target " + perUnitCount + " vnodes, replication " + rs);
        final int targetClusterSize = TARGET_CLUSTER_SIZE;
        NavigableMap<Token, Unit> tokenMap = Maps.newTreeMap();

        random(tokenMap, rs, targetClusterSize / 2, tc, perUnitCount);

        ReplicationAwareTokenAllocator<Unit> t = new ReplicationAwareTokenAllocator<>(tokenMap, rs, partitioner);
        grow(t, targetClusterSize * 9 / 10, tc, perUnitCount, false);
        grow(t, targetClusterSize, tc, perUnitCount, true);
        loseAndReplace(t, targetClusterSize / 10, tc, perUnitCount);
        System.out.println();
    }

    public void testNewCluster(int perUnitCount, TokenCount tc, ReplicationStrategy<Unit> rs)
    {
        System.out.println("Testing new cluster, target " + perUnitCount + " vnodes, replication " + rs);
        final int targetClusterSize = TARGET_CLUSTER_SIZE;
        NavigableMap<Token, Unit> tokenMap = Maps.newTreeMap();

        ReplicationAwareTokenAllocator<Unit> t = new ReplicationAwareTokenAllocator<>(tokenMap, rs, partitioner);
        grow(t, targetClusterSize / 5, tc, perUnitCount, false);
        grow(t, targetClusterSize, tc, perUnitCount, true);
        loseAndReplace(t, targetClusterSize / 5, tc, perUnitCount);
        System.out.println();
    }
    
    private void loseAndReplace(ReplicationAwareTokenAllocator<Unit> t, int howMany, TokenCount tc, int perUnitCount)
    {
        int fullCount = t.unitCount();
        System.out.format("Losing %d units. ", howMany);
        for (int i=0; i<howMany; ++i) {
            Unit u = t.unitFor(partitioner.getRandomToken());
            t.removeUnit(u);
            ((TestReplicationStrategy) t.strategy).removeUnit(u);
        }
        // Grow half without verifying.
        grow(t, (t.unitCount() + fullCount*3) / 4, tc, perUnitCount, false);
        // Metrics should be back to normal by now. Check that they remain so.
        grow(t, fullCount, tc, perUnitCount, true);
    }
    
    static class Summary {
        double min = 1;
        double max = 1;
        double stddev = 0;
        
        void update(SummaryStatistics stat)
        {
            min = Math.min(min, stat.getMin());
            max = Math.max(max, stat.getMax());
            stddev = Math.max(stddev, stat.getStandardDeviation());
        }

        public String toString()
        {
            return String.format("max %.2f min %.2f stddev %.4f", max, min, stddev);
        }
    }

    public void grow(ReplicationAwareTokenAllocator<Unit> t, int targetClusterSize, TokenCount tc, int perUnitCount, boolean verifyMetrics)
    {
        int size = t.unitCount();
        Summary su = new Summary();
        Summary st = new Summary();
        Random rand = new Random(targetClusterSize + perUnitCount);
        if (size < targetClusterSize) {
            System.out.format("Adding %d unit(s) using %s...", targetClusterSize - size, t.toString());
            long time = System.currentTimeMillis();
            while (size < targetClusterSize)
            {
                int tokens = tc.tokenCount(perUnitCount, rand);
                Unit unit = new Unit();
                ((TestReplicationStrategy) t.strategy).addUnit(unit);
                t.addUnit(unit, tokens);
                ++size;
                if (verifyMetrics)
                    updateSummary(t, su, st, false);
            }
            System.out.format(" Done in %.3fs\n", (System.currentTimeMillis() - time) / 1000.0);
            if (verifyMetrics)
            {
                updateSummary(t, su, st, true);
                double maxExpected = 1.0 + tc.spreadExpectation() / (perUnitCount * t.replicas);
                if (su.max > maxExpected)
                {
                    Assert.fail(String.format("Expected max unit size below %.4f, was %.4f", maxExpected, su.max));
                }
                // We can't verify lower side range as small loads can't always be fixed.
            }
        }
    }



    private void updateSummary(ReplicationAwareTokenAllocator<Unit> t, Summary su, Summary st, boolean print)
    {
        int size = t.sortedTokens.size();
        double inverseAverage = 1.0 * size / t.strategy.replicas();

        Map<Unit, Double> ownership = evaluateReplicatedOwnership(t);
        SummaryStatistics unitStat = new SummaryStatistics();
        for (Map.Entry<Unit, Double> en : ownership.entrySet())
            unitStat.addValue(en.getValue() * inverseAverage / t.unitToTokens.get(en.getKey()).size());
        su.update(unitStat);
        
        SummaryStatistics tokenStat = new SummaryStatistics();
        for (Token tok : t.sortedTokens.keySet())
            tokenStat.addValue(replicatedTokenOwnership(tok, t.sortedTokens, t.strategy) * inverseAverage);
        st.update(tokenStat);
        
        if (print)
        {
            System.out.format("Size %d(%d)   \tunit %s  token %s   %s\n",
                              t.unitCount(), size,
                              mms(unitStat),
                              mms(tokenStat),
                              t.strategy);
            System.out.format("Worst intermediate unit\t%s  token %s\n", su, st);
        }
    }



    private static String mms(SummaryStatistics s)
    {
        return String.format("max %.2f min %.2f stddev %.4f", s.getMax(), s.getMin(), s.getStandardDeviation());
    }



    int nextUnitId = 0;

    final class Unit implements Comparable<Unit>
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
