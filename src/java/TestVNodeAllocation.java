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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

public class TestVNodeAllocation
{

    private static final double totalTokenRange = 1.0 + Long.MAX_VALUE - (double) Long.MIN_VALUE;
    
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

        public Weighted(double weight, T unit)
        {
            this.weight = weight;
            this.value = unit;
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
         * Returns a list of all replica units for given token.
         */
        List<Unit> getReplicas(Token token, NavigableMap<Token, Unit> sortedTokens);

        /**
         * Returns the token that holds the last replica for the given token.
         */
        Token lastReplicaToken(Token middle, NavigableMap<Token, Unit> sortedTokens);

        /**
         * Returns the start of the token span that is replicated in this token.
         * Note: Though this is not trivial to see, the replicated span is always contiguous. A token in the same
         * group acts as a barrier; if one is not found the token replicates everything up to the replica'th distinct
         * group seen in front of it.
         */
        Token replicationStart(Token token, Unit unit, NavigableMap<Token, Unit> sortedTokens);

        void addUnit(Unit n);
        void removeUnit(Unit n);

        int replicas();
        
        boolean sameGroup(Unit n1, Unit n2);

        /**
         * Returns a group identifier. getGroup(a) == getGroup(b) iff a and b are on the same group.
         * @return Some hashable object.
         */
        Object getGroup(Unit unit);
    }
    
    static class NoReplicationStrategy implements ReplicationStrategy
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
    
    static class SimpleReplicationStrategy implements ReplicationStrategy
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
    
    static abstract class GroupReplicationStrategy implements ReplicationStrategy
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

        @Override
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
    
    
    static class TokenDistributor {
        
        NavigableMap<Token, Unit> sortedTokens;
        Multimap<Unit, Token> unitToTokens;
        ReplicationStrategy strategy;
        int debug;
        
        public TokenDistributor(NavigableMap<Token, Unit> sortedTokens, ReplicationStrategy strategy)
        {
            this.sortedTokens = new TreeMap<>(sortedTokens);
            unitToTokens = HashMultimap.create();
            for (Map.Entry<Token, Unit> en: sortedTokens.entrySet())
                unitToTokens.put(en.getValue(), en.getKey());
            this.strategy = strategy;
        }
        
        public void addUnit(Unit newUnit, int vunits)
        {
            throw new AssertionError("Don't call");
        }
        
        void addSelectedToken(Token token, Unit unit)
        {
            sortedTokens.put(token, unit);
            unitToTokens.put(unit, token);
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

        double replicatedTokenOwnership(Token token, NavigableMap<Token, Unit> sortedTokens)
        {
            Token next = sortedTokens.higherKey(token);
            if (next == null)
                next = sortedTokens.firstKey();
            return strategy.replicationStart(token, sortedTokens.get(token), sortedTokens).size(next);
        }

        public int unitCount()
        {
            return unitToTokens.asMap().size();
        }
        
        public Map.Entry<Token, Unit> mapEntryFor(Token t)
        {
            Map.Entry<Token, Unit> en = sortedTokens.floorEntry(t);
            if (en == null)
                en = sortedTokens.lastEntry();
            return en;
        }
        
        private Unit unitFor(Token t)
        {
            return mapEntryFor(t).getValue();
        }

        public void removeUnit(Token t)
        {
            removeUnit(unitFor(t));
        }

        public void removeUnit(Unit n)
        {
            Collection<Token> tokens = unitToTokens.removeAll(n);
            sortedTokens.keySet().removeAll(tokens);
            strategy.removeUnit(n);
        }
        
        public String toString()
        {
            return getClass().getSimpleName();
        }
        

        Map<Unit, Double> evaluateReplicatedOwnership()
        {
            Map<Unit, Double> ownership = Maps.newHashMap();
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

        private void addOwnership(Token current, Token next, Map<Unit, Double> ownership)
        {
            double size = current.size(next);
            for (Unit n : strategy.getReplicas(current, sortedTokens)) {
                Double v = ownership.get(n);
                ownership.put(n, v != null ? v + size : size);
            }
        }

        protected boolean verifyOwnership(Map<Unit, Double> ownership)
        {
            for (Map.Entry<Unit, Double> en : ownership.entrySet())
            {
                Unit n = en.getKey();
                double owns = unitToTokens.get(n).stream().
                        mapToDouble(this::replicatedTokenOwnership).sum();
                if (Math.abs(owns - en.getValue()) > totalTokenRange * 1e-14)
                {
                    System.out.format("Unit %s expected %f got %f\n%s\n%s\n",
                                       n, owns, en.getValue(),
                                       ImmutableList.copyOf(sortedTokens.entrySet().stream().filter(tn -> tn.getValue() == n).map(Map.Entry::getKey).iterator()),
                                       ImmutableList.copyOf(sortedTokens.entrySet().stream().filter(tn -> tn.getValue() == n).map(Map.Entry::getKey).mapToDouble(this::replicatedTokenOwnership).iterator())
                                       );
                    return false;
                }
            }
            return true;
        }

        public Map<Token, Token> createReplicationStartMap(NavigableMap<Token, Unit> sortedTokens)
        {
            Map<Token, Token> replicationStart = Maps.newHashMap();
            for (Map.Entry<Token, Unit> en : sortedTokens.entrySet())
            {
                Unit n = en.getValue();
                Token t = en.getKey();
                Token rs = strategy.replicationStart(t, n, sortedTokens);
                replicationStart.put(t, rs);
            }
            return replicationStart;
        }

        public boolean verifyReplicationStartMap(Map<Token, Token> replicationStart)
        {
            Token t;
            Unit n;
            Token rs;
            boolean success = true;
            for (Map.Entry<Token, Unit> ven: sortedTokens.entrySet()) {
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

        public double optimalTokenOwnership(int tokensToAdd)
        {
            return totalTokenRange * strategy.replicas() / (sortedTokens.size() + tokensToAdd);
        }

        protected double evaluateCombinedImprovement(Token middle, Unit newUnit,
                NavigableMap<Token, Unit> sortedTokensWithNew, Map<Unit, Double> ownership, Map<Token, Token> replicationStart,
                double tokenOptimal, double unitMult, double newUnitMult, double tokenMult)
        {
            sortedTokensWithNew.put(middle, newUnit);
            // First, check which tokens are affected by the split.
            Token lr1 = strategy.lastReplicaToken(middle, sortedTokens);
            // The split can cause some tokens to be replicated further than they used to.
            Token lr2 = strategy.lastReplicaToken(middle, sortedTokensWithNew);
            Token lastReplica = max(lr1, lr2, middle);

            // We can't directly calculate unit improvement as a unit may have multiple affected tokens. Store new size in a map.
            Map<Unit, Double> unitToWeight = Maps.newHashMap();

            double tokenImprovement = 0;
            for (Map.Entry<Token, Unit> en : Iterables.concat(sortedTokens.tailMap(middle, false).entrySet(),
                                                              sortedTokens.entrySet()))
            {
                Token token = en.getKey();
                Unit unit = en.getValue();
                Token nextToken = next(token);
                Token rsNew = strategy.replicationStart(token, unit, sortedTokensWithNew);
                Token rsOld = replicationStart.get(token);
                double osz = rsOld.size(nextToken);
                double nsz = rsNew.size(nextToken);
                tokenImprovement += sq(osz - tokenOptimal) - sq(nsz - tokenOptimal);
                addToWeight(unitToWeight, ownership, unit, nsz - osz);
        
                if (token == lastReplica)
                    break;
            }
            sortedTokensWithNew.remove(middle);
            
            // Also calculate change to currently owning token.
            Entry<Token, Unit> en = mapEntryFor(middle);
            Token token = en.getKey();
            Unit unit = en.getValue();
            Token nextToken = next(token);
            Token rsOld = replicationStart.get(token);
            double osz = rsOld.size(nextToken);
            double nsz = rsOld.size(middle);
            tokenImprovement += sq(osz - tokenOptimal) - sq(nsz - tokenOptimal);
            addToWeight(unitToWeight, ownership, unit, nsz - osz);
            
            // Calculate the size of the newly added token.
            Token rsNew = strategy.replicationStart(middle, newUnit, sortedTokensWithNew);
            osz = tokenOptimal;
            nsz = rsNew.size(nextToken);
            tokenImprovement += sq(osz - tokenOptimal) - sq(nsz - tokenOptimal);
            addToWeight(unitToWeight, ownership, newUnit, nsz - osz);
            tokenImprovement *= tokenMult;

            // Evaluate the unit-level improvement from the unitToWeight map.
            double unitImprovement = 0;
            for (Map.Entry<Unit, Double> nw: unitToWeight.entrySet())
            {
                unit = nw.getKey();
                nsz = nw.getValue();
                osz = ownership.get(unit);
                double unitOptimal = unitToTokens.get(unit).size() * tokenOptimal;
                if (unit != newUnit)
                    unitImprovement += (sq(osz - unitOptimal) - sq(nsz - unitOptimal)) * unitMult;
                else
                    unitImprovement += (sq(osz - unitOptimal) - sq(nsz - (unitOptimal + tokenOptimal))) * newUnitMult;
            }

            return unitImprovement + tokenImprovement;
        }

        public static void addToWeight(Map<Unit, Double> unitToWeight, Map<Unit, Double> fallback, Unit n, double weightChange)
        {
            Double v = unitToWeight.get(n);
            if (v == null)
                v = fallback.get(n);
            unitToWeight.put(n, v + weightChange);
        }

        // Debug function tracing the results of the above process.
        public double printChangeStat(Token middle, Unit newUnit, NavigableMap<Token, Unit> sortedTokensWithNew, final double topt, double newUnitMult)
        {
            sortedTokensWithNew.put(middle, newUnit);
            StringBuilder afft = new StringBuilder();
            Token lr1 = strategy.lastReplicaToken(middle, sortedTokens);
            Token lr2 = strategy.lastReplicaToken(middle, sortedTokensWithNew);
            Token lastReplica = max(lr1, lr2, middle);
            Map<Unit, Double> unitToWeight = Maps.newHashMap();
            Map<Unit, Double> ownership = evaluateReplicatedOwnership();
            Double v = ownership.get(newUnit);
            if (v == null)
                ownership.put(newUnit, v = 0.0);

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
                addToWeight(unitToWeight, ownership, unitFor(t), nsz - osz);
                if (t == lastReplica)
                    break;
            }
            double nsz = replicatedTokenOwnership(middle, sortedTokensWithNew);
            System.out.format("change token %s->%.3f(%.5f) affects %s\n",
                              middle, nsz / topt, -sq(nsz / topt - 1),
                              afft);
            tokenImprovement -= sq(nsz - topt);
            addToWeight(unitToWeight, ownership, newUnit, nsz - topt);
            sortedTokensWithNew.remove(middle);

            List<Double> tokenOwnership = Lists.newArrayList(sortedTokens.keySet().stream().mapToDouble(this::replicatedTokenOwnership).iterator());
            double dev = tokenOwnership.stream().mapToDouble(x -> sq(x / topt - 1)).sum();
            DoubleSummaryStatistics stat = tokenOwnership.stream().mapToDouble(x -> x / topt).summaryStatistics();
            System.out.format(" expected token impr %.5f on %.5f max %.2f min %.2f\n", tokenImprovement / sq(topt), dev, stat.getMax(), stat.getMin());
            
            System.out.print(" affects ");
            double unitImprovement = 0;
            for (Map.Entry<Unit, Double> en: unitToWeight.entrySet())
            {
                Unit n = en.getKey();
                nsz = en.getValue();
                double osz = ownership.get(n);
                double oldopt = unitToTokens.get(n).size() * topt;
                double newopt = n == newUnit ? oldopt + topt : oldopt;
                double cm = n == newUnit ? newUnitMult : 1.0;
                unitImprovement += (sq(osz - oldopt) - sq(nsz - newopt)) * cm;
                        
                System.out.format("unit %s:%.3f->%.3f(%.5f),",
                                  n,
                                  osz / oldopt,
                                  nsz / newopt,
                                  sq(osz / oldopt - 1) - sq(nsz / newopt - 1));
            }

            // old ones
            dev = ownership.entrySet().stream().mapToDouble(en -> en.getValue() / optimalUnitOwnership(en.getKey(), topt)).
                    map(x -> sq(x - 1)).sum();
            stat = ownership.entrySet().stream().mapToDouble(en -> en.getValue() / optimalUnitOwnership(en.getKey(), topt)).summaryStatistics();
            System.out.format("\n expected unit impr %.5f on %.5f max %.2f min %.2f\n", unitImprovement / sq(optimalUnitOwnership(newUnit, topt) + topt), dev, stat.getMax(), stat.getMin());
            return tokenImprovement + unitImprovement;
        }
        
        double optimalUnitOwnership(Unit unit, double tokenOptimal)
        {
            return tokenOptimal * unitToTokens.get(unit).size();
        }
        
    }
    
    static GroupInfo getGroup(Unit unit, Map<Object, GroupInfo> groupMap, ReplicationStrategy strategy)
    {
        Object groupClass = strategy.getGroup(unit);
        GroupInfo ri = groupMap.get(groupClass);
        if (ri == null)
            groupMap.put(groupClass, ri = new GroupInfo(groupClass));
        return ri;
    }
    
    /**
     * Unique group object that one or more UnitInfo objects link to.
     */
    static class GroupInfo {
        /** Group identifier given by ReplicationStrategy.getGroup(Unit). */
        final Object group;

        /**
         * Seen marker. When non-null, the group is already seen in replication walks.
         * Also points to previous seen group to enable walking the seen groups and clearing the seen markers.
         */
        GroupInfo prevSeen = null;
        /** Same marker/chain used by populateTokenInfo. */
        GroupInfo prevPopulate = null;

        /** Value used as terminator for seen chains. */
        static GroupInfo TERMINATOR = new GroupInfo(null);

        public GroupInfo(Object group)
        {
            this.group = group;
        }

        public String toString()
        {
            return group.toString() + (prevSeen != null ? "*" : ""); 
        }
    }

    /**
     * Unit information created and used by ReplicationAwareTokenDistributor. Contained vunits all point to the same
     * instance.
     */
    static class UnitInfo {
        final Unit unit;
        final GroupInfo group;
        double ownership;
        int tokenCount;

        /** During evaluateImprovement this is used to form a chain of units affected by the candidate insertion. */
        UnitInfo prevUsed;
        /** During evaluateImprovement this holds the ownership after the candidate insertion. */
        double adjustedOwnership;

        private UnitInfo(Unit unit, GroupInfo group)
        {
            this.unit = unit;
            this.group = group;
            this.tokenCount = 0;
        }
        
        public UnitInfo(Unit unit, double ownership, Map<Object, GroupInfo> groupMap, ReplicationStrategy strategy)
        {
            this(unit, getGroup(unit, groupMap, strategy));
            this.ownership = ownership;
        }
        
        public String toString()
        {
            return String.format("%s%s(%.2e)%s", unit, group.prevSeen != null ? "*" : "", ownership, prevUsed != null ? "prev " + (prevUsed == this ? "this" : prevUsed.toString()) : ""); 
        }
    }

    static class CircularList<T extends CircularList<T>> {
        T prev;
        T next;
        
        /**
         * Inserts this after unit in the circular list which starts at head. Returns the new head of the list, which
         * only changes if head was null.
         */
        @SuppressWarnings("unchecked")
        T insertAfter(T head, T unit) {
            if (head == null) {
                return prev = next = (T) this;
            }
            assert unit != null;
            assert unit.next != null;
            prev = unit;
            next = unit.next;
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
        final UnitInfo owningUnit;

        /**
         * Start of the replication span for the vnode, i.e. the end of the first token of the RF'th group seen before
         * the token. The replicated ownership of the unit is the range between replicationStart and next.token.
         */
        Token replicationStart;
        /**
         * RF minus one boundary, i.e. the end of the first token of the RF-1'th group seen before the token.
         * Used to determine replicationStart after insertion of new token.
         */
        Token rfm1Token;
        /**
         * Whether token can be expanded (and only expanded) by adding new unit that ends at replicationStart.
         */
        boolean expandable;
        /**
         * Current replicated ownership. This number is reflected in the owning unit's ownership.
         */
        double replicatedOwnership = 0;
        
        public BaseTokenInfo(Token token, UnitInfo owningUnit)
        {
            this.token = token;
            this.owningUnit = owningUnit;
        }

        public String toString()
        {
            return String.format("%s(%s)%s", token, owningUnit, expandable ? "=" : ""); 
        }
        
        /**
         * Previous unit in the token ring. For existing tokens this is prev, for candidates it's the host.
         */
        TokenInfo prevInRing()
        {
            return null;
        }
    }
    
    /**
     * TokenInfo about existing tokens/vunits.
     */
    static class TokenInfo extends BaseTokenInfo<TokenInfo> {
        public TokenInfo(Token token, UnitInfo owningUnit)
        {
            super(token, owningUnit);
        }
        
        TokenInfo prevInRing()
        {
            return prev;
        }
    }
    
    /**
     * TokenInfo about candidate new tokens/vunits.
     */
    static class CandidateInfo extends BaseTokenInfo<CandidateInfo> {
        final TokenInfo host;

        public CandidateInfo(Token token, TokenInfo host, UnitInfo owningUnit)
        {
            super(token, owningUnit);
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
    
    static class ReplicationAwareTokenDistributor extends TokenDistributor
    {
        public ReplicationAwareTokenDistributor(NavigableMap<Token, Unit> sortedTokens, ReplicationStrategy strategy)
        {
            super(sortedTokens, strategy);
        }

        @Override
        public void addUnit(Unit newUnit, int numTokens)
        {
            strategy.addUnit(newUnit);
            double optTokenOwnership = optimalTokenOwnership(numTokens);
            Map<Object, GroupInfo> groups = Maps.newHashMap();
            UnitInfo newUnitInfo = new UnitInfo(newUnit, numTokens * optTokenOwnership, groups, strategy);
            TokenInfo tokens = createTokenInfos(createUnitInfos(groups), newUnitInfo.group);
            newUnitInfo.tokenCount = numTokens;

            CandidateInfo candidates = createCandidates(tokens, newUnitInfo, optTokenOwnership);
            assert verifyTokenInfo(tokens);
            
            // Evaluate the expected improvements from all candidates and form a priority queue.
            PriorityQueue<Weighted<CandidateInfo>> improvements = new PriorityQueue<>(sortedTokens.size());
            CandidateInfo candidate = candidates;
            do
            {
                double impr = evaluateImprovement(candidate, optTokenOwnership, 1.0 / numTokens);
                improvements.add(new Weighted<>(impr, candidate));
                candidate = candidate.next;
            } while (candidate != candidates);
            CandidateInfo bestToken = improvements.remove().value;
            candidates = bestToken.removeFrom(candidates);
            
            for (int vn = 0; ; ++vn)
            {
                // Use the token with the best improvement.
                adjustData(bestToken);
                addSelectedToken(bestToken.token, newUnit);

                if (vn == numTokens - 1)
                    break;
                
                for (;;)
                {
                    // Get the next candidate in the queue. Its improvement may have changed (esp. if multiple tokens
                    // were good suggestions because they could improve the same problem)-- evaluate it again to check
                    // if it is still a good candidate.
                    bestToken = improvements.remove().value;
                    candidates = bestToken.removeFrom(candidates);
                    double impr = evaluateImprovement(bestToken, optTokenOwnership, (vn + 1.0) / numTokens);
                    double nextImpr = improvements.peek().weight;
                    
                    // If it is better than the next in the queue, it is good enough. This is a heuristic that doesn't
                    // get the best results, but works well enough and on average cuts search time by a factor of O(vunits).
                    if (impr >= nextImpr)
                        break;
                    improvements.add(new Weighted<>(impr, bestToken));
                }
            }
            // Verify adjustData didn't do something wrong.
            assert verifyTokenInfo(tokens);
        }

        Map<Unit, UnitInfo> createUnitInfos(Map<Object, GroupInfo> groups)
        {
            Map<Unit, UnitInfo> map = Maps.newHashMap();
            for (Unit n: sortedTokens.values()) {
                UnitInfo ni = map.get(n);
                if (ni == null)
                    map.put(n, ni = new UnitInfo(n, 0, groups, strategy));
                ni.tokenCount++;
            }
            return map;
        }

        TokenInfo createTokenInfos(Map<Unit, UnitInfo> units, GroupInfo newUnitGroup)
        {
            TokenInfo prev = null;
            TokenInfo first = null;
            for (Map.Entry<Token, Unit> en: sortedTokens.entrySet())
            {
                Token t = en.getKey();
                UnitInfo ni = units.get(en.getValue());
                TokenInfo ti = new TokenInfo(t, ni);
                first = ti.insertAfter(first, prev);
                prev = ti;
            }

            TokenInfo curr = first;
            do {
                populateTokenInfoAndAdjustUnit(curr, newUnitGroup);
                curr = curr.next;
            } while (curr != first);

            return first;
        }

        CandidateInfo createCandidates(TokenInfo tokens, UnitInfo newUnitInfo, double initialTokenOwnership)
        {
            TokenInfo curr = tokens;
            CandidateInfo first = null;
            CandidateInfo prev = null;
            do {
                CandidateInfo candidate = new CandidateInfo(curr.token.slice(curr.token.size(curr.next.token) / 2), curr, newUnitInfo);
                first = candidate.insertAfter(first, prev);
                
                candidate.replicatedOwnership = initialTokenOwnership;
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
            populateTokenInfo(candidate, candidate.owningUnit.group);
        }

        boolean verifyTokenInfo(TokenInfo tokens)
        {
            Map<Token, Token> replicationStart = Maps.newHashMap();
            Map<Unit, Double> ownership = Maps.newHashMap();
            TokenInfo token = tokens;
            do {
                replicationStart.put(token.token, token.replicationStart);
                UnitInfo ni = token.owningUnit;
                ownership.put(ni.unit, ni.ownership);
                token = token.next;
            } while (token != tokens);
            return verifyReplicationStartMap(replicationStart) && verifyOwnership(ownership);
        }

        double printChangeStat(CandidateInfo candidate, double optTokenOwnership, double newUnitMult)
        {
            return printChangeStat(candidate.token, candidate.owningUnit.unit, new TreeMap<>(sortedTokens), optTokenOwnership, newUnitMult);
        }

        public void adjustData(CandidateInfo candidate)
        {
            // This process is less efficient than it could be (loops through each vunits's replication span instead
            // of recalculating replicationStart, rfm1 and expandable from existing data + new token data in an O(1)
            // case analysis similar to evaluateImprovement). This is fine as the method does not dominate processing
            // time.
            
            // Put the accepted candidate in the token list.
            TokenInfo host = candidate.host;
            TokenInfo next = host.next;
            TokenInfo candidateToken = new TokenInfo(candidate.token, candidate.owningUnit);
            candidateToken.replicatedOwnership = candidate.replicatedOwnership;
            candidateToken.insertAfter(host, host);   // List is not empty so this won't need to change head of list.

            // Update data for both candidate and host.
            UnitInfo newUnit = candidateToken.owningUnit;
            GroupInfo newGroup = newUnit.group;
            populateTokenInfoAndAdjustUnit(candidateToken, newGroup);
            UnitInfo hostUnit = host.owningUnit;
            GroupInfo hostGroup = hostUnit.group;
            populateTokenInfoAndAdjustUnit(host, newGroup);

            GroupInfo groupChain = GroupInfo.TERMINATOR;

            // Loop through all vunits that replicate new token or host and update their data.
            // Also update candidate data for units in the replication span.
            int seenOld = 0;
            int seenNew = 0;
            int rf = strategy.replicas() - 1;
          evLoop:
            for (TokenInfo curr = next; seenOld < rf || seenNew < rf; curr = next)
            {
                candidate = candidate.next;
                populateCandidate(candidate);

                next = curr.next;
                UnitInfo currUnit = curr.owningUnit;
                GroupInfo group = currUnit.group;
                if (group.prevSeen != null)
                    continue evLoop;    // If both have already seen this group, nothing can change within it.
                if (group != newGroup)
                    seenNew += 1;
                if (group != hostGroup)
                    seenOld += 1;
                group.prevSeen = groupChain;
                groupChain = group;
                
                populateTokenInfoAndAdjustUnit(curr, newGroup);
            }
            
            // Clean group seen markers.
            while (groupChain != GroupInfo.TERMINATOR) {
                GroupInfo prev = groupChain.prevSeen;
                groupChain.prevSeen = null;
                groupChain = prev;
            }
        }
        
        private Token populateTokenInfo(BaseTokenInfo<?> token, GroupInfo newUnitGroup)
        {
            GroupInfo groupChain = GroupInfo.TERMINATOR;
            GroupInfo tokenGroup = token.owningUnit.group;
            int seenGroups = 0;
            boolean expandable = false;
            int rf = strategy.replicas();
            // Replication start = the end of a token from the RF'th different group seen before the token.
            Token rs = token.token;
            // The end of a token from the RF-1'th different group seen before the token.
            Token rfm1 = rs;
            for (TokenInfo curr = token.prevInRing();;rs = curr.token, curr = curr.prev) {
                GroupInfo ri = curr.owningUnit.group;
                if (ri.prevPopulate != null)
                    continue; // Group is already seen.
                // Mark the group as seen. Also forms a chain that can be used to clear the marks when we are done.
                // We use prevPopulate instead of prevSeen as this may be called within adjustData which also needs
                // group seen markers.
                ri.prevPopulate = groupChain;
                groupChain = ri;
                if (++seenGroups == rf)
                    break;

                rfm1 = rs;
                // Another instance of the same group is a replication boundary.
                if (ri == tokenGroup)
                {
                    // Inserting a token that ends at this boundary will increase replication coverage,
                    // but only if the inserted unit is not in the same group.
                    expandable = tokenGroup != newUnitGroup;
                    break;
                }
                // An instance of the new group in the replication span also means that we can expand coverage
                // by inserting a token ending at replicationStart.
                if (ri == newUnitGroup)
                    expandable = true;
            }
            token.rfm1Token = rfm1;
            token.replicationStart = rs;
            token.expandable = expandable;

            // Clean group seen markers.
            while (groupChain != GroupInfo.TERMINATOR)
            {
                GroupInfo prev = groupChain.prevPopulate;
                groupChain.prevPopulate  = null;
                groupChain = prev;
            }
            return rs;
        }
        
        private void populateTokenInfoAndAdjustUnit(TokenInfo candidate, GroupInfo newUnitGroup)
        {
            Token rs = populateTokenInfo(candidate, newUnitGroup);
            double newOwnership = rs.size(candidate.next.token);
            double oldOwnership = candidate.replicatedOwnership;
            candidate.replicatedOwnership = newOwnership;
            candidate.owningUnit.ownership += newOwnership - oldOwnership;
        }

        double evaluateImprovement(CandidateInfo candidate, double optTokenOwnership, double newUnitMult)
        {
            double change = 0;
            TokenInfo host = candidate.host;
            TokenInfo next = host.next;
            Token cend = next.token;

            // Reflect change in ownership of the splitting token (candidate).
            double oldOwnership = candidate.replicatedOwnership;
            double newOwnership = candidate.replicationStart.size(next.token);
            UnitInfo newUnit = candidate.owningUnit;
            double tokenCount = newUnit.tokenCount;
            change += (sq(newOwnership - optTokenOwnership) - sq(oldOwnership - optTokenOwnership)) / sq(tokenCount);
            assert tokenCount > 0;
            newUnit.adjustedOwnership = newUnit.ownership + newOwnership - oldOwnership;
            
            // Reflect change of ownership in the token being split (host).
            oldOwnership = host.replicatedOwnership;
            newOwnership = host.replicationStart.size(candidate.token);
            UnitInfo hostUnit = host.owningUnit;
            tokenCount = hostUnit.tokenCount;
            assert tokenCount > 0;
            change += (sq(newOwnership - optTokenOwnership) - sq(oldOwnership - optTokenOwnership)) / sq(tokenCount);
            hostUnit.adjustedOwnership = hostUnit.ownership + newOwnership - oldOwnership;

            // Form a chain of units affected by the insertion to be able to qualify change of unit ownership.
            // A unit may be affected more than once.
            assert newUnit.prevUsed == null;
            newUnit.prevUsed = newUnit;   // end marker
            GroupInfo newGroup = newUnit.group;
            assert hostUnit.prevUsed == null;
            hostUnit.prevUsed = newUnit;
            GroupInfo hostGroup = hostUnit.group;
            UnitInfo unitsChain = hostUnit;

            // Loop through all vunits that replicate candidate or host and update their ownership.
            int seenOld = 0;
            int seenNew = 0;
            int rf = strategy.replicas() - 1;
          evLoop:
            for (TokenInfo curr = next; seenOld < rf || seenNew < rf; curr = next)
            {
                next = curr.next;
                UnitInfo currUnit = curr.owningUnit;
                GroupInfo group = currUnit.group;
                if (group.prevSeen != null)
                    continue evLoop;    // If both have already seen this group, nothing can change within it.
                if (group != newGroup)
                    seenNew += 1;
                if (group != hostGroup)
                    seenOld += 1;
                group.prevSeen = group;   // Just mark it as seen, we are not forming a chain of groups.
                
                if (currUnit.prevUsed == null)
                {
                    currUnit.adjustedOwnership = currUnit.ownership;
                    currUnit.prevUsed = unitsChain;
                    unitsChain = currUnit;
                }

                Token rs;
                if (curr.expandable) // Sees same or new group before rf-1.
                {
                    if (cend != curr.replicationStart)
                        continue evLoop;
                    // Replication expands to start of candidate.
                    rs = candidate.token;
                } else {
                    if (group == newGroup)
                    {
                        if (!preceeds(curr.replicationStart, cend, next.token))
                            continue evLoop; // no changes, another newGroup is closer
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
                tokenCount = currUnit.tokenCount;
                assert tokenCount > 0;
                change += (sq(newOwnership - optTokenOwnership) - sq(oldOwnership - optTokenOwnership)) / sq(tokenCount);
                currUnit.adjustedOwnership += newOwnership - oldOwnership;
            }

            // Now loop through the units chain and add the unit-level changes. Also clear the groups' seen marks.
            for (;;) {
                newOwnership = unitsChain.adjustedOwnership;
                oldOwnership = unitsChain.ownership;
                tokenCount = unitsChain.tokenCount;
                double diff = (sq(newOwnership/tokenCount - optTokenOwnership) - sq(oldOwnership/tokenCount - optTokenOwnership));
                UnitInfo prev = unitsChain.prevUsed;
                unitsChain.prevUsed = null;
                unitsChain.group.prevSeen = null;
                if (unitsChain != newUnit)
                    change += diff;
                else 
                {
                    change += diff * newUnitMult;
                    break;
                }
                unitsChain = prev;
            }

            return -change;
        }
    }
    
    static class ReplicationAwareTokenDistributorTryAll extends ReplicationAwareTokenDistributor
    {

        public ReplicationAwareTokenDistributorTryAll(NavigableMap<Token, Unit> sortedTokens, ReplicationStrategy strategy)
        {
            super(sortedTokens, strategy);
        }
        
        @Override
        public void addUnit(Unit newUnit, int numTokens)
        {
            strategy.addUnit(newUnit);
            double optTokenOwnership = optimalTokenOwnership(numTokens);
            Map<Object, GroupInfo> groups = Maps.newHashMap();
            UnitInfo newUnitInfo = new UnitInfo(newUnit, numTokens * optTokenOwnership, groups, strategy);
            newUnitInfo.tokenCount = numTokens;
            TokenInfo tokens = createTokenInfos(createUnitInfos(groups), newUnitInfo.group);

            CandidateInfo candidates = createCandidates(tokens, newUnitInfo, optTokenOwnership);
            if (debug >= 3)
            {
                dumpTokens("E", tokens);
                dumpTokens("C", candidates);
            }
            assert verifyTokenInfo(tokens);

            // For each vnode to add, try all choices and pick the one that gives best improvement.
            for (int vn = 0; vn < numTokens; ++vn)
            {
                CandidateInfo bestToken = null;
                double bestImprovement = Double.NEGATIVE_INFINITY;
                CandidateInfo candidate = candidates;
                do
                {
                    if (debug >= 5)
                    {
                        double expImpr = printChangeStat(candidate, optTokenOwnership, (vn + 1.0) / numTokens);
                        double impr = evaluateImprovement(candidate, optTokenOwnership, (vn + 1.0) / numTokens);
                        if (Math.abs((impr / expImpr) - 1) > 0.000001)
                        {
                            System.out.format("Evaluation wrong: %.6f vs %.6f\n", impr/sq(optTokenOwnership * numTokens), expImpr/sq(optTokenOwnership * numTokens));
                        }
                    }
                    double impr = evaluateImprovement(candidate, optTokenOwnership, (vn + 1.0) / numTokens);
                    if (impr > bestImprovement)
                    {
                        bestToken = candidate;
                        bestImprovement = impr;
                    }
                    candidate = candidate.next;
                } while (candidate != candidates);

                if (debug >= 3) {
                    System.out.print("Selected ");printChangeStat(bestToken, optTokenOwnership, (vn + 1.0) / numTokens);
                }

                adjustData(bestToken);
                candidates = bestToken.removeFrom(candidates);
                addSelectedToken(bestToken.token, newUnit);
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

    private static void perfectDistribution(Map<Token, Unit> map, int unitCount, int perUnitCount)
    {
        System.out.format("\nPerfect init for %d units with %d tokens each.\n", unitCount, perUnitCount);
        for (int i=0; i<unitCount; ++i)
        {
            Unit unit = new Unit();
            double inc = totalTokenRange / perUnitCount;
            double start = Long.MIN_VALUE + inc / unitCount * i;
            for (int j = 0 ; j < perUnitCount ; j++)
            {
                map.put(new Token((long) start), unit);
                start += inc;
            }
        }
    }

    private static void random(Map<Token, Unit> map, int unitCount, int perUnitCount, boolean localRandom)
    {
        System.out.format("\nRandom generation of %d units with %d tokens each%s\n", unitCount, perUnitCount, (localRandom ? ", locally random" : ""));
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        for (int i = 0 ; i < unitCount ; i++)
        {
            Unit unit = new Unit();
            int tokens = tokenCount(perUnitCount, rand);
            long inc = -(Long.MIN_VALUE / tokens) * 2;
            for (int j = 0 ; j < tokens ; j++)
            {
                long nextToken;
                if (localRandom && tokens > 1) nextToken = Long.MIN_VALUE + j * inc + rand.nextLong(inc);
                else nextToken = rand.nextLong();
                map.put(new Token(nextToken), unit);
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
        Map<Unit, Double> ownership = t.evaluateReplicatedOwnership();
        ownership.replaceAll((n, w) -> w / t.unitToTokens.get(n).size());
        int size = t.sortedTokens.size();
        double inverseAverage = size / (totalTokenRange * t.strategy.replicas());
        List<Double> tokenOwnership = Lists.newArrayList(t.sortedTokens.keySet().stream().mapToDouble(t::replicatedTokenOwnership).iterator());
        System.out.format("Size %d(%d)   unit %s  token %s   %s\n",
                          t.unitCount(), size,
                          stats(ownership.values(), inverseAverage, 1),
                          stats(tokenOwnership, inverseAverage, 1),
                          t.strategy);
    }
    
    public static void main(String[] args)
    {
        int perUnitCount = 16;
        for (perUnitCount = 1; perUnitCount <= 1024; perUnitCount *= 4)
        {
            final int targetClusterSize = 500;
            int rf = 3;
            NavigableMap<Token, Unit> tokenMap = Maps.newTreeMap();
    
    //        perfectDistribution(tokenMap, targetClusterSize, perUnitCount);
            boolean locallyRandom = false;
            random(tokenMap, targetClusterSize, perUnitCount, locallyRandom);
    
            Set<Unit> units = Sets.newTreeSet(tokenMap.values());
            TokenDistributor[] t = {
                new ReplicationAwareTokenDistributorTryAll(tokenMap, new SimpleReplicationStrategy(rf)),
                new ReplicationAwareTokenDistributor(tokenMap, new SimpleReplicationStrategy(rf)),
            };
//            t[0].debug = 5;
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
    
    //        perfectDistribution(tokenMap, targetClusterSize, perUnitCount);
            boolean locallyRandom = false;
            random(tokenMap, initialSize, perUnitCount, locallyRandom);
    
            TokenDistributor[] t = {
                    new ReplicationAwareTokenDistributor(tokenMap, new SimpleReplicationStrategy(rf)),
            };
//            t[1].debug = 5;
            for (int i=initialSize; i<=targetClusterSize; i+=1)
                test(t, i, perUnitCount);
            
//            testLoseAndReplace(t, targetClusterSize / 4);
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

    private static void testLoseAndReplace(TokenDistributor[] ts, int howMany, int perUnitCount)
    {
        for (TokenDistributor t: ts)
            testLoseAndReplace(t, howMany, perUnitCount);
    }

    private static void test(TokenDistributor[] ts, int targetClusterSize, int perUnitCount)
    {
        System.out.println(targetClusterSize);
        for (TokenDistributor t: ts)
            test(t, targetClusterSize, perUnitCount);
    }

    private static void testLoseAndReplace(TokenDistributor t, int howMany, int perUnitCount)
    {
        int fullCount = t.unitCount();
        System.out.format("Losing %d units\n", howMany);
        Random rand = new Random(howMany);
        for (int i=0; i<howMany; ++i)
            t.removeUnit(new Token(rand.nextLong()));
        test(t, t.unitCount(), perUnitCount);
        
        test(t, fullCount, perUnitCount);
    }

    public static void test(TokenDistributor t, int targetClusterSize, int perUnitCount)
    {
        int size = t.unitCount();
        Random rand = new Random(targetClusterSize + perUnitCount);
        if (size < targetClusterSize) {
            System.out.format("Adding %d unit(s) using %s...", targetClusterSize - size, t.toString());
            long time = System.currentTimeMillis();
            while (size < targetClusterSize)
            {
                int tokens = tokenCount(perUnitCount, rand);
                t.addUnit(new Unit(), tokens);
                ++size;
            }
            System.out.format(" Done in %.3fs\n", (System.currentTimeMillis() - time) / 1000.0);
        }
        printDistribution(t);
    }
}
