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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;

class ReplicationAwareTokenAllocator<Unit> implements TokenAllocator<Unit> {

    NavigableMap<Token, Unit> sortedTokens;
    Multimap<Unit, Token> unitToTokens;
    ReplicationStrategy<Unit> strategy;
    IPartitioner partitioner;
    int replicas;
    
    ReplicationAwareTokenAllocator(NavigableMap<Token, Unit> sortedTokens, ReplicationStrategy<Unit> strategy, IPartitioner partitioner)
    {
        this.sortedTokens = new TreeMap<>(sortedTokens);
        unitToTokens = HashMultimap.create();
        for (Map.Entry<Token, Unit> en: sortedTokens.entrySet())
            unitToTokens.put(en.getValue(), en.getKey());
        this.strategy = strategy;
        this.replicas = strategy.replicas();
        this.partitioner = partitioner;
    }

    public Collection<Token> addUnit(Unit newUnit, int numTokens)
    {
        assert !unitToTokens.containsKey(newUnit);
        // Strategy must already know about this unit.
        if (unitCount() < replicas)
            // Allocation does not matter; everything replicates everywhere.
            return generateRandomTokens(newUnit, numTokens);
        if (numTokens > sortedTokens.size())
            // Some of the heuristics below can't deal with this case. Use random for now, later allocations can fix any problems this may cause.
            return generateRandomTokens(newUnit, numTokens);
        
        double optTokenOwnership = optimalTokenOwnership(numTokens);
        Map<Object, GroupInfo> groups = Maps.newHashMap();
        UnitInfo<Unit> newUnitInfo = new UnitInfo<>(newUnit, numTokens * optTokenOwnership, groups, strategy);
        TokenInfo<Unit> tokens = createTokenInfos(createUnitInfos(groups), newUnitInfo.group);
        newUnitInfo.tokenCount = numTokens;

        CandidateInfo<Unit> candidates = createCandidates(tokens, newUnitInfo, optTokenOwnership);

        // Evaluate the expected improvements from all candidates and form a priority queue.
        PriorityQueue<Weighted<CandidateInfo<Unit>>> improvements = new PriorityQueue<>(sortedTokens.size());
        CandidateInfo<Unit> candidate = candidates;
        do
        {
            double impr = evaluateImprovement(candidate, optTokenOwnership, 1.0 / numTokens);
            improvements.add(new Weighted<>(impr, candidate));
            candidate = candidate.next;
        } while (candidate != candidates);
        CandidateInfo<Unit> bestToken = improvements.remove().value;
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
                Weighted<CandidateInfo<Unit>> next = improvements.peek();

                // If it is better than the next in the queue, it is good enough. This is a heuristic that doesn't
                // get the best results, but works well enough and on average cuts search time by a factor of O(vunits).
                if (next == null || impr >= next.weight)
                    break;
                improvements.add(new Weighted<>(impr, bestToken));
            }
        }
        return ImmutableList.copyOf(unitToTokens.get(newUnit));
    }

    private Collection<Token> generateRandomTokens(Unit newUnit, int numTokens)
    {
        Set<Token> tokens = new HashSet<Token>(numTokens);
        while (tokens.size() < numTokens)
        {
            Token token = partitioner.getRandomToken();
            if (!sortedTokens.containsKey(token))
            {
                tokens.add(token);
                addSelectedToken(token, newUnit);
            }
        }
        return tokens;
    }

    private Map<Unit, UnitInfo<Unit>> createUnitInfos(Map<Object, GroupInfo> groups)
    {
        Map<Unit, UnitInfo<Unit>> map = Maps.newHashMap();
        for (Unit n: sortedTokens.values()) {
            UnitInfo<Unit> ni = map.get(n);
            if (ni == null)
                map.put(n, ni = new UnitInfo<>(n, 0, groups, strategy));
            ni.tokenCount++;
        }
        return map;
    }

    private TokenInfo<Unit> createTokenInfos(Map<Unit, UnitInfo<Unit>> units, GroupInfo newUnitGroup)
    {
        TokenInfo<Unit> prev = null;
        TokenInfo<Unit> first = null;
        for (Map.Entry<Token, Unit> en: sortedTokens.entrySet())
        {
            Token t = en.getKey();
            UnitInfo<Unit> ni = units.get(en.getValue());
            TokenInfo<Unit> ti = new TokenInfo<>(t, ni);
            first = ti.insertAfter(first, prev);
            prev = ti;
        }

        TokenInfo<Unit> curr = first;
        do {
            populateTokenInfoAndAdjustUnit(curr, newUnitGroup);
            curr = curr.next;
        } while (curr != first);

        return first;
    }

    private CandidateInfo<Unit> createCandidates(TokenInfo<Unit> tokens, UnitInfo<Unit> newUnitInfo, double initialTokenOwnership)
    {
        TokenInfo<Unit> curr = tokens;
        CandidateInfo<Unit> first = null;
        CandidateInfo<Unit> prev = null;
        do {
            CandidateInfo<Unit> candidate = new CandidateInfo<Unit>(partitioner.midpoint(curr.token, curr.next.token), curr, newUnitInfo);
            first = candidate.insertAfter(first, prev);
            
            candidate.replicatedOwnership = initialTokenOwnership;
            populateCandidate(candidate);
            
            prev = candidate;
            curr = curr.next;
        } while (curr != tokens);
        prev.next = first;
        return first;
    }

    private void populateCandidate(CandidateInfo<Unit> candidate)
    {
        // Only finding replication start would do.
        populateTokenInfo(candidate, candidate.owningUnit.group);
    }

    private void adjustData(CandidateInfo<Unit> candidate)
    {
        // This process is less efficient than it could be (loops through each vunits's replication span instead
        // of recalculating replicationStart, rfm1 and expandable from existing data + new token data in an O(1)
        // case analysis similar to evaluateImprovement). This is fine as the method does not dominate processing
        // time.
        
        // Put the accepted candidate in the token list.
        TokenInfo<Unit> host = candidate.host;
        TokenInfo<Unit> next = host.next;
        TokenInfo<Unit> candidateToken = new TokenInfo<>(candidate.token, candidate.owningUnit);
        candidateToken.replicatedOwnership = candidate.replicatedOwnership;
        candidateToken.insertAfter(host, host);   // List is not empty so this won't need to change head of list.

        // Update data for both candidate and host.
        UnitInfo<Unit> newUnit = candidateToken.owningUnit;
        GroupInfo newGroup = newUnit.group;
        populateTokenInfoAndAdjustUnit(candidateToken, newGroup);
        UnitInfo<Unit> hostUnit = host.owningUnit;
        GroupInfo hostGroup = hostUnit.group;
        populateTokenInfoAndAdjustUnit(host, newGroup);

        GroupInfo groupChain = GroupInfo.TERMINATOR;

        // Loop through all vunits that replicate new token or host and update their data.
        // Also update candidate data for units in the replication span.
        int seenOld = 0;
        int seenNew = 0;
        int rf = replicas - 1;
      evLoop:
        for (TokenInfo<Unit> curr = next; seenOld < rf || seenNew < rf; curr = next)
        {
            candidate = candidate.next;
            populateCandidate(candidate);

            next = curr.next;
            UnitInfo<Unit> currUnit = curr.owningUnit;
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
    
    private Token populateTokenInfo(BaseTokenInfo<Unit, ?> token, GroupInfo newUnitGroup)
    {
        GroupInfo groupChain = GroupInfo.TERMINATOR;
        GroupInfo tokenGroup = token.owningUnit.group;
        int seenGroups = 0;
        boolean expandable = false;
        int rf = replicas;
        // Replication start = the end of a token from the RF'th different group seen before the token.
        Token rs = token.token;
        // The end of a token from the RF-1'th different group seen before the token.
        Token rfm1 = rs;
        for (TokenInfo<Unit> curr = token.prevInRing();;rs = curr.token, curr = curr.prev) {
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
    
    private void populateTokenInfoAndAdjustUnit(TokenInfo<Unit> candidate, GroupInfo newUnitGroup)
    {
        Token rs = populateTokenInfo(candidate, newUnitGroup);
        double newOwnership = rs.size(candidate.next.token);
        double oldOwnership = candidate.replicatedOwnership;
        candidate.replicatedOwnership = newOwnership;
        candidate.owningUnit.ownership += newOwnership - oldOwnership;
    }

    private double evaluateImprovement(CandidateInfo<Unit> candidate, double optTokenOwnership, double newUnitMult)
    {
        double change = 0;
        TokenInfo<Unit> host = candidate.host;
        TokenInfo<Unit> next = host.next;
        Token cend = next.token;

        // Reflect change in ownership of the splitting token (candidate).
        double oldOwnership = candidate.replicatedOwnership;
        double newOwnership = candidate.replicationStart.size(next.token);
        UnitInfo<Unit> newUnit = candidate.owningUnit;
        double tokenCount = newUnit.tokenCount;
        change += (sq(newOwnership - optTokenOwnership) - sq(oldOwnership - optTokenOwnership)) / sq(tokenCount);
        assert tokenCount > 0;
        newUnit.adjustedOwnership = newUnit.ownership + newOwnership - oldOwnership;
        
        // Reflect change of ownership in the token being split (host).
        oldOwnership = host.replicatedOwnership;
        newOwnership = host.replicationStart.size(candidate.token);
        UnitInfo<Unit> hostUnit = host.owningUnit;
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
        UnitInfo<Unit> unitsChain = hostUnit;

        // Loop through all vunits that replicate candidate or host and update their ownership.
        int seenOld = 0;
        int seenNew = 0;
        int rf = replicas - 1;
      evLoop:
        for (TokenInfo<Unit> curr = next; seenOld < rf || seenNew < rf; curr = next)
        {
            next = curr.next;
            UnitInfo<Unit> currUnit = curr.owningUnit;
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
            UnitInfo<Unit> prev = unitsChain.prevUsed;
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

    private void addSelectedToken(Token token, Unit unit)
    {
        sortedTokens.put(token, unit);
        unitToTokens.put(unit, token);
    }
    
    private Map.Entry<Token, Unit> mapEntryFor(Token t)
    {
        Map.Entry<Token, Unit> en = sortedTokens.floorEntry(t);
        if (en == null)
            en = sortedTokens.lastEntry();
        return en;
    }
    
    Unit unitFor(Token t)
    {
        return mapEntryFor(t).getValue();
    }

    private double optimalTokenOwnership(int tokensToAdd)
    {
        return 1.0 * replicas / (sortedTokens.size() + tokensToAdd);
    }

    private static boolean preceeds(Token t1, Token t2, Token towards)
    {
        return t1.size(towards) > t2.size(towards);
    }

    private static double sq(double d)
    {
        return d*d;
    }


    /**
     * For testing, remove the given unit preserving correct state of the allocator.
     */
    void removeUnit(Unit n)
    {
        Collection<Token> tokens = unitToTokens.removeAll(n);
        sortedTokens.keySet().removeAll(tokens);
    }
    
    int unitCount()
    {
        return unitToTokens.asMap().size();
    }
    
    public String toString()
    {
        return getClass().getSimpleName();
    }

    private static <Unit> GroupInfo getGroup(Unit unit, Map<Object, GroupInfo> groupMap, ReplicationStrategy<Unit> strategy)
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
    private static class GroupInfo {
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
    static class UnitInfo<Unit> {
        final Unit unit;
        final GroupInfo group;
        double ownership;
        int tokenCount;

        /** During evaluateImprovement this is used to form a chain of units affected by the candidate insertion. */
        UnitInfo<Unit> prevUsed;
        /** During evaluateImprovement this holds the ownership after the candidate insertion. */
        double adjustedOwnership;

        private UnitInfo(Unit unit, GroupInfo group)
        {
            this.unit = unit;
            this.group = group;
            this.tokenCount = 0;
        }
        
        public UnitInfo(Unit unit, double ownership, Map<Object, GroupInfo> groupMap, ReplicationStrategy<Unit> strategy)
        {
            this(unit, getGroup(unit, groupMap, strategy));
            this.ownership = ownership;
        }
        
        public String toString()
        {
            return String.format("%s%s(%.2e)%s", unit, group.prevSeen != null ? "*" : "", ownership, prevUsed != null ? "prev " + (prevUsed == this ? "this" : prevUsed.toString()) : ""); 
        }
    }

    private static class CircularList<T extends CircularList<T>> {
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
    
    private static class BaseTokenInfo<Unit, T extends BaseTokenInfo<Unit, T>> extends CircularList<T> {
        final Token token;
        final UnitInfo<Unit> owningUnit;

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
        
        public BaseTokenInfo(Token token, UnitInfo<Unit> owningUnit)
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
        TokenInfo<Unit> prevInRing()
        {
            return null;
        }
    }
    
    /**
     * TokenInfo about existing tokens/vunits.
     */
    private static class TokenInfo<Unit> extends BaseTokenInfo<Unit, TokenInfo<Unit>> {
        public TokenInfo(Token token, UnitInfo<Unit> owningUnit)
        {
            super(token, owningUnit);
        }
        
        TokenInfo<Unit> prevInRing()
        {
            return prev;
        }
    }
    
    /**
     * TokenInfo about candidate new tokens/vunits.
     */
    private static class CandidateInfo<Unit> extends BaseTokenInfo<Unit, CandidateInfo<Unit>> {
        final TokenInfo<Unit> host;

        public CandidateInfo(Token token, TokenInfo<Unit> host, UnitInfo<Unit> owningUnit)
        {
            super(token, owningUnit);
            this.host = host;
        }
        
        TokenInfo<Unit> prevInRing()
        {
            return host;
        }
    }
    
    static void dumpTokens(String lead, BaseTokenInfo<?, ?> tokens)
    {
        BaseTokenInfo<?, ?> token = tokens;
        do {
            System.out.format("%s%s: rs %s rfm1 %s size %.2e\n", lead, token, token.replicationStart, token.rfm1Token, token.replicatedOwnership);
            token = token.next;
        } while (token != null && token != tokens);
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
}

