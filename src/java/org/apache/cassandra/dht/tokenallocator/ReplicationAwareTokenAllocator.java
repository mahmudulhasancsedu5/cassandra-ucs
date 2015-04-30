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

/**
 * A Replication Aware allocator for tokens, that attempts to ensure an even distribution of ownership across
 * the known cluster for the provided replication strategy.
 *
 * A unit is shorthand for a "unit of ownership" which translates roughly to a node, or a disk on the node,
 * a CPU on the node, or some other relevant unit of ownership. These units should be the lowest rung over which
 * ownership needs to be evenly distributed. At the moment only nodes as a whole are treated as units, but that
 * will change with the introduction of token ranges per disk.
 */
class ReplicationAwareTokenAllocator<Unit> implements TokenAllocator<Unit>
{
    final NavigableMap<Token, Unit> sortedTokens;
    final Multimap<Unit, Token> unitToTokens;
    final ReplicationStrategy<Unit> strategy;
    final IPartitioner partitioner;
    final int replicas;

    ReplicationAwareTokenAllocator(NavigableMap<Token, Unit> sortedTokens, ReplicationStrategy<Unit> strategy, IPartitioner partitioner)
    {
        this.sortedTokens = sortedTokens;
        unitToTokens = HashMultimap.create();
        for (Map.Entry<Token, Unit> en : sortedTokens.entrySet())
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

        // ============= construct our initial token ring state =============

        double optTokenOwnership = optimalTokenOwnership(numTokens);
        Map<Object, GroupInfo> groups = Maps.newHashMap();
        Map<Unit, UnitInfo<Unit>> unitInfos = createUnitInfos(groups);
        if (groups.size() < replicas)
        {
            // We need at least replicas groups to do allocation correctly. If there aren't enough, 
            // use random allocation.
            // This part of the code should only be reached via the RATATest. StrategyAdapter should disallow
            // token allocation in this case as the algorithm is not able to cover the behavior of NetworkTopologyStrategy.
            return generateRandomTokens(newUnit, numTokens);
        }

        // initialise our new unit's state (with an idealised ownership)
        UnitInfo<Unit> newUnitInfo = new UnitInfo<>(newUnit, numTokens * optTokenOwnership, groups, strategy);

        // build the current token ring state
        TokenInfo<Unit> tokens = createTokenInfos(unitInfos, newUnitInfo.group);
        newUnitInfo.tokenCount = numTokens;

        // ============= construct and rank our candidate token allocations =============

        // walk the token ring, constructing the set of candidates in ring order
        // as the midpoints between all existing tokens
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

        // ============= iteratively take the best candidate, and re-rank =============

        CandidateInfo<Unit> bestToken = improvements.remove().value;
        candidates = bestToken.removeFrom(candidates);
        for (int vn = 0; ; ++vn)
        {
            confirmCandidate(bestToken);

            if (vn == numTokens - 1)
                break;

            while (true)
            {
                // Get the next candidate in the queue. Its improvement may have changed (esp. if multiple tokens
                // were good suggestions because they could improve the same problem)-- evaluate it again to check
                // if it is still a good candidate.
                bestToken = improvements.remove().value;
                candidates = bestToken.removeFrom(candidates);
                double impr = evaluateImprovement(bestToken, optTokenOwnership, (vn + 1.0) / numTokens);
                Weighted<CandidateInfo<Unit>> next = improvements.peek();

                // If it is better than the next in the queue, it is good enough. This is a heuristic that doesn't
                // get the best results, but works well enough and on average cuts search time by a factor of O(vnodes).
                if (next == null || impr >= next.weight)
                    break;
                improvements.add(new Weighted<>(impr, bestToken));
            }
        }
        return ImmutableList.copyOf(unitToTokens.get(newUnit));
    }

    private Collection<Token> generateRandomTokens(Unit newUnit, int numTokens)
    {
        Set<Token> tokens = new HashSet<>(numTokens);
        while (tokens.size() < numTokens)
        {
            Token token = partitioner.getRandomToken();
            if (!sortedTokens.containsKey(token))
            {
                tokens.add(token);
                sortedTokens.put(token, newUnit);
                unitToTokens.put(newUnit, token);
            }
        }
        return tokens;
    }

    private Map<Unit, UnitInfo<Unit>> createUnitInfos(Map<Object, GroupInfo> groups)
    {
        Map<Unit, UnitInfo<Unit>> map = Maps.newHashMap();
        for (Unit n : sortedTokens.values())
        {
            UnitInfo<Unit> ni = map.get(n);
            if (ni == null)
                map.put(n, ni = new UnitInfo<>(n, 0, groups, strategy));
            ni.tokenCount++;
        }
        return map;
    }

    /**
     * Construct the token ring as a CircularList of TokenInfo,
     * and populate the ownership of the UnitInfo's provided
     */
    private TokenInfo<Unit> createTokenInfos(Map<Unit, UnitInfo<Unit>> units, GroupInfo newUnitGroup)
    {
        // build the circular list
        TokenInfo<Unit> prev = null;
        TokenInfo<Unit> first = null;
        for (Map.Entry<Token, Unit> en : sortedTokens.entrySet())
        {
            Token t = en.getKey();
            UnitInfo<Unit> ni = units.get(en.getValue());
            TokenInfo<Unit> ti = new TokenInfo<>(t, ni);
            first = ti.insertAfter(first, prev);
            prev = ti;
        }

        TokenInfo<Unit> curr = first;
        do
        {
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
        do
        {
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

    /**
     * Incorporates the selected candidate and into the ring, adjusting ownership information and calculated token
     * information.
     */
    private void confirmCandidate(CandidateInfo<Unit> candidate)
    {
        // This process is less efficient than it could be (loops through each vnode's replication span instead
        // of recalculating replicationStart, rfm1 and expandable from existing data + new token data in an O(1)
        // case analysis similar to evaluateImprovement). This is fine as the method does not dominate processing
        // time.

        // Put the accepted candidate in the token list.
        UnitInfo<Unit> newUnit = candidate.owningUnit;
        Token newToken = candidate.token;
        sortedTokens.put(newToken, newUnit.unit);
        unitToTokens.put(newUnit.unit, newToken);

        TokenInfo<Unit> split = candidate.split;
        TokenInfo<Unit> newTokenInfo = new TokenInfo<>(newToken, newUnit);
        newTokenInfo.replicatedOwnership = candidate.replicatedOwnership;
        newTokenInfo.insertAfter(split, split);   // List is not empty so this won't need to change head of list.

        // Update data for both candidate and split.
        populateTokenInfoAndAdjustUnit(newTokenInfo, newUnit.group);
        populateTokenInfoAndAdjustUnit(split, newUnit.group);

        ReplicationVisitor replicationVisitor = new ReplicationVisitor(newUnit.group, split.owningUnit.group);
        for (TokenInfo<Unit> curr = newTokenInfo.next; !replicationVisitor.visitedAll() ; curr = curr.next)
        {
            // update the candidate between curr and next
            candidate = candidate.next;
            populateCandidate(candidate);

            if (!replicationVisitor.add(curr.owningUnit.group))
                continue;    // If we've already seen this group, the token cannot be affected.

            populateTokenInfoAndAdjustUnit(curr, newUnit.group);
        }

        replicationVisitor.clean();
    }

    /**
     * Calculates the {@code replicationStart}, {@code replicationMinus1} and {@code expandable} properties of the given
     * {@code TokenInfo}, used by {@code findUpdatedReplicationStart} to quickly identify changes in ownership.
     */
    private Token populateTokenInfo(BaseTokenInfo<Unit, ?> token, GroupInfo newUnitGroup)
    {
        GroupInfo groupChain = GroupInfo.TERMINATOR;
        GroupInfo tokenGroup = token.owningUnit.group;
        int seenGroups = 0;
        boolean expandable = false;
        int rf = replicas;
        // Replication start = the end of a token from the RF'th different group seen before the token.
        Token replicationStart = token.token;
        // The end of a token from the RF-1'th different group seen before the token.
        Token replicationMinus1 = replicationStart;
        for (TokenInfo<Unit> curr = token.prevInRing(); ; replicationStart = curr.token, curr = curr.prev)
        {
            GroupInfo currGroup = curr.owningUnit.group;
            if (currGroup.prevPopulate != null)
                continue; // Group is already seen.

            // Mark the group as seen. Also forms a chain that can be used to clear the marks when we are done.
            // We use prevPopulate instead of prevSeen as this may be called within confirmCandidate which also needs
            // group seen markers.
            currGroup.prevPopulate = groupChain;
            groupChain = currGroup;
            if (++seenGroups == rf)
                break;

            replicationMinus1 = replicationStart;
            if (currGroup == tokenGroup)
            {
                // Another instance of the same group precedes us in the replication range of the ring,
                // so this is where our replication range begins
                // Inserting a token that ends at this boundary will increase replication coverage,
                // but only if the inserted unit is not in the same group.
                expandable = tokenGroup != newUnitGroup;
                break;
            }
            // An instance of the new group in the replication span also means that we can expand coverage
            // by inserting a token ending at replicationStart.
            if (currGroup == newUnitGroup)
                expandable = true;
        }
        token.replicationMinus1 = replicationMinus1;
        token.replicationStart = replicationStart;
        token.expandable = expandable;

        // Clean group seen markers.
        while (groupChain != GroupInfo.TERMINATOR)
        {
            GroupInfo prev = groupChain.prevPopulate;
            groupChain.prevPopulate = null;
            groupChain = prev;
        }
        return replicationStart;
    }

    private void populateTokenInfoAndAdjustUnit(TokenInfo<Unit> populate, GroupInfo newUnitGroup)
    {
        Token replicationStart = populateTokenInfo(populate, newUnitGroup);
        double newOwnership = replicationStart.size(populate.next.token);
        double oldOwnership = populate.replicatedOwnership;
        populate.replicatedOwnership = newOwnership;
        populate.owningUnit.ownership += newOwnership - oldOwnership;
    }

    /**
     * Evaluates the improvement in variance for both units and individual tokens when candidate is inserted into the
     * ring.
     */
    private double evaluateImprovement(CandidateInfo<Unit> candidate, double optTokenOwnership, double newUnitMult)
    {
        double tokenChange = 0;

        UnitInfo<Unit> candidateUnit = candidate.owningUnit;
        TokenInfo<Unit> splitToken = candidate.split;
        UnitInfo<Unit> splitUnit = splitToken.owningUnit;
        Token candidateEnd = splitToken.next.token;

        // Form a chain of units affected by the insertion to be able to qualify change of unit ownership.
        // A unit may be affected more than once.
        UnitAdjustmentTracker<Unit> unitTracker = new UnitAdjustmentTracker<>(candidateUnit);

        // Reflect change in ownership of the splitting token (candidate).
        tokenChange += applyOwnershipAdjustment(candidate, candidateUnit, candidate.replicationStart, candidateEnd, optTokenOwnership, unitTracker);

        // Reflect change of ownership in the token being split
        tokenChange += applyOwnershipAdjustment(splitToken, splitUnit, splitToken.replicationStart, candidate.token, optTokenOwnership, unitTracker);

        // Loop through all vnodes that replicate candidate or split and update their ownership.
        ReplicationVisitor replicationVisitor = new ReplicationVisitor(candidateUnit.group, splitUnit.group);
        for (TokenInfo<Unit> curr = splitToken.next; !replicationVisitor.visitedAll(); curr = curr.next)
        {
            UnitInfo<Unit> currUnit = curr.owningUnit;

            if (!replicationVisitor.add(currUnit.group))
                continue;    // If this group is already seen both before and after splitting, the token cannot be affected.

            Token replicationEnd = curr.next.token;
            Token replicationStart = findUpdatedReplicationStart(curr, currUnit.group, replicationEnd,
                                                                 candidate, candidateUnit.group, candidateEnd);
            tokenChange += applyOwnershipAdjustment(curr, currUnit, replicationStart, replicationEnd, optTokenOwnership, unitTracker);
        }
        replicationVisitor.clean();

        double nodeChange = unitTracker.calculateUnitChange(newUnitMult, optTokenOwnership);
        return -(tokenChange + nodeChange);
    }

    /**
     * Returns the start of the replication span for the token {@code curr} when {@code candidate} is inserted into the
     * ring.
     */
    private Token findUpdatedReplicationStart(TokenInfo<Unit> curr, GroupInfo currGroup, Token currEnd,
                                              CandidateInfo<Unit> candidate, GroupInfo candidateGroup, Token candidateEnd)
    {
        if (currGroup == candidateGroup)
        {
            if (!preceeds(curr.replicationStart, candidateEnd, currEnd))
                return curr.replicationStart; // no changes, another newGroup is closer
            // Replication shrinks to end of candidate.next
            return candidateEnd;
        }
        else if (curr.expandable) // Sees same or new group within rf-1.
        {
            if (candidateEnd != curr.replicationStart)
                return curr.replicationStart;  // candidate does not affect token
            // Replication expands to start of candidate.
            return candidate.token;
        }
        else if (preceeds(curr.replicationMinus1, candidateEnd, currEnd))
            // Candidate is closer than one-before last.
            return curr.replicationMinus1;
        else if (preceeds(candidateEnd, curr.replicationMinus1, currEnd))
            // Candidate is the replication boundary.
            return candidateEnd;
        else
            // Candidate replaces one-before last. The latter becomes the replication boundary.
            return candidate.token;
    }

    /**
     * Applies the ownership adjustment for the given element, updating tracked unit ownership and returning the change
     * of variance.
     */
    private double applyOwnershipAdjustment(BaseTokenInfo<Unit, ?> curr, UnitInfo<Unit> currUnit,
            Token replicationStart, Token replicationEnd,
            double optTokenOwnership, UnitAdjustmentTracker<Unit> unitTracker)
    {
        double oldOwnership = curr.replicatedOwnership;
        double newOwnership = replicationStart.size(replicationEnd);
        double tokenCount = currUnit.tokenCount;
        assert tokenCount > 0;
        unitTracker.add(currUnit, newOwnership - oldOwnership);
        return (sq(newOwnership - optTokenOwnership) - sq(oldOwnership - optTokenOwnership)) / sq(tokenCount);
    }

    /**
     * Tracker for unit ownership changes. The changes are tracked by a chain of UnitInfos where the adjustedOwnership
     * field is being updated as we see changes in token ownership.
     *
     * The chain ends with an element that points to itself; this element must be specified as argument to the
     * constructor as well as be the first unit with which 'add' is called; when calculating the variance change
     * a separate multiplier is applied to it (used to permit more freedom in choosing the first tokens of a unit).
     */
    private static class UnitAdjustmentTracker<Unit>
    {
        UnitInfo<Unit> unitsChain;

        UnitAdjustmentTracker(UnitInfo<Unit> newUnit)
        {
            unitsChain = newUnit;
        }

        void add(UnitInfo<Unit> currUnit, double diff)
        {
            if (currUnit.prevUsed == null)
            {
                assert unitsChain.prevUsed != null || currUnit == unitsChain;

                currUnit.adjustedOwnership = currUnit.ownership + diff;
                currUnit.prevUsed = unitsChain;
                unitsChain = currUnit;
            }
            else
            {
                currUnit.adjustedOwnership += diff;
            }
        }

        double calculateUnitChange(double newUnitMult, double optTokenOwnership)
        {
            double unitChange = 0;
            UnitInfo<Unit> unitsChain = this.unitsChain;
            // Now loop through the units chain and add the unit-level changes. Also clear the groups' seen marks.
            while (true)
            {
                double newOwnership = unitsChain.adjustedOwnership;
                double oldOwnership = unitsChain.ownership;
                double tokenCount = unitsChain.tokenCount;
                double diff = (sq(newOwnership / tokenCount - optTokenOwnership) - sq(oldOwnership / tokenCount - optTokenOwnership));
                UnitInfo<Unit> prev = unitsChain.prevUsed;
                unitsChain.prevUsed = null;
                if (unitsChain != prev)
                    unitChange += diff;
                else
                {
                    unitChange += diff * newUnitMult;
                    break;
                }
                unitsChain = prev;
            }
            this.unitsChain = unitsChain;
            return unitChange;
        }
    }

    /**
     * Loop through all tokens following a candidate or final new token, until all tokens whose replication
     * range may be affected have been visited
     *
     * Once we've visited RF - 1 many groups different to both old and new group, we're done
     */
    private class ReplicationVisitor
    {
        final GroupInfo newGroup, oldGroup;
        // number of groups visited not equal to the new group
        int replicatedFromNew;
        // number of groups visited not equal to the old (split) group
        int replicatedFromOld;

        GroupInfo groupChain = GroupInfo.TERMINATOR;

        private ReplicationVisitor(GroupInfo newGroup, GroupInfo oldGroup)
        {
            this.newGroup = newGroup;
            this.oldGroup = oldGroup;
        }

        // true iff this is the first time we've visited this group
        boolean add(GroupInfo group)
        {
            if (group.prevSeen != null)
                return false;
            if (group != newGroup)
                replicatedFromNew += 1;
            if (group != oldGroup)
                replicatedFromOld += 1;
            group.prevSeen = groupChain;
            groupChain = group;
            return true;
        }

        // true iff we've visited all groups that may be affected
        boolean visitedAll()
        {
            return replicatedFromNew >= replicas - 1 && replicatedFromOld >= replicas - 1;
        }

        // Clean group seen markers.
        void clean()
        {
            GroupInfo groupChain = this.groupChain;
            while (groupChain != GroupInfo.TERMINATOR)
            {
                GroupInfo prev = groupChain.prevSeen;
                groupChain.prevSeen = null;
                groupChain = prev;
            }
            this.groupChain = GroupInfo.TERMINATOR;
        }
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

    // easier mechanism for dealing with wrapping; all we care about is which Token creates the larger range,
    // and consider this to precede the other in the uroboros token ring
    private static boolean preceeds(Token t1, Token t2, Token towards)
    {
        return t1.size(towards) > t2.size(towards);
    }

    private static double sq(double d)
    {
        return d * d;
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

    // get or initialise the shared GroupInfo associated with the unit
    private static <Unit> GroupInfo getGroup(Unit unit, Map<Object, GroupInfo> groupMap, ReplicationStrategy<Unit> strategy)
    {
        Object groupClass = strategy.getGroup(unit);
        GroupInfo group = groupMap.get(groupClass);
        if (group == null)
            groupMap.put(groupClass, group = new GroupInfo(groupClass));
        return group;
    }

    /**
     * Unique group object that one or more UnitInfo objects link to.
     */
    private static class GroupInfo
    {
        /**
         * Group identifier given by ReplicationStrategy.getGroup(Unit).
         */
        final Object group;

        /**
         * Seen marker. When non-null, the group is already seen in replication walks.
         * Also points to previous seen group to enable walking the seen groups and clearing the seen markers.
         */
        GroupInfo prevSeen = null;
        /**
         * Same marker/chain used by populateTokenInfo.
         */
        GroupInfo prevPopulate = null;

        /**
         * Value used as terminator for seen chains.
         */
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
     * Unit information created and used by ReplicationAwareTokenDistributor. Contained vnodes all point to the same
     * instance.
     */
    static class UnitInfo<Unit>
    {
        final Unit unit;
        final GroupInfo group;
        double ownership;
        int tokenCount;

        /**
         * During evaluateImprovement this is used to form a chain of units affected by the candidate insertion.
         */
        UnitInfo<Unit> prevUsed;
        /**
         * During evaluateImprovement this holds the ownership after the candidate insertion.
         */
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

    private static class CircularList<T extends CircularList<T>>
    {
        T prev;
        T next;

        /**
         * Inserts this after unit in the circular list which starts at head. Returns the new head of the list, which
         * only changes if head was null.
         */
        @SuppressWarnings("unchecked")
        T insertAfter(T head, T unit)
        {
            if (head == null)
            {
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
        T removeFrom(T head)
        {
            next.prev = prev;
            prev.next = next;
            return this == head ? (this == next ? null : next) : head;
        }
    }

    private static class BaseTokenInfo<Unit, T extends BaseTokenInfo<Unit, T>> extends CircularList<T>
    {
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
        Token replicationMinus1;
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
         * Previous unit in the token ring. For existing tokens this is prev,
         * for candidates it's "split".
         */
        TokenInfo<Unit> prevInRing()
        {
            return null;
        }
    }

    /**
     * TokenInfo about existing tokens/vnodes.
     */
    private static class TokenInfo<Unit> extends BaseTokenInfo<Unit, TokenInfo<Unit>>
    {
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
     * TokenInfo about candidate new tokens/vnodes.
     */
    private static class CandidateInfo<Unit> extends BaseTokenInfo<Unit, CandidateInfo<Unit>>
    {
        // directly preceding token in the current token ring
        final TokenInfo<Unit> split;

        public CandidateInfo(Token token, TokenInfo<Unit> split, UnitInfo<Unit> owningUnit)
        {
            super(token, owningUnit);
            this.split = split;
        }

        TokenInfo<Unit> prevInRing()
        {
            return split;
        }
    }

    static void dumpTokens(String lead, BaseTokenInfo<?, ?> tokens)
    {
        BaseTokenInfo<?, ?> token = tokens;
        do
        {
            System.out.format("%s%s: rs %s rfm1 %s size %.2e\n", lead, token, token.replicationStart, token.replicationMinus1, token.replicatedOwnership);
            token = token.next;
        } while (token != null && token != tokens);
    }

    static class Weighted<T> implements Comparable<Weighted<T>>
    {
        final double weight;
        final T value;

        public Weighted(double weight, T value)
        {
            this.weight = weight;
            this.value = value;
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

