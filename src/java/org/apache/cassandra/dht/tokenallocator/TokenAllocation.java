package org.apache.cassandra.dht.tokenallocator;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;

import org.apache.commons.math.stat.descriptive.SummaryStatistics;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.TokenMetadata;

public class TokenAllocation {
    
    public static Collection<Token> allocateTokens(final TokenMetadata tokenMetadata,
                                                   final AbstractReplicationStrategy rs,
                                                   final IPartitioner partitioner,
                                                   final InetAddress endpoint,
                                                   int numTokens)
    {
        return create(tokenMetadata, rs, partitioner, endpoint).addUnit(endpoint, numTokens);
    }
    
    static public Map<InetAddress, Double> evaluateReplicatedOwnership(TokenMetadata tokenMetadata, AbstractReplicationStrategy rs)
    {
        Map<InetAddress, Double> ownership = Maps.newHashMap();
        List<Token> sortedTokens = tokenMetadata.sortedTokens();
        Iterator<Token> it = sortedTokens.iterator();
        Token current = it.next();
        while (it.hasNext()) {
            Token next = it.next();
            addOwnership(tokenMetadata, rs, current, next, ownership);
            current = next;
        }
        addOwnership(tokenMetadata, rs, current, sortedTokens.get(0), ownership);

        return ownership;
    }

    static void addOwnership(final TokenMetadata tokenMetadata, final AbstractReplicationStrategy rs, Token current, Token next, Map<InetAddress, Double> ownership)
    {
        double size = current.size(next);
        for (InetAddress n : rs.calculateNaturalEndpoints(current, tokenMetadata))
        {
            Double v = ownership.get(n);
            ownership.put(n, v != null ? v + size : size);
        }
    }
    
    static public String replicatedOwnershipAsText(TokenMetadata tokenMetadata, AbstractReplicationStrategy rs, InetAddress endpoint)
    {
        SummaryStatistics stat = replicatedOwnershipStats(tokenMetadata, rs, endpoint);
        return String.format("max %.2f min %.2f stddev %.4f", stat.getMax() / stat.getMean(), stat.getMin() / stat.getMean(), stat.getStandardDeviation());
    }

    static public SummaryStatistics replicatedOwnershipStats(TokenMetadata tokenMetadata,
            AbstractReplicationStrategy rs, InetAddress endpoint)
    {
        SummaryStatistics stat = new SummaryStatistics();
        IEndpointSnitch snitch = rs.snitch;
        String dc = snitch.getDatacenter(endpoint);
        for (Map.Entry<InetAddress, Double> en : evaluateReplicatedOwnership(tokenMetadata, rs).entrySet())
        {
            // Filter only in the same datacentre.
            if (Objects.equal(dc, snitch.getDatacenter(en.getKey())))
                stat.addValue(en.getValue());
        }
        return stat;
    }

    static TokenAllocator<InetAddress> create(final TokenMetadata tokenMetadata, final AbstractReplicationStrategy rs, IPartitioner partitioner, final InetAddress endpoint)
    {
        if (rs instanceof NetworkTopologyStrategy)
            return create(tokenMetadata, (NetworkTopologyStrategy) rs, rs.snitch, partitioner, endpoint);

        NavigableMap<Token, InetAddress> sortedTokens = new TreeMap<>(tokenMetadata.getNormalAndBootstrappingTokenToEndpointMap());
        final int replicas = rs.getReplicationFactor();

        ReplicationStrategy<InetAddress> strategy = new ReplicationStrategy<InetAddress>() {
            @Override
            public int replicas()
            {
                return replicas;
            }

            @Override
            public Object getGroup(InetAddress unit)
            {
                return unit;
            }
        };
        return new ReplicationAwareTokenAllocator<>(sortedTokens, strategy, partitioner);
    }

    static TokenAllocator<InetAddress> create(final TokenMetadata tokenMetadata, final NetworkTopologyStrategy rs, final IEndpointSnitch snitch, IPartitioner partitioner, final InetAddress endpoint)
    {
        String dc = snitch.getDatacenter(endpoint);
        final int replicas = rs.getReplicationFactor(dc);

        NavigableMap<Token, InetAddress> sortedTokens = new TreeMap<>();
        for (Map.Entry<Token, InetAddress> en : tokenMetadata.getNormalAndBootstrappingTokenToEndpointMap().entrySet())
        {
            if (dc.equals(snitch.getDatacenter(en.getValue())))
                    sortedTokens.put(en.getKey(), en.getValue());
        }

        ReplicationStrategy<InetAddress> strategy = new ReplicationStrategy<InetAddress>() {

            @Override
            public int replicas()
            {
                return replicas;
            }

            @Override
            public Object getGroup(InetAddress unit)
            {
                return snitch.getRack(unit);
            }
        };
        return new ReplicationAwareTokenAllocator<>(sortedTokens, strategy, partitioner);
    }

}

