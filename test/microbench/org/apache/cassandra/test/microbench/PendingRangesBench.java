package org.apache.cassandra.test.microbench;

import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.PendingRangeMaps;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 50, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 3,jvmArgsAppend = "-Xmx512M")
@Threads(1)
@State(Scope.Benchmark)
public class PendingRangesBench
{
    PendingRangeMaps pendingRangeMaps;
    int maxToken = 256 * 100;

    private Range<Token> genRange(String left, String right)
    {
        return new Range<Token>(new RandomPartitioner.BigIntegerToken(left), new RandomPartitioner.BigIntegerToken(right));
    }

    @Setup
    public void setUp(final Blackhole bh) throws UnknownHostException {
        pendingRangeMaps = new PendingRangeMaps();

        InetAddress address = InetAddress.getByName("127.0.0.1");
        int wrapAroundStartToken = 256 * 90;
        int numWrapAroundToken = 256 * 10;

        for (int i = 0; i < maxToken; i++)
        {
            pendingRangeMaps.addPendingRange(
                genRange(Integer.toString(i * 10 + 5), Integer.toString(i * 10 + 15)), address);
        }

        for (int i = 0; i < numWrapAroundToken; i++)
        {
            pendingRangeMaps.addPendingRange(
                genRange(Integer.toString(i * 10 + wrapAroundStartToken + 5), Integer.toString(i * 10 + 5)), address
            );
        }
    }

    @Benchmark
    public void searchToken()
    {
        int randomToken = ThreadLocalRandom.current().nextInt(maxToken);
        Token searchToken = new RandomPartitioner.BigIntegerToken(Integer.toString(randomToken));
        pendingRangeMaps.pendingEndpointsFor(searchToken);
    }

}
