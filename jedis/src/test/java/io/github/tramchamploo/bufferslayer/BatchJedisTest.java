package io.github.tramchamploo.bufferslayer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.tramchamploo.bufferslayer.internal.MessageFuture;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.util.SafeEncoder;

@SuppressWarnings("unchecked")
public class BatchJedisTest {

  final byte[] bfoo = {0x01, 0x02, 0x03, 0x04};
  final byte[] bbar = {0x05, 0x06, 0x07, 0x08};

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private Jedis jedis;
  private BatchJedis batchJedis;
  private AsyncReporter reporter;

  @Before
  public void setup() {
    jedis = new Jedis("localhost", 6379);
    jedis.flushAll();

    reporter = AsyncReporter.builder(new JedisSender(jedis))
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .build();
    batchJedis = new BatchJedis(jedis, reporter);
  }

  @Test
  public void pipelined() {
    Jedis delegate = mock(Jedis.class);
    reporter = AsyncReporter.builder(new JedisSender(delegate))
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .build();

    batchJedis = new BatchJedis(delegate, reporter);

    Pipeline pipeline = mock(Pipeline.class);
    when(delegate.pipelined()).thenReturn(pipeline);

    batchJedis.append("foo", "bar");
    batchJedis.append("foo", "bar");
    reporter.flush();

    verify(delegate).pipelined();
    verify(pipeline, times(2))
        .append(SafeEncoder.encode("foo"), SafeEncoder.encode("bar"));
    verify(pipeline).syncAndReturnAll();
  }

  @Test
  public void append() {
    long value = blocking(batchJedis.append("foo", "bar"));
    assertEquals(3L, value);
    assertEquals("bar", jedis.get("foo"));

    value = blocking(batchJedis.append("foo", "bar"));
    assertEquals(6L, value);
    assertEquals("barbar", jedis.get("foo"));
  }

  @Test
  public void blpop() {
    List<String> value = blocking(batchJedis.blpop(1, "foo"));
    assertNull(value);

    jedis.lpush("foo", "bar");
    value = blocking(batchJedis.blpop(1, "foo"));
    assertEquals(2, value.size());
    assertEquals("foo", value.get(0));
    assertEquals("bar", value.get(1));

    // Binary
    jedis.lpush(bfoo, bbar);
    List<byte[]> value2 = blocking(batchJedis.blpop(1, bfoo));
    assertEquals(2, value2.size());
    assertArrayEquals(bfoo, value2.get(0));
    assertArrayEquals(bbar, value2.get(1));
  }

  @Test
  public void brpop() {
    List<String> value = blocking(batchJedis.brpop(1, "foo"));
    assertNull(value);

    jedis.lpush("foo", "bar");
    value = blocking(batchJedis.brpop(1, "foo"));
    assertEquals(2, value.size());
    assertEquals("foo", value.get(0));
    assertEquals("bar", value.get(1));

    // Binary
    jedis.lpush(bfoo, bbar);
    List<byte[]> value2 = blocking(batchJedis.brpop(1, bfoo));
    assertEquals(2, value2.size());
    assertArrayEquals(bfoo, value2.get(0));
    assertArrayEquals(bbar, value2.get(1));
  }

  @Test
  public void decr() {
    assertEquals(new Long(1L), jedis.incr("foo"));
    assertEquals(new Long(0L), blocking(batchJedis.decr("foo")));

    // Binary
    assertEquals(new Long(1L), jedis.incr(bfoo));
    assertEquals(new Long(0L), blocking(batchJedis.decr(bfoo)));
  }

  @Test
  public void decrBy() {
    assertEquals(new Long(2L), jedis.incrBy("foo", 2));
    assertEquals(new Long(0L), blocking(batchJedis.decrBy("foo", 2)));

    // Binary
    assertEquals(new Long(2L), jedis.incrBy(bfoo, 2));
    assertEquals(new Long(0L), blocking(batchJedis.decrBy(bfoo, 2)));
  }

  @Test
  public void del() {
    assertEquals("OK", jedis.set("foo", "bar"));
    assertEquals(new Long(1L), blocking(batchJedis.del("foo")));

    // Binary
    assertEquals("OK", jedis.set(bfoo, bbar));
    assertEquals(new Long(1L), blocking(batchJedis.del(bfoo)));
  }

  @Test
  public void echo() {
    assertEquals("foo", blocking(batchJedis.echo("foo")));

    // Binary
    assertArrayEquals(bfoo, blocking(batchJedis.echo(bfoo)));
  }

  @Test
  public void exists() {
    assertEquals("OK", jedis.set("foo", "bar"));
    assertEquals(true, blocking(batchJedis.exists("foo")));

    assertEquals("OK", jedis.set("bar", "bar"));
    assertEquals(new Long(2L), blocking(batchJedis.exists("foo", "bar")));

    // Binary
    assertEquals("OK", jedis.set(bfoo, bbar));
    assertEquals(true, blocking(batchJedis.exists(bfoo)));

    assertEquals("OK", jedis.set(bbar, bbar));
    assertEquals(new Long(2L), blocking(batchJedis.exists(bfoo, bbar)));
  }

  private <T> T blocking(MessageFuture<?> future) {
    reporter.flush();

    try {
      return (T) future.get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
