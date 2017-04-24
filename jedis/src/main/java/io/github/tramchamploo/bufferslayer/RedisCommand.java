package io.github.tramchamploo.bufferslayer;

import redis.clients.jedis.Pipeline;

/**
 * A redis command to be executed
 */
abstract class RedisCommand extends Message {

  final byte[] key;

  RedisCommand(byte[] key) {
    this.key = key;
  }

  /**
   * This implies how redis pipeline execute this command.
   *
   * @param pipeline pipeline to behave on.
   */
  protected abstract void apply(Pipeline pipeline);

  @Override
  public MessageKey asMessageKey() {
    return Message.SINGLE_KEY;
  }

  String keysString() {
    return new String(key);
  }

  abstract static class MultiKeyCommand extends RedisCommand {

    final byte[][] keys;

    MultiKeyCommand(byte[][] keys) {
      super(keys[0]);
      this.keys = keys;
    }

    String keysString() {
      StringBuilder builder = new StringBuilder();
      for (int i = 0; i < keys.length; i++) {
        builder.append(new String(keys[i]));
      }
      return builder.toString();
    }
  }

  // Commands start

  final static class Append extends RedisCommand {

    final byte[] value;

    Append(byte[] key, byte[] value) {
      super(key);
      this.value = value;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.append(key, value);
    }

    @Override
    public String toString() {
      return "Append(" + new String(key) + ", " + new String(value) + ")";
    }
  }

  final static class Blpop extends MultiKeyCommand {

    final int timeout;

    Blpop(int timeout, byte[]... keys) {
      super(keys);
      this.timeout = timeout;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      if (timeout == 0) {
        pipeline.blpop(keys);
      } else {
        pipeline.blpop(timeout, keys);
      }
    }

    @Override
    public String toString() {
      return "Blpop(" + keysString() + ", " + timeout + ")";
    }
  }

  final static class Brpop extends MultiKeyCommand {

    final int timeout;

    Brpop(int timeout, byte[]... keys) {
      super(keys);
      this.timeout = timeout;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      if (timeout == 0) {
        pipeline.brpop(keys);
      } else {
        pipeline.brpop(timeout, keys);
      }
    }

    @Override
    public String toString() {
      return "Brpop(" + keysString() + ", " + timeout + ")";
    }
  }

  final static class Decr extends RedisCommand {

    Decr(byte[] key) {
      super(key);
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.decr(key);
    }

    @Override
    public String toString() {
      return "Decr(" + keysString() + ")";
    }
  }

  final static class DecrBy extends RedisCommand {

    final long value;

    DecrBy(byte[] key, long value) {
      super(key);
      this.value = value;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.decrBy(key, value);
    }

    @Override
    public String toString() {
      return "DecrBy(" + keysString() + ", " +  + value + ")";
    }
  }

  final static class Del extends MultiKeyCommand {

    Del(byte[]... keys) {
      super(keys);
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.del(keys);
    }

    @Override
    public String toString() {
      return "Del(" + keysString() + ")";
    }
  }

  final static class Echo extends RedisCommand {

    Echo(byte[] key) {
      super(key);
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.echo(key);
    }

    @Override
    public String toString() {
      return "Echo(" + keysString() + ")";
    }
  }

  final static class Exists extends MultiKeyCommand {

    Exists(byte[]... keys) {
      super(keys);
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.exists(keys);
    }

    @Override
    public String toString() {
      return "Exists(" + keysString() + ")";
    }
  }
}
