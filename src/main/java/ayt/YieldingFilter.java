package ayt;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.YieldCallback;

public abstract class YieldingFilter implements SortedKeyValueIterator<Key, Value> {

  private SortedKeyValueIterator<Key, Value> source;
  private int yieldAfter;
  private YieldCallback<Key> callback;

  public abstract boolean accept(Key k, Value v);

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options,
      IteratorEnvironment env) throws IOException {
    this.source = source;
    this.yieldAfter = Integer.parseInt(options.getOrDefault("yieldCount", "10000"));
  }

  @Override
  public boolean hasTop() {
    return source.hasTop() && !callback.hasYielded();
  }

  protected void findTop() throws IOException {
    int count = 0;
    while (source.hasTop() && !source.getTopKey().isDeleted()
        && !accept(source.getTopKey(), source.getTopValue())) {
      if (count > yieldAfter) {
        callback.yield(source.getTopKey());
        break;
      }

      source.next();
      count++;
    }
  }

  @Override
  public void next() throws IOException {
    source.next();
    findTop();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    source.seek(range, columnFamilies, inclusive);
    findTop();
  }

  @Override
  public Key getTopKey() {
    return source.getTopKey();
  }

  @Override
  public Value getTopValue() {
    return source.getTopValue();
  }

  @Override
  public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void enableYielding(YieldCallback<Key> callback) {
    this.callback = callback;
  }

}
