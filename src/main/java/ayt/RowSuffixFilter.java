package ayt;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public class RowSuffixFilter extends YieldingFilter {

  ByteSequence suffix;

  @Override
  public boolean accept(Key k, Value v) {
    ByteSequence row = k.getRowData();
    int rowLen = row.length();

    if (rowLen >= suffix.length()) {
      return row.subSequence(rowLen - suffix.length(), rowLen).equals(suffix);
    }

    return false;
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options,
      IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    this.suffix = new ArrayByteSequence(options.get("suffix"));
  }

}
