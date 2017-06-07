package ayt;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

public class LongScans {
  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    if (args.length == 1) {
      try (FileInputStream fis = new FileInputStream(new File(args[0]))) {
        props.load(fis);
      }
    }

    Connector conn = Util.getConnector(props);
    String table = props.getProperty("table", "yieldTest");
    String suffix = props.getProperty("suffix", "973973");
    int numScans = Integer.parseInt(props.getProperty("numLongScans", "10"));
    String yieldCount = props.getProperty("yieldCount", "10000");


    ExecutorService executor = Executors.newCachedThreadPool();
    try {

      List<Future<Integer>> futures = new ArrayList<>();

      for (int i = 0; i < numScans; i++) {
        futures.add(executor.submit(() -> {
          try (Scanner scanner = conn.createScanner(table, Authorizations.EMPTY)) {
            IteratorSetting iterCfg =
                new IteratorSetting(100, "suffixFilter", RowSuffixFilter.class);
            iterCfg.addOption("yieldCount", yieldCount);
            iterCfg.addOption("suffix", suffix);
            scanner.addScanIterator(iterCfg);
            scanner.setReadaheadThreshold(0);

            int count = 0;
            for (Entry<Key, Value> entry : scanner) {
              if (!entry.getKey().getRowData().toString().endsWith(suffix)) {
                System.err.println("Saw incorrect suffix " + entry.getKey());
              }

              count++;
            }

            return count;
          } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
          }
        }));
      }

      for (Future<Integer> future : futures) {
        int count = future.get();
        System.out.println("Got count " + count);
      }

    } finally {
      executor.shutdownNow();
    }



  }
}
