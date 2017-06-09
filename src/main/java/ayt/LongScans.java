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
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

public class LongScans {
  public static void main(String[] args) throws Exception {
    Properties props = new Properties();

    try (FileInputStream fis = new FileInputStream(new File(args[0]))) {
      props.load(fis);
    }


    Connector conn = Util.getConnector(props);
    String table = props.getProperty("table");
    String suffix = props.getProperty("suffix");
    int numScans = Integer.parseInt(props.getProperty("numLongScans"));
    String yieldCount = props.getProperty("yieldCount");


    ExecutorService executor = Executors.newCachedThreadPool();
    try {

      List<Future<Long>> futures = new ArrayList<>();

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

            long t1 = System.currentTimeMillis();

            for (Entry<Key, Value> entry : scanner) {
              if (!entry.getKey().getRowData().toString().endsWith(suffix)) {
                System.err.println("Saw incorrect suffix " + entry.getKey());
              }

              count++;
            }

            long t2 = System.currentTimeMillis();

            return t2-t1;
          } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
          }
        }));
      }

      SummaryStatistics stats = new SummaryStatistics();

      for (Future<Long> future : futures) {
        long time = future.get();
        stats.addValue(time);
      }

      System.out.println(stats.toString());
    } finally {
      executor.shutdownNow();
    }



  }
}
