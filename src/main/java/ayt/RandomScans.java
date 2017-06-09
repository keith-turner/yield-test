package ayt;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

public class RandomScans {
  public static void main(String[] args) throws Exception {
    Properties props = new Properties();

    try (FileInputStream fis = new FileInputStream(new File(args[0]))) {
      props.load(fis);
    }


    Connector conn = Util.getConnector(props);
    String table = props.getProperty("table");
    int numRows = Integer.parseInt(props.getProperty("rows"));
    int numShortScans = Integer.parseInt(props.getProperty("numShortScans"));
    int rangesPerScan = Integer.parseInt(props.getProperty("rangesPerScan"));

    try(BatchScanner scanner = conn.createBatchScanner(table, Authorizations.EMPTY, 3)){
      Random rand = new Random();
      List<Range> ranges = new ArrayList<>();

      SummaryStatistics stats = new SummaryStatistics();

      for(int i = 0; i < numShortScans; i++) {
        ranges.clear();
        for(int j = 0; j < rangesPerScan; j++){
          ranges.add(Range.exact(String.format("row%09d", rand.nextInt(numRows))));
        }
        scanner.setRanges(ranges);
        int count = 0;
        long t1 = System.currentTimeMillis();
        for (Entry<Key,Value> entry : scanner) {
          count++;
        }
        long t2 = System.currentTimeMillis();
        stats.addValue(t2-t1);
        //System.out.printf("time: %d  ranges: %d\n", t2-t1, count);
      }

      System.out.println(stats);
    }
  }
}
