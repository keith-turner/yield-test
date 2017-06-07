package ayt;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Mutation;

public class Write {
  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    if (args.length == 1) {
      try (FileInputStream fis = new FileInputStream(new File(args[0]))) {
        props.load(fis);
      }
    }

    Connector conn = Util.getConnector(props);
    String table = props.getProperty("table", "yieldTest");
    int numRows = Integer.parseInt(props.getProperty("rows", "10000000"));

    try (BatchWriter writer = conn.createBatchWriter(table, new BatchWriterConfig())) {
      for (int r = 0; r < numRows; r++) {
        String row = String.format("row%09d", r);
        Mutation m = new Mutation(String.format("row%09d", r));
        m.put("f22", "q11", row.hashCode() + "");
        writer.addMutation(m);
      }
    }
  }
}
