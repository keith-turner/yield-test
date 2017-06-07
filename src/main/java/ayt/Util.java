package ayt;

import java.util.Properties;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;

public class Util {
  @SuppressWarnings("deprecation")
  static Connector getConnector(Properties props)
      throws AccumuloException, AccumuloSecurityException {
    ZooKeeperInstance zki = new ZooKeeperInstance(props.getProperty("instance", "uno"),
        props.getProperty("zookeepers", "localhost"));
    return zki.getConnector(props.getProperty("user", "root"),
        props.getProperty("password", "secret"));
  }

}
