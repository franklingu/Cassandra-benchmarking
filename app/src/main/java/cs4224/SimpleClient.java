package cs4224;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

/**
 * Created by Gison on 4/10/15.
 */
public class SimpleClient {
    private Cluster cluster;
    private Session session;

    public void connect(String node, String keyspace) {
        cluster = Cluster.builder()
                .addContactPoint(node)
                .build();
        session = cluster.connect(keyspace);
    }

    public Session getSession() {
        return this.session;
    }

    public void close() {
        cluster.close();
    }

}
