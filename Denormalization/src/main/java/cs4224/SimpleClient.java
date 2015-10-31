package cs4224;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;

/**
 * Created by Gison on 4/10/15.
 */
public class SimpleClient {
    private Cluster cluster;
    private Session session;

    public void connect(String node, String keyspace) {
        cluster = Cluster.builder()
                .addContactPoint(node)
                .withLoadBalancingPolicy(new DCAwareRoundRobinPolicy())
                .build();
        cluster.getConfiguration().getPoolingOptions().setMaxConnectionsPerHost(HostDistance.LOCAL, 1000);
        cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(1000000000);
        session = cluster.connect(keyspace);
    }

    public Session getSession() {
        return this.session;
    }

    public void close() {
        session.close();
        cluster.close();
    }

}
