package com.fhw;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import java.util.UUID;

public class CLoad {

    private Cluster cluster;
    private Session session;

    public static void main(String args[]) 
    {
        System.out.println("go");
        CLoad c = new CLoad();
        c.connect("localhost");
        c.readSomeRows();
        c.close();
    }

    public void connect(String node)
    {
        cluster = Cluster.builder().addContactPoint(node).build();
        Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
        for (Host host : metadata.getAllHosts())
        {
            System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
        }
        session = cluster.connect();
    }
    
    public void readSomeRows()
    {        
        ResultSet results = session.execute("select * from	mykeyspace.reports ;");
        for (Row row : results)
        {
            UUID uuid = row.getUUID("report_uuid"); 
            System.out.println(String.format("%-30s\t%-20s\t%d", uuid.toString(),row.getString("report_id"),row.getInt("bank_id")));
        }        
    }
    
   public void close()
   {
        session.shutdown();
        cluster.shutdown();
   }
    
}

//
//INSERT INTO mykeyspace.reports (report_uuid,bank_id,year, month, day, report_id, description)
//VALUES (756716f7-2e54-4715-9f00-91dcbea6cf50, 47900, 2012,1, 15, '0790', 'this is da bomb');