package com.fhw;

import com.beust.jcommander.*;
import com.beust.jcommander.converters.*;
import com.datastax.driver.core.*;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.*;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import com.fhw.Statement;
import dnl.utils.text.table.*;
import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;
import java.util.UUID;

public class CLoad
{
    @Parameter(names = "-node", description = "cassandra host:port", converter = HostPortConverter.class, required = true)
    private List<HostPort> nodes = new ArrayList<>();
    
    @Parameter(names = "-date", description = "specfic date to operate on (fmt  yyyy-MM-dd)")
    private Date statementDate; 
    
    @Parameter(names = "-customerid", description = "The integral id of the customer performing operation")
    private Integer customerId;     
    
    @Parameter(names ="-keyspace", description = "key space name to work on (default 'statementarchive')")
    private String keyspace = "statementarchive";
    
    @Parameter(names ="-table", description = "key space name to work on (default 'statements')")
    private String table = "statements";
    
    @Parameter(names ="-statementtype", description = "(required)The statement type")
    private String statementType;    
    
    @Parameter(names ="-file", description = "(required)The path & name of the statement file to operate on", converter = FileConverter.class)
    private File statementFile;  
    
    @Parameter(names = "-consistency", description="(required)The consistency level of the cassandra operation", converter = ConsistencyLevelConverter.class)
    private ConsistencyLevel clevel = ConsistencyLevel.ONE;
    
    @Parameter(names = "-select", description="if specified, the program will select files based on input parameters")
    private Boolean doSelect = Boolean.FALSE;
    
    @Parameter(names = "-uuid", description = "specify a uuid to looks for. only make sense with -select", converter = UUIDConverter.class)
    private UUID statementUUID; 
    
    private PrintStream pStream = System.out; 
    
    private Cluster cluster;
    private Session session;

    public static void main(String args[])
    {
        //String margs[] = new String[]{"-node", "127.0.0.1", "-node", "127.0.0.2", "-node", "127.0.0.3", "-select",  "-uuid", "de7436ce-a096-4d3a-a210-c833cb6ad9db","-date", "2014-02-26", "-customerid", "4799"};
        //String margs[] = new String[]{"-node", "127.0.0.1", "-node", "127.0.0.2", "-node", "127.0.0.3",  "-date", "2014-02-26", "-customerid", "4799", "-statementtype", "9700", "-consistency", "ONE", "-file", "/home/fwelland/Downloads/pdf-sample.pdf"};        
        //String margs[] = new String[]{"-node", "127.0.0.1",  "-date", "2014-02-27", "-customerid", "4799", "-statementtype", "9700", "-file", "/home/fwelland/Downloads/pdf-sample.pdf"};        
        //String margs[] = new String[]{"-node", "127.0.0.1", "-node", "127.0.0.2", "-node", "127.0.0.3", "-select",  "-uuid", "53ad6a82-cfa6-4808-9423-9f76d9fd6de9"};
        String margs[] = new String[]{"-node", "127.0.0.1", "-node", "127.0.0.2", "-node", "127.0.0.3", "-select",  "-customerid", "47900"};        
        CLoad c = new CLoad();
        new JCommander(c, margs);
        c.connect();
        try
        {
            if(c.doSelect)
            {
                c.selectStatements();
            }
            else
            {
                Calendar cal = Calendar.getInstance();
                cal.setTime(c.statementDate);
                Statement s = new Statement(); 
                s.setCustomerId(c.customerId);
                s.setYear( cal.get(Calendar.YEAR)  );
                s.setDay(cal.get(Calendar.DAY_OF_MONTH));
                s.setMonth(cal.get(Calendar.MONTH) + 1);
                s.setStatementPath(c.statementFile.getAbsolutePath());
                s.setStatementType(c.statementType);
                c.addStatement(s);
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        c.close();
        System.exit(0);
    }
    
    private void verbosef(String fmt, Object ... args)
    {
        pStream.printf(fmt, args); 
    }
    
    public void connect()
    {
        Builder bob = Cluster.builder(); 
        for(HostPort node : nodes)
        {
            bob.addContactPoint(node.getHost());
            Integer p = node.getPort(); 
            if(null != p)
                bob.withPort(p);
        }
        cluster = bob.build();
        Metadata metadata = cluster.getMetadata();
        verbosef("Connected to cluster: %s\n", metadata.getClusterName());
        for (Host host : metadata.getAllHosts())
        {
            verbosef("Datatacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
        }
        session = cluster.connect();
    }
    
    public String addStatement(Statement s)
            throws IOException
    {        
        ByteBuffer buffer;
        try (RandomAccessFile aFile = new RandomAccessFile(s.getStatementPath(), "r"); FileChannel inChannel = aFile.getChannel())
        {
            long fileSize = inChannel.size();
            buffer = ByteBuffer.allocate((int) fileSize);
            inChannel.read(buffer);
            buffer.rewind();
        }    
        Insert i = QueryBuilder.insertInto(keyspace,table);
        i.value("archived_statement_id", s.getArchivedStatementId());
        i.value("customer_id", s.getCustomerId());
        i.value("day", s.getDay());
        i.value("month", s.getMonth());
        i.value("year", s.getYear()); 
        i.value("statement_type", s.getStatementType()); 
        i.value("statement_filename", s.getStatementFilename());
        i.value("statement", buffer);
        i.setConsistencyLevel(clevel); 
        session.execute(i);
        return(s.getArchivedStatementId().toString()); 
    }    
    
    public void close()
    {
        session.shutdown();
        cluster.shutdown();
    }
    
    public void selectStatements()
            throws Exception
    {
        Select q = QueryBuilder.select("archived_statement_id", "customer_id", "day", "month","year","statement_type", "statement_filename").from(keyspace, table);
        if(null != statementUUID)
        {
            q.where(eq("archived_statement_id", statementUUID));
        }
        else
        {
            if( null != customerId)
            {
                q.where(eq("customer_id", customerId));
            }
        }
        
        if(null != clevel)
        {
            q.setConsistencyLevel(clevel);            
        }
        
        ResultSet rs = session.execute(q);
        
        String colNames [] = {"statement id", "date","customer id", "statement type"};
        List<Object[]> list = new ArrayList<>();
        int count = 0;
        for(Row r : rs)
        {
            Object row[] = new Object[4];
            row[0] = r.getUUID("archived_statement_id").toString();
            row[1] = String.format("%02d-%02d-%d", r.getInt("day"),r.getInt("month"), r.getInt("year"));
            row[2] = r.getInt("customer_id"); 
            row[3] = r.getString("statement_type"); 
            list.add(row);
            count++;
        }
        Object data[][] = new Object[list.size()][];
        list.toArray(data); 
        TextTable tt = new TextTable(colNames, data);
        tt.printTable();
        System.out.println();
        System.out.println(count + " records found.");
    }

    public void loadReports()
            throws IOException
    {
        StatementFileVistor crawler = new StatementFileVistor(); 
        crawler.setLoader(this);
        crawler.setRootLen(root_len);
        Files.walkFileTree(Paths.get(root), crawler);
        System.out.println("I added " + crawler.getCount() + " statements");
    }

    private static final String root = "/home/fwelland/statements";
    private static final int root_len = root.length();
}