package com.fhw;

import com.datastax.driver.core.*;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class ConsistencyLevelConverterTest
{
    
    public ConsistencyLevelConverterTest()
    {
    }
    
    @BeforeClass
    public static void setUpClass()
    {
    }
    
    @AfterClass
    public static void tearDownClass()
    {
    }
    
    @Before
    public void setUp()
    {
    }
    
    @After
    public void tearDown()
    {
    }

    @Test
    public void testConvertLOCAL_QUORUM()
    {
        System.out.println("convert");
        String string = "LOCAL_QUORUM";
        ConsistencyLevelConverter instance = new ConsistencyLevelConverter();
        ConsistencyLevel result = instance.convert(string);
        assertEquals(ConsistencyLevel.LOCAL_QUORUM, result);
    }
    
}
