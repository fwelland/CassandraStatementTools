package com.fhw;

import com.beust.jcommander.*;
import com.datastax.driver.core.*;


public class ConsistencyLevelConverter
    implements IStringConverter<ConsistencyLevel>
{
    @Override
    public ConsistencyLevel convert(String string)
    {
        return ( ConsistencyLevel.valueOf(string));
    }    
}
