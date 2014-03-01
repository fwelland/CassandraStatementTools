package com.fhw;

import com.beust.jcommander.*;


public class HostPortConverter
    implements IStringConverter<HostPort>
{
    @Override
    public HostPort convert(String string)
    {
        HostPort hp = new HostPort();
       
        String[] s = string.split(":");
        hp.setHost(s[0]);
        if(s.length == 2)
        {
            hp.setPort(Integer.parseInt(s[1]));
        }
        return(hp);
    }
    
}
