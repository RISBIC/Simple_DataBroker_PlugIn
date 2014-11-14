/*
 * Copyright (c) 2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.simple.dataflownodes;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.arjuna.databroker.data.DataFlow;
import com.arjuna.databroker.data.DataProvider;
import com.arjuna.databroker.data.DataSource;
import com.arjuna.databroker.data.connector.NamedDataProvider;
import com.arjuna.databroker.data.jee.annotation.DataProviderInjection;
import com.arjuna.databroker.data.jee.annotation.PreActivated;
import com.arjuna.databroker.data.jee.annotation.PreDeactivated;

public class SimpleDataSource implements DataSource
{
    private static final Logger logger = Logger.getLogger(SimpleDataSource.class.getName());

    public SimpleDataSource(String name, Map<String, String> properties)
    {
        logger.log(Level.FINE, "SimpleDataSource: " + name + ", " + properties);

        _name          = name;
        _properties    = properties;
    }
    
    @Override
    public String getName()
    {
        return _name;
    }

    @Override
    public void setName(String name)
    {
        _name = name;
    }

    @Override
    public Map<String, String> getProperties()
    {
        return Collections.unmodifiableMap(_properties);
    }

    @Override
    public void setProperties(Map<String, String> properties)
    {
        _properties = properties;
    }
    
    @Override
    public DataFlow getDataFlow()
    {
        return _dataFlow;
    }

    @Override
    public void setDataFlow(DataFlow dataFlow)
    {
        _dataFlow = dataFlow;
    }

    public void dummyGetData(String data)
    {
        logger.log(Level.FINE, "SimpleDataSource.dummyGetData: " + data);

        _dataProvider.produce(data);
    }

    @Override
    public Collection<Class<?>> getDataProviderDataClasses()
    {
        Set<Class<?>> dataProviderDataClasses = new HashSet<Class<?>>();

        dataProviderDataClasses.add(String.class);
        
        return dataProviderDataClasses;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> DataProvider<T> getDataProvider(Class<T> dataClass)
    {
        if (dataClass == String.class)
            return (DataProvider<T>) _dataProvider;
        else
            return null;
    }

    @PreActivated
    public void start()
    {
        _finish = false;
        _waiter = new Waiter();
        _waiter.start();
    }

    @PreDeactivated
    public void finish()
    {
    	_waiter.finish();
    }

    private class Waiter extends Thread
    {
        @Override
        public void run()
        {
            try
            {
                while (! _finish)
                {
                    _dataProvider.produce("Data: " + (new Date()).toString());
                    if (_finish)
                        Thread.sleep(2000);
                }
            }
            catch (InterruptedException interruptedException)
            {
            }
            catch (Throwable throwable)
            {
                logger.log(Level.WARNING, "Problem while watching file", throwable);
            }
        }

        public void finish()
        {
            try
            {
                _finish = true;
                this.interrupt();
                this.join();
            }
            catch (Throwable throwable)
            {
                logger.log(Level.WARNING, "Problem during file watcher shutdown", throwable);
            }
        }
    }

    private Waiter  _waiter;
    private boolean _finish;

    private String               _name;
    private Map<String, String>  _properties;
    private DataFlow             _dataFlow;
    @DataProviderInjection
    private NamedDataProvider<String> _dataProvider;
}
