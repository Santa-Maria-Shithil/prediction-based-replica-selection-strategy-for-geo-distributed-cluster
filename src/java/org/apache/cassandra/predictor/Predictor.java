package org.apache.cassandra.predictor;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Predictor {
	 private static final double ALPHA = 0.9;
	private static final ConcurrentHashMap<InetAddressAndPort,AtomicInteger> queuesize =new ConcurrentHashMap<>();
	private static final HashMap<InetAddressAndPort,Long> servicetime = new HashMap<>();
	private static final HashMap<InetAddressAndPort,Long> latency=new HashMap<>();
	public static final ConcurrentHashMap<InetAddressAndPort,Double> queuesizeEMA =new ConcurrentHashMap<>();
	public static final HashMap<InetAddressAndPort,Double> servicetimeEMA = new HashMap<>();
	public static final HashMap<InetAddressAndPort,Double> latencyEMA=new HashMap<>();
	private static final ConcurrentHashMap<InetAddressAndPort,AtomicInteger> queuesize2 =new ConcurrentHashMap<>();
	private static final Logger logger=LoggerFactory.getLogger(Predictor.class);
	public static boolean containsKey(InetAddressAndPort key)
	{
		if(queuesize.containsKey(key))
		{
			return true;
		}
		else 
			return false;
	}
	 public static AtomicInteger putqueue(InetAddressAndPort key, AtomicInteger value)
	 {
		 return queuesize.put(key, value);
	 }
	 public static AtomicInteger PUTqueue2(InetAddressAndPort key,AtomicInteger value)
	 {
		 return queuesize2.put(key, value);
	 }
	 public static AtomicInteger getqueue(InetAddressAndPort key)
	 {
		 return queuesize.get(key);
	 }
	 public static Long putservicetime(InetAddressAndPort key, long value)
	 {
		 return servicetime.put(key, value);
	 }
	 public static Long getservicetime(InetAddressAndPort key)
	 {
		 return servicetime.get(key);
	 }
	public static AtomicInteger getPendingRequestCounter(InetAddressAndPort key)
	{
		AtomicInteger counter = queuesize.get(key);
		if(counter==null)
		{
			queuesize.put(key,new AtomicInteger(0));
			counter=queuesize.get(key);
		}
		return counter;
	}
	public static void updateMetrices(InetAddressAndPort key,int qsize, long l, long stime, String tag)
	{
		double lema,sema,qema;
		//latency.put(key, l);
		//servicetime.put(key, stime);
		//int qsize=get(key).decrementAndGet();
	   //	logger.info("decrementing pending job inside predictor");
		//String data = key.toString() + " " + Integer.toString(qsize) + " " +l + " " + stime+" "+"UPDATELOCAL"+"\n";
		String data = key.toString() + " " +l + " " + stime + " " + Integer.toString(qsize) +" "+ getSeverity(key)+ " " + "UPDATELOCAL"+"\n";
		logger.info(data);
		l=l-stime;
		if(latencyEMA.containsKey(key))
		{
			lema=(long) (ALPHA * (l/1000000) + (1 - ALPHA) * latencyEMA.get(key));
			latencyEMA.put(key, lema);
		}
		else
		{
			lema=0;
			latencyEMA.put(key, lema);
		}
		
		if(queuesizeEMA.containsKey(key))
		{
			qema=(long) (ALPHA * qsize + (1 - ALPHA) * queuesizeEMA.get(key));
			queuesizeEMA.put(key, qema);
		}
		else
		{
			qema=0;
			queuesizeEMA.put(key, qema);
		}
		if(servicetimeEMA.containsKey(key))
		{
			sema=(long) (ALPHA * (stime/1000000) + (1 - ALPHA) * servicetimeEMA.get(key));
			servicetimeEMA.put(key, sema);
		}
		else
		{
			sema=0;
			servicetimeEMA.put(key,sema);
		}
		
		DynamicEndpointSnitch.updateScores(lema,qema,sema,key);
		
		/*File file =new File("data.txt");
		FileWriter fr = null;
		try
		{
			fr = new FileWriter(file,true);
			fr.write(data);
		}catch (IOException e)
		{
			e.printStackTrace();
		}finally {
			try
			{
				fr.close();
			}catch (IOException e)
			{
				e.printStackTrace();
			}
		}*/
	} 
	public static void updateMetricesRemote(InetAddressAndPort key, long l,String tag)
	{
		double lema,sema,qema;
		//latency.put(key, l);
		long stime=servicetime.get(key);
		l=l-stime;
		if(queuesize.get(key).get()>0)
		queuesize.get(key).decrementAndGet();
		int qsize=queuesize2.get(key).get();
	   //	logger.info("decrementing pending job inside predictor");
		//String data = key.toString() + " " + Integer.toString(qsize) + " " +l + " " + stime+" "+"UPDATEREMOTE"+"\n";
		String data = key.toString() + " " +l + " " + stime + " " + Integer.toString(qsize) +" "+getSeverity(key)+" "+"UPDATEREMOTE"+"\n";
		logger.info(data);
		l=l-stime;
		if(latencyEMA.containsKey(key))
		{
			lema=(long) (ALPHA * (l/1000000) + (1 - ALPHA) * latencyEMA.get(key));
			latencyEMA.put(key, lema);
		}
		else
		{
			lema=0;
			latencyEMA.put(key, lema);
		}
		
		if(queuesizeEMA.containsKey(key))
		{
			qema=(long) (ALPHA * qsize + (1 - ALPHA) * queuesizeEMA.get(key));
			queuesizeEMA.put(key, qema);
		}
		else
		{
			qema=0;
			queuesizeEMA.put(key, qema);
		}
		if(servicetimeEMA.containsKey(key))
		{
			sema=(long) (ALPHA * (stime/1000000) + (1 - ALPHA) * servicetimeEMA.get(key));
			servicetimeEMA.put(key, sema);
		}
		else
		{
			sema=0;
			servicetimeEMA.put(key,sema);
		}
		
		DynamicEndpointSnitch.updateScores(lema,qema,sema,key);
		
		
		
		
		/*File file =new File("data.txt");
		FileWriter fr = null;
		try
		{
			fr = new FileWriter(file,true);
			fr.write(data);
		}catch (IOException e)
		{
			e.printStackTrace();
		}finally {
			try
			{
				fr.close();
			}catch (IOException e)
			{
				e.printStackTrace();
			}
		}*/
	}
	
public static void updateMetrices2(InetAddressAndPort key, long l, long stime, String tag)
	{
		
		//latency.put(key, l);
		//servicetime.put(key, stime);
		//int qsize=get(key).decrementAndGet();
	   //	logger.info("decrementing pending job inside predictor");
	/*	String data = key.toString() + " "+l + " " + stime+" "+tag+"\n";
		File file =new File("datatwo.txt");
		FileWriter fr = null;
		try
		{
			fr = new FileWriter(file);
			fr.write(data);
		}catch (IOException e)
		{
			e.printStackTrace();
		}finally {
			try
			{
				fr.close();
			}catch (IOException e)
			{
				e.printStackTrace();
			}
		}*/
	} 
}