/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.net;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.predictor.Predictor;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.utils.MonotonicClock.approxTime;

class ResponseVerbHandler implements IVerbHandler
{
    public static final ResponseVerbHandler instance = new ResponseVerbHandler();

    private static final Logger logger = LoggerFactory.getLogger(ResponseVerbHandler.class);

  /*  @Override
    public void doVerb(Message message)
    {
        RequestCallbacks.CallbackInfo callbackInfo = MessagingService.instance().callbacks.remove(message.id(), message.from());
        if (callbackInfo == null)
        {
            String msg = "Callback already removed for {} (from {})";
            logger.trace(msg, message.id(), message.from());
            Tracing.trace(msg, message.id(), message.from());
            return;
        }

        long latencyNanos = approxTime.now() - callbackInfo.createdAtNanos;
        Tracing.trace("Processing response from {}", message.from());

        RequestCallback cb = callbackInfo.callback;
        if (message.isFailureResponse())
        {
            cb.onFailure(message.from(), (RequestFailureReason) message.payload);
        }
        else
        {
            MessagingService.instance().latencySubscribers.maybeAdd(cb, message.from(), latencyNanos, NANOSECONDS);
            cb.onResponse(message);
        }

        if (callbackInfo.callback.supportsBackPressure())
        {
            MessagingService.instance().updateBackPressureOnReceive(message.from(), cb, false);
        }
    }*/
    
    
    
    @Override
    public void doVerb(Message message)
    {
    //	logger.info("inside do verb");
    	long start = System.nanoTime();
        RequestCallbacks.CallbackInfo callbackInfo = MessagingService.instance().callbacks.remove(message.id(), message.from());
     // AtomicInteger counter = predictor.getPendingRequestCounter(FBUtilities.getBroadcastAddressAndPort());
     
       
      
      if (callbackInfo == null)
        {
            String msg = "Callback already removed for {} (from {})";
            logger.trace(msg, message.id(), message.from());
            Tracing.trace(msg, message.id(), message.from());
            return;
        }

        long latencyNanos = approxTime.now() - callbackInfo.createdAtNanos;
        Tracing.trace("Processing response from {}", message.from());

        RequestCallback cb = callbackInfo.callback;
        if (message.isFailureResponse())
        {
        	logger.info("failed inside response verb handler");
            cb.onFailure(message.from(), (RequestFailureReason) message.payload);
        }
        else
        {
        	
        	//logger.info("Successed inside response verb handler");
            MessagingService.instance().latencySubscribers.maybeAdd(cb, message.from(), latencyNanos, NANOSECONDS);
            cb.onResponse(message);
            
                   
         //
            if(message.verb().equals(Verb.READ_RSP))
    		{
		   long serviceTimeNanos = System.nanoTime()-start;
		 //   int queueSize = counter.decrementAndGet();
		//	logger.info("decrementing pending job inside doverb()");
		   // int queueSize = counter.get();
		   Predictor.updateMetricesRemote(FBUtilities.getBroadcastAddressAndPort(), latencyNanos, message.verb().toString());
		    //System.out.println("hi please run man. I am crying. :'( ");
		//	predictor.updateMetrices(FBUtilities.getBroadcastAddressAndPort(),queueSize, latencyNanos, serviceTimeNanos, "read_rsp");
    		}
            long serviceTimeNanos = System.nanoTime()-start;
          //  Predictor.updateMetricesRemote(FBUtilities.getBroadcastAddressAndPort(), latencyNanos, message.verb().toString());
            Predictor.updateMetrices2(FBUtilities.getBroadcastAddressAndPort(), latencyNanos, serviceTimeNanos,message.verb().toString());
	                      
        if (callbackInfo.callback.supportsBackPressure())
        {
            MessagingService.instance().updateBackPressureOnReceive(message.from(), cb, false);
        }
    }
    }
}
