/*******************************************************************************
 * Copyright 2013 Johns Hopkins University
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package edu.jhu.pha.vospace;

import java.io.IOException;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

public class QueueConnector {
    static Configuration conf = SettingsServlet.getConfig();
	private static final Logger logger = Logger.getLogger(QueueConnector.class);
	//RabbitMQ
	private static ConnectionFactory factory;

	static {
		factory = new ConnectionFactory();
		factory.setHost(conf.getString("rabbitmq.host"));
		factory.setVirtualHost(conf.getString("rabbitmq.vhost"));
	}
	
    /** Helper class for goAMQP() */
    public static abstract class AMQPWorker<T> {
        abstract public T go(Connection conn, Channel channel) throws IOException;
        public void error(String context, Exception e) { logger.error(context, e); }
    }

    /** Helper function to setup and teardown AMQP connection & statement. */
    public static <T> T goAMQP(String context, AMQPWorker<T> goer) {
    	logger.debug("AMPQ "+context);
        Connection conn = null;
        Channel channel = null;
        try {
            conn = factory.newConnection();
            channel = conn.createChannel();
            T result = goer.go(conn, channel);
            conn.addShutdownListener(new ShutdownListener() {
                public void shutdownCompleted(ShutdownSignalException cause) {
                	//logger.info("RabbitMQ connection shutdown");
                }
            });            return result;
        } catch (IOException e) {
            goer.error(context, e);
            return null;
        } finally {
            close(channel);
            close(conn);
        }
    }
    public static void close(Connection c) { if (c != null) { try { c.close(); } catch(Exception ignored) {} } }
    public static void close(Channel c) { if (c != null) { try { c.close(); } catch(Exception ignored) {} } }
}
