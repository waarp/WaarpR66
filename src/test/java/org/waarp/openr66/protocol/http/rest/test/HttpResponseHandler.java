/*
 * Copyright 2009 Red Hat, Inc.
 * 
 * Red Hat licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.waarp.openr66.protocol.http.rest.test;

import java.net.ConnectException;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Iterator;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.waarp.common.crypto.ssl.WaarpSslUtility;
import org.waarp.common.json.JsonHandler;
import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.common.utility.WaarpStringUtils;
import org.waarp.gateway.kernel.exception.HttpIncorrectRequestException;
import org.waarp.gateway.kernel.exception.HttpInvalidAuthenticationException;
import org.waarp.gateway.kernel.rest.RestArgument;
import org.waarp.gateway.kernel.rest.RootOptionsRestMethodHandler;
import org.waarp.gateway.kernel.rest.DataModelRestMethodHandler.COMMAND_TYPE;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * 
 * @author Frederic Bregier
 */
public class HttpResponseHandler extends SimpleChannelUpstreamHandler {
	/**
     * Internal Logger
     */
    private static final WaarpInternalLogger logger = WaarpInternalLoggerFactory
            .getLogger(HttpResponseHandler.class);
    
    private volatile boolean readingChunks;
    private ChannelBuffer cumulativeBody = null;
    protected JsonNode jsonObject = null;
    
    protected void addContent(HttpResponse response) throws HttpIncorrectRequestException {
    	ChannelBuffer content = response.getContent();
        if (content != null && content.readable()) {
            if (cumulativeBody != null) {
				cumulativeBody = ChannelBuffers.wrappedBuffer(cumulativeBody, content);
			} else {
				cumulativeBody = content;
			}
            // get the Json equivalent of the Body
    		try {
    			String json = cumulativeBody.toString(WaarpStringUtils.UTF8);
    			jsonObject = JsonHandler.getFromString(json);
    		} catch (UnsupportedCharsetException e2) {
    			logger.warn("Error", e2);
    			throw new HttpIncorrectRequestException(e2);
    		}
			cumulativeBody = null;
        }
    }
    
    protected void actionFromResponse(Channel channel) {
    	HttpRestR66TestClient.count.incrementAndGet();
    	boolean newMessage = false;
    	RestArgument ra = new RestArgument((ObjectNode) jsonObject);
    	if (HttpRestR66TestClient.DEBUG && jsonObject == null) {
    		logger.warn("Recv: EMPTY");
    	}
    	switch (ra.getMethod()) {
			case CONNECT:
				break;
			case DELETE:
				newMessage = delete(channel, ra);
				break;
			case GET:
				newMessage = get(channel, ra);
				break;
			case HEAD:
				break;
			case OPTIONS:
				newMessage = options(channel, ra);
				break;
			case PATCH:
				break;
			case POST:
				newMessage = post(channel, ra);
				break;
			case PUT:
				newMessage = put(channel, ra);
				break;
			case TRACE:
				break;
			default:
				break;
    		
    	}
        if (! newMessage && channel.isConnected()) {
            logger.debug("Will close");
        	WaarpSslUtility.closingSslChannel(channel);
        }
    }
    
    protected boolean get(Channel channel, RestArgument ra) {
    	boolean newMessage = false;
    	if (HttpRestR66TestClient.DEBUG) {
    		logger.warn(ra.prettyPrint());
    	}
    	if (ra.getCommand() == COMMAND_TYPE.GET) {
	    	// Update 1
	    	try {
				HttpRestR66TestClient.updateData(channel, ra);
		    	newMessage = true;
			} catch (HttpInvalidAuthenticationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	return newMessage;
    }
    
    protected boolean put(Channel channel, RestArgument ra) {
    	boolean newMessage = false;
    	if (HttpRestR66TestClient.DEBUG) {
    		logger.warn(ra.prettyPrint());
    	}
    	// Delete 1
    	try {
			HttpRestR66TestClient.deleteData(channel, ra);
	    	newMessage = true;
		} catch (HttpInvalidAuthenticationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return newMessage;
    }

    protected boolean post(Channel channel, RestArgument ra) {
    	boolean newMessage = false;
    	if (HttpRestR66TestClient.DEBUG) {
    		logger.warn(ra.prettyPrint());
    	}
    	// Select 1
    	try {
			HttpRestR66TestClient.readData(channel, ra);
	    	newMessage = true;
		} catch (HttpInvalidAuthenticationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return newMessage;
    }
    
    protected boolean delete(Channel channel, RestArgument ra) {
    	boolean newMessage = false;
    	if (HttpRestR66TestClient.DEBUG) {
    		logger.warn(ra.prettyPrint());
    	}
    	return newMessage;
    }
    
    protected boolean options(Channel channel, RestArgument ra) {
    	boolean newMessage = false;
    	JsonNode node = ra.getDetailedAllowOption();
    	if (! node.isMissingNode()) {
	    	for (JsonNode jsonNode : node) {
				Iterator<String> iterator = jsonNode.fieldNames();
				while (iterator.hasNext()) {
					String name = iterator.next();
					if (! jsonNode.path(name).path(RestArgument.REST_FIELD.JSON_PATH.field).isMissingNode()) {
						break;
					}
					if (name.equals(RootOptionsRestMethodHandler.ROOT)) {
						continue;
					}
					try {
						HttpRestR66TestClient.options(channel, name);
				    	newMessage = true;
					} catch (HttpInvalidAuthenticationException e) {
						logger.error("Not authenticated", e);
					}
				}
			}
    	}
    	if (HttpRestR66TestClient.DEBUG) {
    		logger.warn(ra.prettyPrint());
    	}
    	return newMessage;
    }
    
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
            throws Exception {
        Object obj = e.getMessage();
        if (!readingChunks && (obj instanceof HttpResponse)) {
            HttpResponse response = (HttpResponse) e.getMessage();
            HttpResponseStatus status = response.getStatus();
            logger.debug(HttpHeaders.Names.REFERER+": "+response.headers().get(HttpHeaders.Names.REFERER) +
                    " STATUS: " + status);

            if (response.getStatus().getCode() == 200 && response.isChunked()) {
                readingChunks = true;
            } else if (response.getStatus().getCode() != 200) {
                logger.error(" Error: "+response.getStatus().getCode());
                addContent(response);
                if (HttpRestR66TestClient.DEBUG && jsonObject != null) {
                    RestArgument ra = new RestArgument((ObjectNode) jsonObject);
                    logger.warn(ra.prettyPrint());
            	}
                HttpRestR66TestClient.count.incrementAndGet();
            	if (e.getChannel().isConnected()) {
                    logger.debug("Will close");
                	WaarpSslUtility.closingSslChannel(e.getChannel());
                }
            } else {
                addContent(response);
                actionFromResponse(e.getChannel());
            }
        } else {
            readingChunks = true;
            HttpChunk chunk = (HttpChunk) e.getMessage();
            if (chunk.isLast()) {
                readingChunks = false;
                ChannelBuffer content = chunk.getContent();
                if (content != null && content.readable()) {
                    if (cumulativeBody != null) {
        				cumulativeBody = ChannelBuffers.wrappedBuffer(cumulativeBody, content);
        			} else {
        				cumulativeBody = content;
        			}
                }
                // get the Json equivalent of the Body
                if (cumulativeBody == null) {
                	jsonObject = JsonHandler.createObjectNode();
                } else {
	        		try {
	        			String json = cumulativeBody.toString(WaarpStringUtils.UTF8);
	        			jsonObject = JsonHandler.getFromString(json);
	        		} catch (Throwable e2) {
	        			logger.warn("Error", e2);
	        			throw new HttpIncorrectRequestException(e2);
	        		}
	    			cumulativeBody = null;
                }                
                actionFromResponse(e.getChannel());
            } else {
            	ChannelBuffer content = chunk.getContent();
                if (content != null && content.readable()) {
                    if (cumulativeBody != null) {
        				cumulativeBody = ChannelBuffers.wrappedBuffer(cumulativeBody, content);
        			} else {
        				cumulativeBody = content;
        			}
                }
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
            throws Exception {
        if (e.getCause() instanceof ClosedChannelException) {
        	logger.debug("Close before ending");
            return;
        } else if (e.getCause() instanceof ConnectException) {
            System.err.println("Connection refused");
            if (ctx.getChannel().isConnected()) {
            	logger.debug("Will close");
            	WaarpSslUtility.closingSslChannel(e.getChannel());
            }
            return;
        }
        e.getCause().printStackTrace();
        logger.debug("Will close");
        WaarpSslUtility.closingSslChannel(e.getChannel());
    }

}
