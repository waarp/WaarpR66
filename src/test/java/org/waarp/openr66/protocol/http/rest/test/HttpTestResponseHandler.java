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
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
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
import org.waarp.gateway.kernel.rest.client.RestFuture;
import org.waarp.openr66.protocol.http.rest.HttpRestR66Handler.RESTHANDLERS;
import org.waarp.openr66.protocol.http.rest.handler.HttpRestAbstractR66Handler.ACTIONS_TYPE;
import org.waarp.openr66.protocol.localhandler.packet.json.InformationJsonPacket;
import org.waarp.openr66.protocol.localhandler.packet.json.JsonPacket;
import org.waarp.openr66.protocol.localhandler.packet.json.TransferRequestJsonPacket;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Test Rest client response handler
 * @author Frederic Bregier
 */
public class HttpTestResponseHandler extends SimpleChannelUpstreamHandler {
	/**
     * Internal Logger
     */
    private static final WaarpInternalLogger logger = WaarpInternalLoggerFactory
            .getLogger(HttpTestResponseHandler.class);
    
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
    
    protected void actionFromResponse(Channel channel) throws HttpInvalidAuthenticationException {
    	HttpTestRestR66Client.count.incrementAndGet();
    	boolean newMessage = false;
    	RestArgument ra = new RestArgument((ObjectNode) jsonObject);
    	if (HttpTestRestR66Client.DEBUG && jsonObject == null) {
    		logger.warn("Recv: EMPTY");
    	}
    	// set the result as the "last" Json received
    	((RestFuture) channel.getAttachment()).setRestArgument(ra);
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
            // finalize the future only in Final success or Final error: when closing channel
            ((RestFuture) channel.getAttachment()).setSuccess();
            WaarpSslUtility.closingSslChannel(channel);
        }
    }
    
    protected boolean action(Channel channel, RestArgument ra, ACTIONS_TYPE act) {
    	boolean newMessage = false;
    	switch (act) {
			case CreateTransfer: {
				// Continue with GetTransferInformation
				TransferRequestJsonPacket recv;
				try {
					recv = (TransferRequestJsonPacket) JsonPacket.createFromBuffer(ra.getAnswer().toString());
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return newMessage;
				}
				InformationJsonPacket node = new InformationJsonPacket();
				node.setRequestUserPacket();
				node.setId(recv.getSpecialId());
				node.setRulename(recv.getRequested());
				node.setIdRequest(true);
				HttpTestRestR66Client.action(channel, HttpMethod.GET, RESTHANDLERS.Transfer.uri, node);
				newMessage = true;
				break;
			}
			case ExecuteBusiness:
				// End
				break;
			case ExportConfig:
				// no Import in automatic test
				break;
			case GetBandwidth:
				// End
				break;
			case GetInformation:
				// End
				break;
			case GetLog:
				// End
				break;
			case GetTransferInformation:
				// Continue with Stop in StopOrCancelTransfer
				break;
			case ImportConfig:
				// End
				break;
			case OPTIONS:
				break;
			case RestartTransfer:
				// End
				break;
			case SetBandwidth:
				// Continue with GetBandwidth
				break;
			case ShutdownOrBlock:
				// End
				break;
			case StopOrCancelTransfer:
				// Continue with RestartTransfer
				break;
			default:
				break;
    		
    	}
    	return newMessage;
    }
    
    protected boolean get(Channel channel, RestArgument ra) throws HttpInvalidAuthenticationException {
    	boolean newMessage = false;
    	if (HttpTestRestR66Client.DEBUG) {
    		logger.warn(ra.prettyPrint());
    	}
    	if (ra.getCommand() == COMMAND_TYPE.GET) {
	    	// Update 1
			HttpTestRestR66Client.updateData(channel, ra);
	    	newMessage = true;
    	} else {
    		String cmd = ra.getCommandField();
    		try {
    			ACTIONS_TYPE act = ACTIONS_TYPE.valueOf(cmd);
    			return action(channel, ra, act);
    		} catch (Exception e) {
    			return newMessage;
    		}
    	}
    	return newMessage;
    }
    
    protected boolean put(Channel channel, RestArgument ra) throws HttpInvalidAuthenticationException {
    	boolean newMessage = false;
    	if (HttpTestRestR66Client.DEBUG) {
    		logger.warn(ra.prettyPrint());
    	}
    	if (ra.getCommand() == COMMAND_TYPE.UPDATE) {
	    	// Delete 1
			HttpTestRestR66Client.deleteData(channel, ra);
	    	newMessage = true;
		} else {
			String cmd = ra.getCommandField();
			try {
				ACTIONS_TYPE act = ACTIONS_TYPE.valueOf(cmd);
				return action(channel, ra, act);
			} catch (Exception e) {
				return newMessage;
			}
		}
    	return newMessage;
    }

    protected boolean post(Channel channel, RestArgument ra) throws HttpInvalidAuthenticationException {
    	boolean newMessage = false;
    	if (HttpTestRestR66Client.DEBUG) {
    		logger.warn(ra.prettyPrint());
    	}
    	if (ra.getCommand() == COMMAND_TYPE.CREATE) {
	    	// Select 1
			HttpTestRestR66Client.readData(channel, ra);
	    	newMessage = true;
		} else {
			String cmd = ra.getCommandField();
			try {
				ACTIONS_TYPE act = ACTIONS_TYPE.valueOf(cmd);
				return action(channel, ra, act);
			} catch (Exception e) {
				return newMessage;
			}
		}
    	return newMessage;
    }
    
    protected boolean delete(Channel channel, RestArgument ra) {
    	boolean newMessage = false;
    	if (HttpTestRestR66Client.DEBUG) {
    		logger.warn(ra.prettyPrint());
    	}
    	if (ra.getCommand() == COMMAND_TYPE.DELETE) {
		} else {
			String cmd = ra.getCommandField();
			try {
				ACTIONS_TYPE act = ACTIONS_TYPE.valueOf(cmd);
				return action(channel, ra, act);
			} catch (Exception e) {
				return newMessage;
			}
		}
    	return newMessage;
    }
    
    protected boolean options(Channel channel, RestArgument ra) throws HttpInvalidAuthenticationException {
    	boolean newMessage = false;
    	AtomicInteger counter = null;
    	RestFuture future = (RestFuture) channel.getAttachment();
    	if (future.getOtherObject() == null) {
    		counter = new AtomicInteger();
    		future.setOtherObject(counter);
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
						counter.incrementAndGet();
						HttpTestRestR66Client.options(channel, name);
				    	newMessage = true;
					}
				}
	    	}
    	}
    	if (! newMessage) {
    		counter = (AtomicInteger) future.getOtherObject();
    		newMessage = counter.decrementAndGet() > 0;
    		if (! newMessage) {
    			future.setOtherObject(null);
    		}
    	}
    	if (HttpTestRestR66Client.DEBUG) {
    		logger.warn(ra.prettyPrint());
    	} else {
    		logger.debug("Options received: "+ra.getBaseUri()+":"+counter);
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
                addContent(response);
                if (HttpTestRestR66Client.DEBUG && jsonObject != null) {
                    RestArgument ra = new RestArgument((ObjectNode) jsonObject);
                    ((RestFuture) e.getChannel().getAttachment()).setRestArgument(ra);
                    logger.warn(ra.prettyPrint());
            	} else {
                    logger.error("Error: "+response.getStatus().getCode());
            	}
                HttpTestRestR66Client.count.incrementAndGet();
                ((RestFuture) e.getChannel().getAttachment()).cancel();
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
        	((RestFuture) e.getChannel().getAttachment()).setFailure(e.getCause());
            return;
        } else if (e.getCause() instanceof ConnectException) {
            if (ctx.getChannel().isConnected()) {
            	logger.debug("Will close");
            	((RestFuture) e.getChannel().getAttachment()).setFailure(e.getCause());
            	WaarpSslUtility.closingSslChannel(e.getChannel());
            }
            return;
        }
        e.getCause().printStackTrace();
        ((RestFuture) e.getChannel().getAttachment()).setFailure(e.getCause());
        logger.debug("Will close");
        WaarpSslUtility.closingSslChannel(e.getChannel());
    }

}
