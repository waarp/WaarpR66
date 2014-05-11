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
package org.waarp.openr66.protocol.http.rest.client;

import java.net.ConnectException;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.UnsupportedCharsetException;

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
import org.waarp.gateway.kernel.rest.DataModelRestMethodHandler.COMMAND_TYPE;
import org.waarp.gateway.kernel.rest.client.RestFuture;
import org.waarp.openr66.protocol.http.rest.handler.HttpRestAbstractR66Handler.ACTIONS_TYPE;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Rest client response handler.
 * 
 * Note: by default, no connection are closed except in case of error or if in HTTP 1.0 or explicitly to be closed.
 * @author Frederic Bregier
 */
public abstract class HttpRestR66ClientResponseHandler extends SimpleChannelUpstreamHandler {
	/**
     * Internal Logger
     */
    private static final WaarpInternalLogger logger = WaarpInternalLoggerFactory
            .getLogger(HttpRestR66ClientResponseHandler.class);
    
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
    
    /**
     * Setting the RestArgument to the RestFuture and validating RestFuture.
     * @param channel
     * @throws HttpInvalidAuthenticationException
     */
    protected void actionFromResponse(Channel channel) throws HttpInvalidAuthenticationException {
    	boolean includeValidation = false;
    	RestArgument ra = new RestArgument((ObjectNode) jsonObject);
    	if (jsonObject == null) {
    		logger.debug("Recv: EMPTY");
    	}
    	((RestFuture) channel.getAttachment()).setRestArgument(ra);
    	switch (ra.getMethod()) {
			case CONNECT:
				break;
			case DELETE:
				includeValidation = delete(channel, ra);
				break;
			case GET:
				includeValidation = get(channel, ra);
				break;
			case HEAD:
				break;
			case OPTIONS:
				includeValidation = options(channel, ra);
				break;
			case PATCH:
				break;
			case POST:
				includeValidation = post(channel, ra);
				break;
			case PUT:
				includeValidation = put(channel, ra);
				break;
			case TRACE:
				break;
			default:
				break;
    		
    	}
        if (! includeValidation) {
            // finalize the future
            ((RestFuture) channel.getAttachment()).setSuccess();
        }
    }

    /**
     * Method calls when a action REST command is raised as answer
     * @param channel
     * @param ra
     * @param act
     * @return if validation is done (or suppose to be)
     * @throws HttpInvalidAuthenticationException 
     */
    protected abstract boolean action(Channel channel, RestArgument ra, ACTIONS_TYPE act) throws HttpInvalidAuthenticationException;
    /**
     * Method calls when a REST Get command is raised as answer
     * @param channel
     * @param ra
     * @return if validation is done (or suppose to be)
     * @throws HttpInvalidAuthenticationException 
     */
    protected abstract boolean afterDbGet(Channel channel, RestArgument ra) throws HttpInvalidAuthenticationException;
    /**
     * Method calls when a REST Post command is raised as answer
     * @param channel
     * @param ra
     * @return if validation is done (or suppose to be)
     * @throws HttpInvalidAuthenticationException 
     */
    protected abstract boolean afterDbPost(Channel channel, RestArgument ra) throws HttpInvalidAuthenticationException;
    /**
     * Method calls when a REST Put command is raised as answer
     * @param channel
     * @param ra
     * @return if validation is done (or suppose to be)
     * @throws HttpInvalidAuthenticationException 
     */
    protected abstract boolean afterDbPut(Channel channel, RestArgument ra) throws HttpInvalidAuthenticationException;
    /**
     * Method calls when a REST Delete command is raised as answer
     * @param channel
     * @param ra
     * @return if validation is done (or suppose to be)
     * @throws HttpInvalidAuthenticationException 
     */
    protected abstract boolean afterDbDelete(Channel channel, RestArgument ra) throws HttpInvalidAuthenticationException;
    /**
     * Method calls when a REST GetMultiple command is raised as answer
     * @param channel
     * @param ra
     * @return if validation is done (or suppose to be)
     * @throws HttpInvalidAuthenticationException 
     */
    protected abstract boolean afterDbGetMultiple(Channel channel, RestArgument ra) throws HttpInvalidAuthenticationException;
    /**
     * Method calls when a REST Options command is raised as answer
     * @param channel
     * @param ra
     * @return if validation is done (or suppose to be)
     * @throws HttpInvalidAuthenticationException 
     */
    protected abstract boolean afterDbOptions(Channel channel, RestArgument ra) throws HttpInvalidAuthenticationException;
    /**
     * Method calls when a REST command is in error
     * @param channel
     * @param ra (might be null)
     * @return if validation is done (or suppose to be)
     * @throws HttpInvalidAuthenticationException 
     */
    protected abstract boolean afterError(Channel channel, RestArgument ra) throws HttpInvalidAuthenticationException;
    
    protected boolean get(Channel channel, RestArgument ra) throws HttpInvalidAuthenticationException {
    	if (logger.isDebugEnabled()) {
    		logger.debug(ra.prettyPrint());
    	}
    	if (ra.getCommand() == COMMAND_TYPE.GET) {
			return afterDbGet(channel, ra);
    	} else if (ra.getCommand() == COMMAND_TYPE.MULTIGET) {
			return afterDbGetMultiple(channel, ra);
    	} else {
    		String cmd = ra.getCommandField();
    		try {
    			ACTIONS_TYPE act = ACTIONS_TYPE.valueOf(cmd);
    			return action(channel, ra, act);
    		} catch (Exception e) {
    			return false;
    		}
    	}
    }
    
    protected boolean put(Channel channel, RestArgument ra) throws HttpInvalidAuthenticationException {
    	if (logger.isDebugEnabled()) {
    		logger.debug(ra.prettyPrint());
    	}
    	if (ra.getCommand() == COMMAND_TYPE.UPDATE) {
			return afterDbPut(channel, ra);
		} else {
			String cmd = ra.getCommandField();
			try {
				ACTIONS_TYPE act = ACTIONS_TYPE.valueOf(cmd);
				return action(channel, ra, act);
			} catch (Exception e) {
				return false;
			}
		}
    }

    protected boolean post(Channel channel, RestArgument ra) throws HttpInvalidAuthenticationException {
    	if (logger.isDebugEnabled()) {
    		logger.debug(ra.prettyPrint());
    	}
    	if (ra.getCommand() == COMMAND_TYPE.CREATE) {
			return afterDbPost(channel, ra);
		} else {
			String cmd = ra.getCommandField();
			try {
				ACTIONS_TYPE act = ACTIONS_TYPE.valueOf(cmd);
				return action(channel, ra, act);
			} catch (Exception e) {
				return false;
			}
		}
    }
    
    protected boolean delete(Channel channel, RestArgument ra) throws HttpInvalidAuthenticationException {
    	if (logger.isDebugEnabled()) {
    		logger.debug(ra.prettyPrint());
    	}
    	if (ra.getCommand() == COMMAND_TYPE.DELETE) {
    		return afterDbDelete(channel, ra);
		} else {
			String cmd = ra.getCommandField();
			try {
				ACTIONS_TYPE act = ACTIONS_TYPE.valueOf(cmd);
				return action(channel, ra, act);
			} catch (Exception e) {
				return false;
			}
		}
    }
    
    protected boolean options(Channel channel, RestArgument ra) throws HttpInvalidAuthenticationException {
    	if (logger.isDebugEnabled()) {
    		logger.debug(ra.prettyPrint());
    	}
    	if (ra.getCommand() == COMMAND_TYPE.OPTIONS) {
    		return afterDbOptions(channel, ra);
		} else {
			String cmd = ra.getCommandField();
			try {
				ACTIONS_TYPE act = ACTIONS_TYPE.valueOf(cmd);
				return action(channel, ra, act);
			} catch (Exception e) {
				return false;
			}
		}
    }
    
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
            throws Exception {
        Object obj = e.getMessage();
        if (!readingChunks && (obj instanceof HttpResponse)) {
            HttpResponse response = (HttpResponse) e.getMessage();
            HttpResponseStatus status = response.getStatus();
            logger.debug(HttpHeaders.Names.REFERER+": "+response.headers().get(HttpHeaders.Names.REFERER) + " STATUS: " + status);
            if (response.getStatus().getCode() == 200 && response.isChunked()) {
                readingChunks = true;
            } else if (response.getStatus().getCode() != 200) {
                addContent(response);
                RestArgument ra = null;
                if (jsonObject != null) {
                    ra = new RestArgument((ObjectNode) jsonObject);
                    ((RestFuture) e.getChannel().getAttachment()).setRestArgument(ra);
                    logger.error(ra.prettyPrint());
            	} else {
                    logger.error("Error: "+response.getStatus().getCode()+" "+response.getStatus().getReasonPhrase());
            	}
                if (! afterError(e.getChannel(), ra)) {
                	((RestFuture) e.getChannel().getAttachment()).cancel();
                }
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
        logger.warn("Error", e.getCause());
        ((RestFuture) e.getChannel().getAttachment()).setFailure(e.getCause());
        logger.debug("Will close");
        WaarpSslUtility.closingSslChannel(e.getChannel());
    }

}
