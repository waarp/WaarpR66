/**
 * This file is part of Waarp Project.
 * 
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author tags. See the
 * COPYRIGHT.txt in the distribution for a full listing of individual contributors.
 * 
 * All Waarp Project is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 * 
 * Waarp is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with Waarp . If not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.waarp.openr66.protocol.http;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.Cookie;
import org.jboss.netty.handler.codec.http.CookieDecoder;
import org.jboss.netty.handler.codec.http.CookieEncoder;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.stream.ChunkedNioFile;

/**
 * 
 * Utility class to write external file with cache enable properties
 * 
 * @author Frederic Bregier
 * 
 */
public class HttpWriteCacheEnable {

	/**
	 * Write a file, taking into account cache enabled and removing session cookie
	 * 
	 * @param request
	 * @param channel
	 * @param filename
	 * @param cookieNameToRemove
	 */
	public static void writeFile(HttpRequest request, Channel channel, String filename,
			String cookieNameToRemove) {
		// Convert the response content to a ChannelBuffer.
        HttpResponse response;
        if (request.containsHeader(HttpHeaders.Names.IF_MODIFIED_SINCE)) {
            response = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
                    HttpResponseStatus.NOT_MODIFIED);
            handleCookies(request, response, cookieNameToRemove);
            channel.write(response);
            return;
        }
        File file = new File(filename);
        if (!file.isFile() || !file.canRead()) {
            response = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
                    HttpResponseStatus.NOT_FOUND);
            handleCookies(request, response, cookieNameToRemove);
            channel.write(response);
            return;
        }
        long size = file.length();
        ChunkedNioFile nioFile;
		try {
			nioFile = new ChunkedNioFile(file);
		} catch (IOException e) {
            response = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
                    HttpResponseStatus.NOT_FOUND);
            handleCookies(request, response, cookieNameToRemove);
            channel.write(response);
            return;
		}
        response = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
                HttpResponseStatus.OK);
        response.setHeader(HttpHeaders.Names.CONTENT_LENGTH,
                String.valueOf(size));
        ArrayList<String> cache_control = new ArrayList<String>(2);
        cache_control.add(HttpHeaders.Values.PUBLIC);
        cache_control.add(HttpHeaders.Values.MAX_AGE + "=" + 31536000);// 1 year
        response.setHeader(HttpHeaders.Names.CACHE_CONTROL, cache_control);
        response.setHeader(HttpHeaders.Names.LAST_MODIFIED,
                "Tue, 15 Nov 1994 12:45:26 GMT");
        handleCookies(request, response, cookieNameToRemove);
        // Write the response.
        channel.write(response);
        channel.write(nioFile);
	}

	/**
	 * Remove the given named cookie
	 * 
	 * @param request
	 * @param response
	 * @param cookieNameToRemove
	 */
	public static void handleCookies(HttpRequest request, HttpResponse response,
			String cookieNameToRemove) {
		String cookieString = request.getHeader(HttpHeaders.Names.COOKIE);
		if (cookieString != null) {
			CookieDecoder cookieDecoder = new CookieDecoder();
			Set<Cookie> cookies = cookieDecoder.decode(cookieString);
			if (!cookies.isEmpty()) {
				// Reset the sessions if necessary.
				CookieEncoder cookieEncoder = new CookieEncoder(true);
				// Remove all Session for images
				for (Cookie cookie : cookies) {
					if (cookie.getName().equalsIgnoreCase(cookieNameToRemove)) {
					} else {
						cookieEncoder.addCookie(cookie);
						response.addHeader(HttpHeaders.Names.SET_COOKIE, cookieEncoder.encode());
						cookieEncoder = new CookieEncoder(true);
					}
				}
			}
		}
	}

}
