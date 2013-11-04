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
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Locale;
import java.util.Set;
import java.util.TimeZone;

import javax.activation.MimetypesFileTypeMap;

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
     * US locale - all HTTP dates are in english
     */
    public final static Locale LOCALE_US = Locale.US;

    /**
     * GMT timezone - all HTTP dates are on GMT
     */
    public final static TimeZone GMT_ZONE = TimeZone.getTimeZone("GMT");

    /**
     * format for RFC 1123 date string -- "Sun, 06 Nov 1994 08:49:37 GMT"
     */
    public final static String RFC1123_PATTERN =
        "EEE, dd MMM yyyyy HH:mm:ss z";
    /**
     * set MIME TYPE if possible
     */
    public static final MimetypesFileTypeMap mimetypesFileTypeMap = new MimetypesFileTypeMap();
    static {
    	mimetypesFileTypeMap.addMimeTypes("text/css css CSS");
    	mimetypesFileTypeMap.addMimeTypes("text/javascript js JS");
    	//Official but not supported mimetypesFileTypeMap.addMimeTypes("application/javascript js JS");
    	mimetypesFileTypeMap.addMimeTypes("application/json json JSON");
    	mimetypesFileTypeMap.addMimeTypes("text/plain txt text TXT");
    	mimetypesFileTypeMap.addMimeTypes("text/html htm html HTM HTML htmls htx");
    	mimetypesFileTypeMap.addMimeTypes("image/jpeg jpe jpeg jpg JPG");
    	mimetypesFileTypeMap.addMimeTypes("image/png png PNG");
    	mimetypesFileTypeMap.addMimeTypes("image/gif gif GIF");
    	mimetypesFileTypeMap.addMimeTypes("image/x-icon ico ICO");
    }

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
        File file = new File(filename);
        if (!file.isFile() || !file.canRead()) {
            response = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
                    HttpResponseStatus.NOT_FOUND);
            handleCookies(request, response, cookieNameToRemove);
            channel.write(response);
            return;
        }
        DateFormat rfc1123Format = new SimpleDateFormat(RFC1123_PATTERN, LOCALE_US);
        rfc1123Format.setTimeZone(GMT_ZONE);
        Date lastModifDate = new Date(file.lastModified());
        if (request.containsHeader(HttpHeaders.Names.IF_MODIFIED_SINCE)) {
        	String sdate = request.getHeader(HttpHeaders.Names.IF_MODIFIED_SINCE);
        	try {
				Date ifmodif = rfc1123Format.parse(sdate);
				if (ifmodif.after(lastModifDate)) {
		            response = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
		                    HttpResponseStatus.NOT_MODIFIED);
		            handleCookies(request, response, cookieNameToRemove);
		            channel.write(response);
		            return;
				}
			} catch (ParseException e) {
			}
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
        
        String type = mimetypesFileTypeMap.getContentType(filename);
        response.setHeader(HttpHeaders.Names.CONTENT_TYPE, type);
        ArrayList<String> cache_control = new ArrayList<String>(2);
        cache_control.add(HttpHeaders.Values.PUBLIC);
        cache_control.add(HttpHeaders.Values.MAX_AGE + "=" + 604800);// 1 week
        response.setHeader(HttpHeaders.Names.CACHE_CONTROL, cache_control);
        response.setHeader(HttpHeaders.Names.LAST_MODIFIED,
                rfc1123Format.format(lastModifDate));
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
