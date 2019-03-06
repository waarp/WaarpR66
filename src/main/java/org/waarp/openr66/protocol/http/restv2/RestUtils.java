/*
 *  This file is part of Waarp Project (named also Waarp or GG).
 *
 *  Copyright 2009, Waarp SAS, and individual contributors by the @author
 *  tags. See the COPYRIGHT.txt in the distribution for a full listing of
 *  individual contributors.
 *
 *  All Waarp Project is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or (at your
 *  option) any later version.
 *
 *  Waarp is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 *  A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along with
 *  Waarp . If not, see <http://www.gnu.org/licenses/>.
 *
 */

package org.waarp.openr66.protocol.http.restv2;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import org.glassfish.jersey.message.internal.AcceptableLanguageTag;
import org.glassfish.jersey.message.internal.HttpHeaderReader;
import org.waarp.gateway.kernel.rest.RestConfiguration;
import org.waarp.openr66.protocol.http.restv2.data.Bounds;
import org.waarp.openr66.protocol.http.restv2.data.ConsistencyCheck;
import org.waarp.openr66.protocol.http.restv2.data.Required;
import org.waarp.openr66.protocol.http.restv2.errors.BadRequest;
import org.waarp.openr66.protocol.http.restv2.errors.BadRequestFactory;
import org.waarp.openr66.protocol.http.restv2.exceptions.BadRequestException;
import org.waarp.openr66.protocol.http.restv2.exceptions.NotJsonException;
import org.waarp.openr66.protocol.http.restv2.handler.AbstractRestHttpHandler;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.Path;
import javax.ws.rs.core.HttpHeaders;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;


/** A series of utility methods shared by all handlers of the RESTv2 API. */
public final class RestUtils {

    /** This is a utility class that should never be instantiated. */
    private RestUtils() {
        throw new UnsupportedOperationException(this.getClass().getName() +
                " cannot be instantiated.");
    }

    /**
     * Returns the Locale corresponding to the language requested in the request headers.
     * @param request   The HTTP request.
     * @return          The requested Locale.
     */
    public static Locale getLocale(HttpRequest request) {
        String langHead = request.headers().get(HttpHeaders.ACCEPT_LANGUAGE);
        try {
            List<AcceptableLanguageTag> acceptableLanguages =
                    HttpHeaderReader.readAcceptLanguage(langHead);
            AcceptableLanguageTag bestMatch = acceptableLanguages.get(0);
            for (AcceptableLanguageTag acceptableLanguage : acceptableLanguages) {
                if (acceptableLanguage.getQuality() > bestMatch.getQuality()) {
                    bestMatch = acceptableLanguage;
                }
            }
            return bestMatch.getAsLocale();
        } catch (ParseException e) {
            return Locale.getDefault();
        }
    }

    /**
     * This method transforms a request body in JSON format into an object of
     * the class passed as argument. If the {@code initializeFields} argument
     * is true the object missing fields will be initialized with the value
     * specified in their attached {@link DefaultValue} annotation.
     *
     * @param request The http request whose body has to be deserialized.
     * @param c       The class of the object that will be created from the JSON
     *                object.
     * @param initializeFields Specifies if missing fields should be initialized.
     * @param <T>     The generic type of the output object.
     * @return The Java object representing the source JSON object.
     * @throws BadRequestException Thrown if an error occurred during
     *                                        the processing.
     * @throws IOException Thrown if the request body cannot be read.
     * @throws NotJsonException Thrown if the request body does not contain a
     *                          valid JSON object.
     * @throws IllegalAccessException Thrown if the access modifier of one of
     *                                the field of the target class does not allow
     *                                access from this method.
     *
     */
    public static <T> T deserializeJsonRequest(HttpRequest request, Class<T> c,
                                               boolean initializeFields)
            throws BadRequestException, IOException, NotJsonException,
            IllegalAccessException {

        try {
            if (request instanceof FullHttpRequest) {
                String body = ((FullHttpRequest) request).content().toString(
                        Charset.forName("UTF-8"));
                ObjectMapper mapper = new ObjectMapper();
                mapper.configure(
                        DeserializationFeature.USE_JAVA_ARRAY_FOR_JSON_ARRAY, true);
                mapper.configure(
                        DeserializationFeature.FAIL_ON_NUMBERS_FOR_ENUMS, true);
                mapper.configure(
                        DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY, true);
                mapper.configure(
                        DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, true);
                mapper.configure(
                        DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT, true);
                mapper.configure(
                        DeserializationFeature.FAIL_ON_INVALID_SUBTYPE, false);
                mapper.configure(
                        DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
                mapper.configure(
                        DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);

                T bodyObject = mapper.readValue(body, c);
                if (initializeFields) {
                    checkEntry(bodyObject, true);
                    return fillEmptyFields(bodyObject);
                } else {
                    checkEntry(bodyObject, false);
                    return bodyObject;
                }
            } else {
                throw new BadRequestException(
                        BadRequestFactory.missingBody());
            }
        } catch (JsonMappingException e) {
            if (e.getPath().size() == 0) {
                throw new BadRequestException(
                        BadRequestFactory.missingBody());
            }
            JsonMappingException.Reference ref =
                    e.getPath().get(e.getPath().size() - 1);
            String field = ref.getFieldName();
            try {
                System.err.println(ref.getFrom().getClass().getName());
                ref.getFrom().getClass().getField(field);
                String val = e.getLocation().toString();
                throw new BadRequestException(
                        BadRequestFactory.illegalFieldValue(field, val));
            } catch (NoSuchFieldException nsf) {
                throw new BadRequestException(
                        BadRequestFactory.unknownField(field));
            }
        } catch (JsonParseException e) {
            String contentType = request.headers().get(HttpHeaders.CONTENT_TYPE);
            if (contentType == null || contentType.isEmpty()) {
                throw new NotJsonException();
            } else {
                throw new BadRequestException(BadRequestFactory.malformedJson());
            }
        }
    }

    /**
     * Extracts the available HTTP methods of a handler and returns them as a
     * {@link String} listing all the methods extracted.
     *
     * @param handler The handler from which to extract the available HTTP methods.
     * @return A String listing the available HTTP methods on the handler,
     * separated by comas.
     */
    public static String options(Class<? extends AbstractRestHttpHandler> handler,
                                 byte crud) {
        ArrayList<String> methods = new ArrayList<String>();
        for (Method method : handler.getMethods()) {
            if (!method.isAnnotationPresent(Path.class)) {
                for (Annotation annotation : method.getDeclaredAnnotations()) {
                    if (annotation.annotationType().isAnnotationPresent(
                            HttpMethod.class)) {
                        String methodName = annotation.annotationType()
                                .getAnnotation(HttpMethod.class).value();
                        if (!methods.contains(methodName)) {
                            if ( (methodName.equals(HttpMethod.GET) ||
                                    methodName.equals(HttpMethod.HEAD) ) &&
                                    RestConfiguration.CRUD.READ.isValid(crud)) {
                                methods.add(methodName);
                            }
                            else if (methodName.equals(HttpMethod.POST) &&
                                    RestConfiguration.CRUD.CREATE.isValid(crud)) {
                                methods.add(methodName);
                            }
                            else if (methodName.equals(HttpMethod.PUT) &&
                                    RestConfiguration.CRUD.UPDATE.isValid(crud)) {
                                methods.add(methodName);
                            }
                            else if (methodName.equals(HttpMethod.DELETE) &&
                                    RestConfiguration.CRUD.DELETE.isValid(crud)) {
                                methods.add(methodName);
                            }
                        }
                    }
                }
            }
        }

        return methods.toString().replaceAll("[\\[\\]]", "");
    }

    /**
     * Transforms a Java Object into it's equivalent JSON object in a String.
     *
     * @param object The Object to convert to JSON.
     * @return The rule as a JSON String.
     * @throws JsonProcessingException Thrown if the object cannot be serialized
     *                                 into JSON.
     */
    public static String serialize(Object object)
            throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(object);
    }

    /**
     *
     * @param object
     * @param <T>
     * @return
     * @throws IllegalAccessException
     */
    private static <T> T fillEmptyFields(T object) throws IllegalAccessException {
        for (Field field : object.getClass().getFields()) {
            if (field.get(object) == null) {
                if (field.isAnnotationPresent(DefaultValue.class)) {
                    String defaultValue = field.getAnnotation(DefaultValue.class).value();
                    Class clazz = field.getType();

                    if (clazz == String.class) {
                        field.set(object, defaultValue);
                    } else if (clazz == Boolean.class) {
                        field.set(object, Boolean.valueOf(defaultValue));
                    } else if (clazz == Integer.class) {
                        field.set(object, Integer.valueOf(defaultValue));
                    } else if (clazz == Long.class) {
                        field.set(object, Long.valueOf(defaultValue));
                    } else if (clazz.isEnum()) {
                        field.set(object, Enum.valueOf(clazz, defaultValue));
                    } else if (clazz.isArray()) {
                        field.set(object, Array.newInstance(clazz.getComponentType(), 0));
                    } else {
                        field.set(object, null);
                    }
                }
            }
        }
        return object;
    }

    /**
     * Checks if the value entered is within the bounds defined in the {@link Bounds}
     * annotation attached to the field.
     *
     * @param value     The value to check.
     * @param bounds    The 'Or' annotation containing the bounds to check.
     * @return  'true' if the value is within the bounds, 'false' if not.
     */
    private static boolean checkBounds(long value, Bounds bounds) {
        return value >= bounds.min() && value <= bounds.max();
    }

    /**
     * Checks if the entry fields have all been initialized and have correct
     * values, then returns the entry. If the {@code checkRequired} parameter is
     * true, all required fields with a {@code null} value will produce an error.
     *
     * @param entry         The entry to check.
     * @param checkRequired Specifies whether or not required fields should be
     *                      checked.
     * @throws BadRequestException  Thrown if one of the entry fields
     *                              is inaccessible because of its access modifier.
     */
    private static void checkEntry(Object entry, boolean checkRequired)
            throws BadRequestException, IllegalAccessException {

        List<BadRequest> errors = new ArrayList<BadRequest>();
        for (Field field : entry.getClass().getFields()) {
            Class clazz = field.getType();
            Object val = field.get(entry);

            if (val == null) {
                if (checkRequired && field.isAnnotationPresent(Required.class)) {
                    errors.add(BadRequestFactory.missingFieldValue(field.getName()));
                }
            }
            else {
                if (field.isAnnotationPresent(Bounds.class)) {
                    if (val.getClass() == Integer.class) {
                        long num = (Integer) val;
                        if (!checkBounds(num, field.getAnnotation(Bounds.class))) {
                            errors.add(BadRequestFactory.illegalFieldValue(field.getName(), val.toString()));
                        }
                    }
                    else if (val.getClass() == Long.class) {
                        long num = (Long) val;
                        if (!checkBounds(num, field.getAnnotation(Bounds.class))) {
                            errors.add(BadRequestFactory.illegalFieldValue(field.getName(), val.toString()));
                        }
                    }
                } else if (!clazz.isEnum()) {
                    if (clazz.isArray()) {
                        for (Object obj : (Object[]) val) {
                            checkEntry(obj, checkRequired);
                        }
                    } else if (clazz.isAnnotationPresent(ConsistencyCheck.class)) {
                        checkEntry(val, checkRequired);
                    }
                }
            }
        }
        if(!errors.isEmpty()) {
            throw new BadRequestException(errors);
        }
    }

    /**
     * Converts a string to the corresponding Boolean object. If the String is
     * neither empty, "true" or "false" (case insensitive) then an exception
     * is thrown.
     *
     * @param string    The String to convert.
     * @return          The corresponding Boolean object (true, false or null).
     */
    public static Boolean stringToBoolean(String string) {
        if(string.isEmpty()) {
            return null;
        } else if("true".equalsIgnoreCase(string)) {
            return true;
        } else if("false".equalsIgnoreCase(string)) {
            return false;
        } else {
            return null;
        }
    }

    /**
     * Transforms a Java array into a String where each element is separated by
     * a single space.
     *
     * @param array The array to transform.
     * @return  The transformed String representing the array elements.
     */
    public static String toArrayDbList(Object[] array) {
        StringBuilder list = new StringBuilder();
        for(Object object : array) {
            list.append(object.toString()).append(" ");
        }
        return list.toString().trim();
    }

    /**
     * Formats an unformatted XML String into a human readable one.
     *
     * @param input   The unformatted XML String.
     * @param indent  The number of spaces used to indent the XML tags.
     * @return      The XML String in human readable format.
     * @throws TransformerException Thrown if an error occurred during the
     *                              String transformer's initialization or
     *                              during the String transformation.
     */
    public static String prettyXML(String input, int indent) throws TransformerException {
        Source xmlInput = new StreamSource(new StringReader(input));
        StringWriter stringWriter = new StringWriter();
        StreamResult xmlOutput = new StreamResult(stringWriter);
        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        transformerFactory.setAttribute("indent-number", indent);
        Transformer transformer = transformerFactory.newTransformer();
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.transform(xmlInput, xmlOutput);
        return xmlOutput.getWriter().toString();
    }

    /**
     * Serializes and save a Java object to an XML file saved at the location
     * given as parameter.
     *
     * @param object    The object to save as XML.
     * @param filePath  The path where to save the XML file.
     * @param <T>       The type of the saved object.
     * @throws JAXBException Thrown if the object cannot be serialized into an
     *                       XML, usually because it is missing the necessary
     *                       JAXB annotations.
     * @throws IOException If the path designate an existing file, this is thrown
     *                     if the file cannot be written into. If the path does
     *                     not designate an existing file, this is thrown if no
     *                     new file can be created at the given location.
     */
    public static <T> void objectToXmlFile(T object, String filePath)
            throws JAXBException, IOException {

        File file = new File(filePath);
        if (!file.createNewFile() && !file.canWrite()) {
            throw new IOException();
        }

        JAXBContext context = JAXBContext.newInstance(object.getClass());
        Marshaller marshaller = context.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        marshaller.marshal(object, file);
    }

    /**
     * Deserialize an XML file into a corresponding Java object.
     * @param filePath  Location of the input XML file.
     * @param c         The class of the desired Java object.
     * @param <T>       The type parameter of the output Java object.
     * @return          The deserialized Java object.
     * @throws JAXBException Thrown if the XML file cannot be deserialized into
     *                       the desired object, usually because the XML is malformed.
     * @throws IOException   Thrown if the given file cannot be read.
     * @throws FileNotFoundException Thrown if the given file does not exist.
     */
    public static <T> T xmlFileToObject(String filePath, Class<T> c)
            throws JAXBException, FileNotFoundException, IOException {

        FileReader fileReader = new FileReader(filePath);
        if (!fileReader.ready()) {
            throw new IOException();
        }
        StreamSource fileSource = new StreamSource(fileReader);

        JAXBContext context = JAXBContext.newInstance(c);
        Unmarshaller unmarshaller = context.createUnmarshaller();

        return unmarshaller.unmarshal(fileSource, c).getValue();
    }

    /**
     * Saves an unformatted XML String to a formatted XML file saved at the
     * location given as parameter.
     *
     * @param xml       The unformatted XML String.
     * @param filePath  The path where to save the XML file.
     * @throws IOException  Thrown if the output file cannot be written into.
     * @throws TransformerException Thrown if the XML String cannot be formatted,
     *                              this generally means that the String does not
     *                              represent a valid XML.
     */
    public static void xmlStringToXmlFile(String xml, String filePath)
            throws TransformerException, IOException {
        FileWriter fileWriter = new FileWriter(filePath, false);
        String formattedXML = RestUtils.prettyXML(xml, 4);
        fileWriter.write(formattedXML);
        fileWriter.flush();
        fileWriter.close();
    }

    /**
     * Loads an XML file directly into a Java String.
     *
     * @param filePath  The path where to save the XML file.
     * @throws IOException           Thrown if the file cannot be read.
     * @throws FileNotFoundException Thrown if the file does not exist.
     */
    public static String xmlFileToXmlString(String filePath)
            throws FileNotFoundException, IOException {
