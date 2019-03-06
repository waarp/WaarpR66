package org.waarp.openr66.protocol.http.restv2.data;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies that, after deserialization, all instances of classes marked with
 * this annotation must be checked for illegal or missing values in their fields.
 * This means that the object's field will be checked for compliance with the
 * {@link Required} and {@link Bounds} annotations which might be present.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface ConsistencyCheck {
}
