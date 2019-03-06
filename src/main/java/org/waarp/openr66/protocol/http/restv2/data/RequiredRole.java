package org.waarp.openr66.protocol.http.restv2.data;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static org.waarp.common.role.RoleDefault.ROLE;

/**
 * This annotation specifies what minimum {@link ROLE} is required
 * in order to be allowed to call the method annotated.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface RequiredRole {
    ROLE value();
}
