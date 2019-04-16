package org.waarp.openr66.protocol.http.restv2.errors;

/**
 * Factory class used to create instances of {@link Error} representing
 * input errors in an HTTP request made to the REST API.
 */
public final class Errors {

    /** Prevent the default constructor from being called. */
    private Errors() throws InstantiationException {
        throw new InstantiationException(this.getClass().getName() +
                " cannot be instantiated.");
    }

    /**
     * Creates an error saying that the request is missing a body when one
     * was required.
     *
     * @return  The corresponding new Error object.
     */
    public static Error MISSING_BODY() {
        return new Error("BadRequest.MissingBody", new String[0], 1);
    }

    /**
     * Creates an error saying that the request content is not a valid JSON object.
     *
     * @return  The corresponding new Error object.
     */
    public static Error MALFORMED_JSON(int line, int column, String cause) {
        String[] args = {Integer.toString(line), Integer.toString(column), cause};
        return new Error("BadRequest.MalformedJSON", args, 2);
    }

    /**
     * Creates an error saying that JSON object given has a duplicate field.
     *
     * @param field The missing parameter's name.
     * @return  The corresponding new Error object.
     */
    public static Error DUPLICATE_KEY(String field) {
        String[] args = {field};
        return new Error("BadRequest.DuplicateKey", args, 3);
    }

    /**
     * Creates an error saying that one of the request's parameters has an
     * illegal value. This includes numbers out of their expected range, or
     * invalid enum values.
     *
     * @param parameter The incorrect parameter's name.
     * @return  The corresponding new Error object.
     */
    public static Error ILLEGAL_PARAMETER_VALUE(String parameter, String value) {
        String[] args = {value, parameter};
        return new Error("BadRequest.IllegalParameterValue", args, 4);
    }

    /**
     * Creates an error saying that the JSON object sent had an extra unknown
     * field named with the given name.
     *
     * @param field The extra field encountered.
     * @return  The corresponding new Error object.
     */
    public static Error UNKNOWN_FIELD(String field) {
        String[] args = {field};
        return new Error("BadRequest.UnknownField", args, 5);
    }

    /**
     * Creates an error saying that one of the JSON object's field is missing
     * its value when one was required. This error is also raised when a
     * required field is missing from the object.
     *
     * @param field The field whose value is missing.
     * @return  The corresponding new Error object.
     */
    public static Error MISSING_FIELD(String field) {
        String[] args = {field};
        return new Error("BadRequest.MissingFieldValue", args, 6);
    }

    /**
     * Creates an error saying that the field named $field was given an illegal
     * value.
     *
     * @param field The name of the field whose value is incorrect.
     * @param value The given incorrect value.
     * @return  The corresponding new Error object.
     */
    public static Error ILLEGAL_FIELD_VALUE(String field, String value) {
        String[] args = {value, field};
        return new Error("BadRequest.IllegalFieldValue", args, 7);
    }

    /**
     * Creates an error saying that the database already contains an entry with
     * the same ID as the one in the entry the user tried to create or change.
     * Since the database cannot contain entries with duplicate IDs, the
     * requested entry cannot be created/updated.
     *
     * @param id The duplicate ID in the requested collection.
     * @return  The corresponding new Error object.
     */
    public static Error ALREADY_EXISTING(String id) {
        String[] args = {id};
        return new Error("BadRequest.AlreadyExisting", args, 8);
    }

    /**
     * Creates an error saying that the file requested for import at the given
     * location does not exist on the server.
     *
     * @param path The incorrect path.
     * @return  The corresponding new Error object.
     */
    public static Error FILE_NOT_FOUND(String path) {
        String[] args = {path};
        return new Error("BadRequest.FileNotFound", args, 9);
    }

    /**
     * Creates an error saying that the transfer rule entered alongside the
     * newly created transfer does not exist.
     *
     * @param rule  The unknown rule name.
     * @return  The corresponding new Error object.
     */
    public static Error UNKNOWN_RULE(String rule) {
        String[] args = {rule};
        return new Error("BadRequest.UnknownRule", args, 10);
    }

    /**
     * Creates an error saying that the transfer rule entered alongside the
     * newly created transfer does not exist.
     *
     * @param host  The unknown host name.
     * @return  The corresponding new Error object.
     */
    public static Error UNKNOWN_HOST(String host) {
        String[] args = {host};
        return new Error("BadRequest.UnknownHost", args, 11);
    }

    /**
     * Creates an error saying that the request was not signed, or that the
     * provided signature is invalid.
     *
     * @return  The corresponding new Error object.
     */
    public static Error REQUEST_NOT_SIGNED() {
        return new Error("BadRequest.UnsignedRequest", new String[0], 12);
    }

    /**
     * Creates an error saying that the given host is not allowed to use the
     * given rule for its transfers.
     *
     * @param host  The host executing the transfer.
     * @param rule  The rule which the host is not allowed to use.
     * @return  The corresponding new Error object.
     */
    public static Error RULE_NOT_ALLOWED(String host, String rule) {
        String[] args = {host, rule};
        return new Error("BadRequest.RuleNotAllowed", args, 13);
    }

    /**
     * Creates an error saying that the given field is a primary key, and thus
     * cannot be changed when updating an entry.
     *
     * @param field  The name of the field which was illegally modified.
     * @return  The corresponding new Error object.
     */
    public static Error FIELD_NOT_ALLOWED(String field) {
        String[] args = {field};
        return new Error("BadRequest.UnauthorizedField", args, 14);
    }
}
