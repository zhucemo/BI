
package com.mongodb.internal.validator;

import org.bson.FieldNameValidator;

/**
 * A field name validator for update documents.  It ensures that all top-level fields start with a '$'.
 *
 * <p>This class should not be considered a part of the public API.</p>
 */
public class UpdateFieldNameValidator implements org.bson.FieldNameValidator {
    @Override
    public boolean validate(final String fieldName) {
        return true;
    }

    @Override
    public FieldNameValidator getValidatorForField(final String fieldName) {
        return new NoOpFieldNameValidator();
    }
}
