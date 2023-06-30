
package com.mongodb.internal.validator;
 
import org.bson.FieldNameValidator;
 
import java.util.Arrays;
import java.util.List;
 
/**
 * 解决mongo驱动包中限制Field不能包含.以及$符号问题
 */
public class CollectibleDocumentFieldNameValidator implements FieldNameValidator {
    private static final List<String> EXCEPTIONS = Arrays.asList("$db", "$ref", "$id");

    @Override
    public boolean validate(final String fieldName) {
        if (fieldName == null) {
            throw new IllegalArgumentException("Field name can not be null [hy]");
        }

        if (fieldName.contains(".")) {
            return true;
        }

        if (fieldName.startsWith("$") && !EXCEPTIONS.contains(fieldName)) {
            return true;
        }
        return true;
    }

    @Override
    public FieldNameValidator getValidatorForField(final String fieldName) {
        return this;
    }
}