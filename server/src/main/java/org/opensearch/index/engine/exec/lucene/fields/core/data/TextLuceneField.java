/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.fields.core.data;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.opensearch.index.engine.exec.lucene.fields.LuceneField;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.TextFieldMapper;

public class TextLuceneField extends LuceneField {

    private static final FieldType[] FIELD_TYPE_CACHE = new FieldType[2];

    static {
        for (int i = 0; i < 2; i++) {
            FieldType ft = new FieldType();
            ft.setStored((i & 1) != 0);
            ft.setIndexOptions(IndexOptions.DOCS);
            ft.freeze();
            FIELD_TYPE_CACHE[i] = ft;
        }
    }

    @Override
    public void createField(MappedFieldType mappedFieldType, ParseContext.Document document, Object parseValue) {
        final TextFieldMapper.TextFieldType textFieldType = (TextFieldMapper.TextFieldType) mappedFieldType;
        final String value = (String) parseValue;
        FieldType fieldType = FIELD_TYPE_CACHE[textFieldType.isStored() ? 1 : 0];
        Field field = new Field(textFieldType.name(), value, fieldType);
        document.add(field);
    }
}
