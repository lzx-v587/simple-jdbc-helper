package com.luzx.simplejdbchelper.JdbcHelper;

import lombok.Getter;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TableMeta {
    @Getter
    private final String tableName;
    @Getter
    private final List<String> columns;
    private final Map<String, String> fieldToColumn;

    public TableMeta(String tableName, List<String> columns, Map<String, String> fieldToColumn) {
        this.tableName = tableName;
        this.columns = Collections.unmodifiableList(columns);
        this.fieldToColumn = Collections.unmodifiableMap(fieldToColumn);
    }

    public String getColumn(String fieldName){
        return fieldToColumn.get(fieldName);
    }

}
