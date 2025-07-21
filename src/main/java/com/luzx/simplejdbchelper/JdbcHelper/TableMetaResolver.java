package com.luzx.simplejdbchelper.JdbcHelper;

import com.luzx.simplejdbchelper.JdbcAnnotation.Column;
import com.luzx.simplejdbchelper.JdbcAnnotation.Table;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;


public class TableMetaResolver {
    private static final ConcurrentHashMap<Class<?>, TableMeta> CACHE = new ConcurrentHashMap<>();

    public static TableMeta getTableMeta(Class<?> clazz) {
        return CACHE.computeIfAbsent(clazz, TableMetaResolver::loadMeta);
    }

    private static TableMeta loadMeta(Class<?> aClass) {
        Table tableAnno = aClass.getAnnotation(Table.class);
        if (tableAnno == null) {
            throw new IllegalStateException("你TM@Table注解呢!!!" + aClass.getName());
        }
        String tableName = tableAnno.value();
        LinkedHashMap<String, String> fieldToColumn = new LinkedHashMap<>();
        ArrayList<String> columns = new ArrayList<>();

        Class<?> current = aClass;
        while (current != Object.class) {
            for (Field field : current.getDeclaredFields()) {
                Column colAnno = field.getAnnotation(Column.class);
                String col = colAnno != null ? colAnno.value() : toSnakeCase(field.getName());
                fieldToColumn.put(field.getName(), col);
                columns.add(col);
            }
            current = current.getSuperclass();
        }
        return new TableMeta(tableName, columns, fieldToColumn);

    }

    private static String toSnakeCase(String camel) {
        return camel.replaceAll("([a-z])([A-Z])", "$1_$2").toLowerCase();
    }
}
