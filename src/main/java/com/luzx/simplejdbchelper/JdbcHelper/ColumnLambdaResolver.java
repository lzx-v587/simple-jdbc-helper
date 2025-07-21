package com.luzx.simplejdbchelper.JdbcHelper;

import com.luzx.simplejdbchelper.JdbcAnnotation.Column;
import org.springframework.util.StringUtils;

import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class ColumnLambdaResolver {

    /**
     * 解析Lambda引用对应字段的@Column的注解值
     *
     * @param fn          可序列化Getter ,例如User::getName
     * @param entityClass 实体class 比如 User.class
     * @param <T>         实体类型
     * @param <R>         字段类型
     * @return @Column 标注的类名,或字段名转下划线
     */
    public static <T, R> String resolverColumn(JdbcHelperFunction<T, R> fn, Class<T> entityClass) {
        try {
            // 反射调用 writeReplace 拿到 SerializedLambda
            Method writeReplace = fn.getClass().getDeclaredMethod("writeReplace");
            writeReplace.setAccessible(true);
            SerializedLambda sl = (SerializedLambda) writeReplace.invoke(fn);

            // 获取方法名, 如"getName"
            String implMethodName = sl.getImplMethodName();

            // 提取字段名: 去掉get/is 前缀
            String fieldName;
            if (implMethodName.startsWith("get")) {
                fieldName = implMethodName.substring(3);
            } else if (implMethodName.startsWith("is")) {
                fieldName = implMethodName.substring(2);
            } else {
                fieldName = implMethodName;
            }

            fieldName = Character.toLowerCase(fieldName.charAt(0)) + fieldName.substring(1);

            // 在实体或父类中查找该 field
            Field field = findField(entityClass, fieldName);
            if (field == null) {
                throw new IllegalStateException("找不到字段: " + fieldName + ",在类:" + entityClass + "看看字段名符不符合一般约定");
            }

            // 读取@Column 的注解值
            Column colAnno = field.getAnnotation(Column.class);
            if (colAnno != null && StringUtils.hasText(colAnno.value())) {
                return colAnno.value();
            }

            // 无注解时,驼峰转下划线
            return toSnakeCase(fieldName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String toSnakeCase(String camel) {
        return camel.replaceAll("([a-z])([A-Z])", "$1_$2").toLowerCase();
    }


    private static <T> Field findField(Class<T> entityClass, String fieldName) {
        Class<?> cur = entityClass;
        while (cur != null && cur != Object.class) {
            try {
                Field declaredField = cur.getDeclaredField(fieldName);
                declaredField.setAccessible(true);
                return declaredField;
            } catch (NoSuchFieldException ignored) {
            }
            cur = cur.getSuperclass();
        }
        return null;
    }
}
