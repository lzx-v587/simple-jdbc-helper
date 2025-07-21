package com.luzx.simplejdbchelper.JdbcHelper;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
public class QueryChainUtil<T> {
    private final JdbcTemplate jdbc;
    private final String table;
    private final List<String> columns = new ArrayList<>();
    private final StringBuilder whereSql = new StringBuilder();
    private final List<Object> params = new ArrayList<>();
    private String orderBy;
    private Integer offset, limit;
    private final Class<T> entityClass;

    private QueryChainUtil(JdbcTemplate jdbc, Class<T> entityClass) {
        this.jdbc = jdbc;
        this.entityClass = entityClass;
        TableMeta tableMeta = TableMetaResolver.getTableMeta(entityClass);
        this.table = tableMeta.getTableName();
        this.columns.addAll(tableMeta.getColumns());
        this.limit = 10;
        this.offset = 0;
    }

    public static <T> QueryChainUtil<T> of(JdbcTemplate jdbc, Class<T> entityClass) {
        return new QueryChainUtil<>(jdbc, entityClass);
    }

    /**
     * 添加where条件,支持多次调用自动用and连接
     */
    public QueryChainUtil<T> where(String sql, Object... args) {
        if (!whereSql.isEmpty()) {
            whereSql.append(" and ");
        }
        whereSql.append("(").append(sql).append(")");
        Collections.addAll(params, args);
        return this;
    }

    public <R> QueryChainUtil<T> eq(JdbcHelperFunction<T, R> getter, Object... args) {
        return getQueryChain(getter, " = ", args);
    }

    public <R> QueryChainUtil<T> ne(JdbcHelperFunction<T, R> getter, Object... args) {
        return getQueryChain(getter, " <> ", args);
    }

    public <R> QueryChainUtil<T> gt(JdbcHelperFunction<T, R> getter, Object... args) {
        return getQueryChain(getter, " > ", args);
    }

    public <R> QueryChainUtil<T> ge(JdbcHelperFunction<T, R> getter, Object... args) {
        return getQueryChain(getter, " >= ", args);
    }

    public <R> QueryChainUtil<T> it(JdbcHelperFunction<T, R> getter, Object... args) {
        return getQueryChain(getter, " < ", args);
    }

    public <R> QueryChainUtil<T> ie(JdbcHelperFunction<T, R> getter, Object... args) {
        return getQueryChain(getter, " <= ", args);
    }

    private <R> QueryChainUtil<T> getQueryChain(JdbcHelperFunction<T, R> getter, String str, Object... args) {
        if (entityClass == null) {
            throw new IllegalStateException("请先调用 of方法 指定实体类");
        }
        if (!whereSql.isEmpty()) {
            whereSql.append(" and ");
        }
        String col = ColumnLambdaResolver.resolverColumn(getter, entityClass);
        whereSql.append("(").append(col).append(str).append(")");
        Collections.addAll(params, args);
        return this;
    }

    /**
     * order by 子句
     */
    public QueryChainUtil<T> orderBy(String expr) {
        this.orderBy = expr;
        return this;
    }

    /**
     * 分页
     */
    public QueryChainUtil<T> limit(int offset, int limit) {
        this.limit = limit;
        this.offset = offset;
        return this;
    }

    /**
     * 返回List<T>
     */
    public List<T> list() {
        String sql = buildSql();
        Object[] arr = params.toArray();
        return jdbc.query(sql, new BeanPropertyRowMapper<>(entityClass), arr);
    }

    /**
     * 返回单个对象,没查到就null
     */
    public T one() {
        this.limit = 1;
        List<T> list = list();
        return list.isEmpty() ? null : list.getFirst();
    }

    private String buildSql() {
        if (table == null) {
            throw new IllegalStateException("你TM表呢!!!");
        }
        String cols = columns.isEmpty() ? "*" : String.join(", ", columns);
        StringBuilder sb = new StringBuilder("SELECT ")
                .append(cols)
                .append(" FROM ")
                .append(table);

        if (!whereSql.isEmpty()) {
            sb.append(" WHERE ").append(whereSql);
        }
        if (orderBy != null) {
            sb.append(" ORDER BY ").append(orderBy);
        }
        /*
          不同数据库分页语法不同,这里用MySQL的语法,如有需要自行添加其他构建sql方法
         */
        if (limit != null) {
            sb.append(" LIMIT ").append(offset == null ? "0" : offset)
                    .append(" , ").append(limit);
        }
        log.info("sql:{}", sb);
        return sb.toString();
    }

}
