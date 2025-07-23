package com.luzx.simplejdbchelper.JdbcHelper;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

@Slf4j
public class QueryChainUtil<T> {
    private final JdbcTemplate jdbc;
    private final String table;
    private final List<String> columns;
    private final Class<T> entityClass;

    // SQL 片段
    private final List<String> joinClauses  = new ArrayList<>();
    private final List<String> whereClauses = new ArrayList<>();
    private final List<Object> params       = new ArrayList<>();

    private String groupBy;
    private String having;
    private String orderBy;
    private Integer offset = 0, limit = 10;

    /** 缓存 Lambda → 列名 映射，减少反射解析开销 */
    private static final ConcurrentMap<String, String> LAMBDA_COLUMN_CACHE = new ConcurrentHashMap<>();

    /**
     * 构造函数
     *
     * @param jdbc        JdbcTemplate实例
     * @param entityClass 实体类类型
     */
    private QueryChainUtil(JdbcTemplate jdbc, Class<T> entityClass) {
        this.jdbc        = jdbc;
        this.entityClass = entityClass;

        TableMeta meta = TableMetaResolver.getTableMeta(entityClass);
        this.table   = meta.getTableName();
        this.columns = new ArrayList<>(meta.getColumns());
    }

    /**
     * 静态工厂方法，创建QueryChainUtil实例
     *
     * @param jdbc        JdbcTemplate实例
     * @param entityClass 实体类类型
     * @param <T>         实体类类型参数
     * @return QueryChainUtil实例
     */
    public static <T> QueryChainUtil<T> of(JdbcTemplate jdbc, Class<T> entityClass) {
        return new QueryChainUtil<>(jdbc, entityClass);
    }

    /** 自定义 SELECT 字段 */
    public QueryChainUtil<T> select(String... cols) {
        this.columns.clear();
        Collections.addAll(this.columns, cols);
        return this;
    }

    /** 添加 WHERE 条件（多次调用自动 AND 连接） */
    public QueryChainUtil<T> where(String sql, Object... args) {
        whereClauses.add("(" + sql + ")");
        Collections.addAll(params, args);
        return this;
    }


    public <R> QueryChainUtil<T> eq(JdbcHelperFunction<T, R> getter, Object... args) {
        return getQueryChain(getter, " = ?", args);
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

    public <R> QueryChainUtil<T> lt(JdbcHelperFunction<T, R> getter, Object... args) {
        return getQueryChain(getter, " < ", args);
    }

    public <R> QueryChainUtil<T> le(JdbcHelperFunction<T, R> getter, Object... args) {
        return getQueryChain(getter, " <= ", args);
    }

    private <R> QueryChainUtil<T> getQueryChain(JdbcHelperFunction<T, R> getter,
                                                String opWithPlaceholder,
                                                Object... args) {
        String col = getCol(getter);
        whereClauses.add("(" + col + opWithPlaceholder + "? )");
        Collections.addAll(params, args);
        return this;
    }

    public <R> QueryChainUtil<T> in(JdbcHelperFunction<T, R> getter, Object... values) {
        if (values == null || values.length == 0) {
            // 空集合时不添加条件
            return this;
        }
        String col = getCol(getter);
        String placeholders = String.join(", ", Collections.nCopies(values.length, "?"));
        whereClauses.add("(" + col + " IN (" + placeholders + "))");
        Collections.addAll(params, values);
        return this;
    }

    public <R> QueryChainUtil<T> like(JdbcHelperFunction<T, R> getter, Object value) {
        String col = getCol(getter);
        whereClauses.add("(" + col + " LIKE ? )");
        params.add(value);
        return this;
    }

    private <R> String getCol(JdbcHelperFunction<T, R> getter) {
        String key = getter.toString();
        return LAMBDA_COLUMN_CACHE
                .computeIfAbsent(key, k -> ColumnLambdaResolver.resolverColumn(getter, entityClass));
    }

    /** 内连接 */
    public QueryChainUtil<T> innerJoin(String tableExpr, String on) {
        joinClauses.add("INNER JOIN " + tableExpr + " ON " + on);
        return this;
    }

    /** 左连接 */
    public QueryChainUtil<T> leftJoin(String tableExpr, String on) {
        joinClauses.add("LEFT JOIN " + tableExpr + " ON " + on);
        return this;
    }

    /** 右连接 */
    public QueryChainUtil<T> rightJoin(String tableExpr, String on) {
        joinClauses.add("RIGHT JOIN " + tableExpr + " ON " + on);
        return this;
    }

    /** GROUP BY 子句 */
    public QueryChainUtil<T> groupBy(String expr) {
        this.groupBy = expr;
        return this;
    }

    /** HAVING 子句 */
    public QueryChainUtil<T> having(String expr) {
        this.having = expr;
        return this;
    }

    /** ORDER BY 子句 */
    public QueryChainUtil<T> orderBy(String expr) {
        this.orderBy = expr;
        return this;
    }

    /** 分页 */
    public QueryChainUtil<T> limit(int offset, int limit) {
        this.offset = offset;
        this.limit  = limit;
        return this;
    }

    /** 返回结果列表 */
    public List<T> list() {
        String sql  = buildSql(false);
        Object[] ps = params.toArray();
        return execute(sql, ps, args ->
                jdbc.query(sql,
                        new BeanPropertyRowMapper<>(entityClass),
                        args));
    }

    /** 返回单个对象，无则 null */
    public T one() {
        this.limit = 1;
        List<T> all = list();
        return all.isEmpty() ? null : all.getFirst();
    }

    /** 返回 COUNT(*) 结果 */
    public long count() {
        String sql  = buildSql(true);
        Object[] ps = params.toArray();
        return execute(sql, ps, args ->
                jdbc.query(sql,
                        rs -> rs.next() ? rs.getLong(1) : 0L,
                        args));
    }


    /** 构造最终 SQL */
    private String buildSql(boolean isCount) {
        StringBuilder sb = new StringBuilder(256);
        // SELECT / COUNT
        sb.append(isCount
                        ? "SELECT COUNT(*)"
                        : "SELECT " + (columns.isEmpty() ? "*" : String.join(", ", columns)))
                .append(" FROM ").append(table);

        // JOINs
        if (!joinClauses.isEmpty()) {
            sb.append(" ")
                    .append(String.join(" ", joinClauses));
        }

        // WHERE
        if (!whereClauses.isEmpty()) {
            sb.append(" WHERE ")
                    .append(String.join(" AND ", whereClauses));
        }

        // GROUP BY
        if (groupBy != null) {
            sb.append(" GROUP BY ").append(groupBy);
        }

        // HAVING
        if (having != null) {
            sb.append(" HAVING ").append(having);
        }

        // ORDER BY + LIMIT (仅非 count)
        if (!isCount) {
            if (orderBy != null) {
                sb.append(" ORDER BY ").append(orderBy);
            }
            if (limit != null) {
                sb.append(" LIMIT ")
                        .append(offset).append(", ").append(limit);
            }
        }

        String sql = sb.toString();
        log.debug("Built SQL: {}", sql);
        return sql;
    }

    /**
     * 统一执行入口，打印 SQL & 参数
     */
    private <R> R execute(String sql,
                          Object[] args,
                          Function<Object[], R> executor) {
        log.info("Executing SQL: {}; params: {}", sql, Arrays.toString(args));
        return executor.apply(args);
    }

}
