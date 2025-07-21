package com.luzx.simplejdbchelper.JdbcHelper;

import java.io.Serializable;
import java.util.function.Function;

@FunctionalInterface
public interface JdbcHelperFunction<T, R> extends Function<T, R>, Serializable {
}
