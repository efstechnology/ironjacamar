package org.jboss.jca.adapters.jdbc;

import org.jboss.logging.Logger;

import java.sql.*;

/**
 * Basic decorator which captures sql and prints out query times
 *
 * @author jbaxter - 2014-01-27
 */
public class LoggingStatement implements Statement, StatementLogger {

    private static final Logger log = Logger.getLogger("com.efstech." + LoggingStatement.class.getName());

    private final Statement statement;

    private String sql;

    public LoggingStatement(Statement statement) {
        log.debug("Statement created");
        this.statement = statement;
    }

    @Override
    public String getSql() {
        return sql;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        this.sql = sql;
        long startTime = System.currentTimeMillis();
        final ResultSet resultSet = statement.executeQuery(sql);
        log.debug("[" + (System.currentTimeMillis() - startTime) + " ms] executeQuery(string) [" + sql + "]");
        return resultSet;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        this.sql = sql;
        long startTime = System.currentTimeMillis();
        final int i = statement.executeUpdate(sql);
        log.debug("[" + (System.currentTimeMillis() - startTime) + " ms] executeUpdate(string) [" + sql + "]");
        return i;
    }

    @Override
    public void close() throws SQLException {
        statement.close();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return statement.getMaxFieldSize();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        statement.setMaxFieldSize(max);
    }

    @Override
    public int getMaxRows() throws SQLException {
        return statement.getMaxRows();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        statement.setMaxRows(max);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        statement.setEscapeProcessing(enable);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return statement.getQueryTimeout();
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        statement.setQueryTimeout(seconds);
    }

    @Override
    public void cancel() throws SQLException {
        statement.cancel();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return statement.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        statement.clearWarnings();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        statement.setCursorName(name);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        this.sql = sql;
        long startTime = System.currentTimeMillis();
        final boolean execute = statement.execute(sql);
        log.debug("[" + (System.currentTimeMillis() - startTime) + " ms] execute(string) [" + sql + "]");
        return execute;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return statement.getResultSet();
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return statement.getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return statement.getMoreResults();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        statement.setFetchDirection(direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return statement.getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        statement.setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return statement.getFetchSize();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return statement.getResultSetConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
        return statement.getResultSetType();
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        this.sql = sql;
        statement.addBatch(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        statement.clearBatch();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return statement.executeBatch();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return statement.getConnection();
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return statement.getMoreResults(current);
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return statement.getGeneratedKeys();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        this.sql = sql;
        long startTime = System.currentTimeMillis();
        final int i = statement.executeUpdate(sql, autoGeneratedKeys);
        log.debug("[" + (System.currentTimeMillis() - startTime) + " ms] executeUpdate(string,int) [" + sql + "]");
        return i;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        this.sql = sql;
        long startTime = System.currentTimeMillis();
        final int i = statement.executeUpdate(sql, columnIndexes);
        log.debug("[" + (System.currentTimeMillis() - startTime) + " ms] executeUpdate(string,int[]) [" + sql + "]");
        return i;
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        this.sql = sql;
        long startTime = System.currentTimeMillis();
        final int i = statement.executeUpdate(sql, columnNames);
        log.debug("[" + (System.currentTimeMillis() - startTime) + " ms] executeUpdate(string,string[]) [" + sql + "]");
        return i;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        this.sql = sql;
        long startTime = System.currentTimeMillis();
        final boolean execute = statement.execute(sql, autoGeneratedKeys);
        log.debug("[" + (System.currentTimeMillis() - startTime) + " ms] execute(string,int) [" + sql + "]");
        return execute;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        this.sql = sql;
        long startTime = System.currentTimeMillis();
        final boolean execute = statement.execute(sql, columnIndexes);
        log.debug("[" + (System.currentTimeMillis() - startTime) + " ms] execute(string,int[]) [" + sql + "]");
        return execute;
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        this.sql = sql;
        long startTime = System.currentTimeMillis();
        final boolean execute = statement.execute(sql, columnNames);
        log.debug("[" + (System.currentTimeMillis() - startTime) + " ms] execute(string,string[]) [" + sql + "]");
        return execute;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return statement.getResultSetHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return statement.isClosed();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        statement.setPoolable(poolable);
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return statement.isPoolable();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        statement.closeOnCompletion();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return statement.isCloseOnCompletion();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return statement.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return statement.isWrapperFor(iface);
    }

}
