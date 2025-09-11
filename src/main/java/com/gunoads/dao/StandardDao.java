package com.gunoads.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.util.List;
import java.util.Optional;

/**
 * Standard DAO for small-scale CRUD operations using JDBC batch
 * For large datasets, use BulkInsertDao with COPY FROM
 */
public abstract class StandardDao<T, ID> {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Autowired
    protected NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    // Abstract methods to be implemented by subclasses
    protected abstract String getTableName();
    protected abstract RowMapper<T> getRowMapper();
    protected abstract String getIdColumnName();
    protected abstract SqlParameterSource getInsertParameters(T entity);
    protected abstract SqlParameterSource getUpdateParameters(T entity);
    protected abstract String buildInsertSql();
    protected abstract String buildUpdateSql();

    /**
     * Find entity by ID
     */
    public Optional<T> findById(ID id) {
        String sql = "SELECT * FROM " + getTableName() + " WHERE " + getIdColumnName() + " = ?";

        try {
            T entity = jdbcTemplate.queryForObject(sql, getRowMapper(), id);
            return Optional.ofNullable(entity);
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        } catch (Exception e) {
            logger.error("Error finding entity by id {}: {}", id, e.getMessage());
            throw new RuntimeException("Database error while finding entity", e);
        }
    }

    /**
     * Find all entities
     */
    public List<T> findAll() {
        String sql = "SELECT * FROM " + getTableName();

        try {
            return jdbcTemplate.query(sql, getRowMapper());
        } catch (Exception e) {
            logger.error("Error finding all entities: {}", e.getMessage());
            throw new RuntimeException("Database error while finding entities", e);
        }
    }

    /**
     * Find entities with pagination
     */
    public List<T> findAll(int limit, int offset) {
        String sql = "SELECT * FROM " + getTableName() + " LIMIT ? OFFSET ?";

        try {
            return jdbcTemplate.query(sql, getRowMapper(), limit, offset);
        } catch (Exception e) {
            logger.error("Error finding entities with pagination: {}", e.getMessage());
            throw new RuntimeException("Database error while finding entities", e);
        }
    }

    /**
     * Count total entities
     */
    public long count() {
        String sql = "SELECT COUNT(*) FROM " + getTableName();

        try {
            Long count = jdbcTemplate.queryForObject(sql, Long.class);
            return count != null ? count : 0L;
        } catch (Exception e) {
            logger.error("Error counting entities: {}", e.getMessage());
            throw new RuntimeException("Database error while counting entities", e);
        }
    }

    /**
     * Check if entity exists by ID
     */
    public boolean existsById(ID id) {
        String sql = "SELECT COUNT(*) FROM " + getTableName() + " WHERE " + getIdColumnName() + " = ?";

        try {
            Long count = jdbcTemplate.queryForObject(sql, Long.class, id);
            return count != null && count > 0;
        } catch (Exception e) {
            logger.error("Error checking entity existence: {}", e.getMessage());
            throw new RuntimeException("Database error while checking existence", e);
        }
    }

    /**
     * Insert new entity
     */
    public void insert(T entity) {
        String sql = buildInsertSql();

        try {
            SqlParameterSource params = getInsertParameters(entity);
            int rowsAffected = namedParameterJdbcTemplate.update(sql, params);

            if (rowsAffected == 0) {
                throw new RuntimeException("Insert failed, no rows affected");
            }

            logger.debug("Successfully inserted entity into {}", getTableName());
        } catch (Exception e) {
            logger.error("Error inserting entity: {}", e.getMessage());
            throw new RuntimeException("Database error while inserting entity", e);
        }
    }

    /**
     * Update existing entity
     */
    public void update(T entity) {
        String sql = buildUpdateSql();

        try {
            SqlParameterSource params = getUpdateParameters(entity);
            int rowsAffected = namedParameterJdbcTemplate.update(sql, params);

            if (rowsAffected == 0) {
                throw new RuntimeException("Update failed, no rows affected");
            }

            logger.debug("Successfully updated entity in {}", getTableName());
        } catch (Exception e) {
            logger.error("Error updating entity: {}", e.getMessage());
            throw new RuntimeException("Database error while updating entity", e);
        }
    }

    /**
     * Delete entity by ID
     */
    public void deleteById(ID id) {
        String sql = "DELETE FROM " + getTableName() + " WHERE " + getIdColumnName() + " = ?";

        try {
            int rowsAffected = jdbcTemplate.update(sql, id);

            if (rowsAffected == 0) {
                logger.warn("No entity found to delete with id: {}", id);
            } else {
                logger.debug("Successfully deleted entity from {}", getTableName());
            }
        } catch (Exception e) {
            logger.error("Error deleting entity: {}", e.getMessage());
            throw new RuntimeException("Database error while deleting entity", e);
        }
    }

    /**
     * Batch insert entities (for small batches, use BulkInsertDao for large datasets)
     */
    public void batchInsert(List<T> entities) {
        if (entities == null || entities.isEmpty()) {
            return;
        }

        String sql = buildInsertSql();

        try {
            SqlParameterSource[] batchParams = entities.stream()
                    .map(this::getInsertParameters)
                    .toArray(SqlParameterSource[]::new);

            int[] rowsAffected = namedParameterJdbcTemplate.batchUpdate(sql, batchParams);

            logger.info("Batch inserted {} entities into {}", rowsAffected.length, getTableName());
        } catch (Exception e) {
            logger.error("Error in batch insert: {}", e.getMessage());
            throw new RuntimeException("Database error while batch inserting entities", e);
        }
    }

    /**
     * Execute custom query with parameters
     */
    protected List<T> executeQuery(String sql, SqlParameterSource params) {
        try {
            return namedParameterJdbcTemplate.query(sql, params, getRowMapper());
        } catch (Exception e) {
            logger.error("Error executing custom query: {}", e.getMessage());
            throw new RuntimeException("Database error while executing query", e);
        }
    }

    /**
     * Execute custom query without parameters
     */
    protected List<T> executeQuery(String sql) {
        try {
            return jdbcTemplate.query(sql, getRowMapper());
        } catch (Exception e) {
            logger.error("Error executing custom query: {}", e.getMessage());
            throw new RuntimeException("Database error while executing query", e);
        }
    }
}