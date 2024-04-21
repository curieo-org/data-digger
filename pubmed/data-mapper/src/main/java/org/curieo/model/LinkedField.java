package org.curieo.model;

/**
 * For extracting multiple fields from a single record we wrap the fields and link them to the
 * original through the publication id
 *
 * @param <T> field to be wrapped
 */
public record LinkedField<T>(int ordinal, Long publicationId, T field) {}
