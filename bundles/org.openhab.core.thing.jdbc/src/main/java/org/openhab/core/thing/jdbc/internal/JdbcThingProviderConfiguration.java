/*
 * Copyright (c) 2010-2025 Contributors to the openHAB project
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0
 *
 * SPDX-License-Identifier: EPL-2.0
 */
package org.openhab.core.thing.jdbc.internal;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import org.eclipse.jdt.annotation.NonNullByDefault;
import org.eclipse.jdt.annotation.Nullable;

/**
 * Immutable configuration holder for the {@link JdbcThingProvider}.
 *
 * @author Gaël L'hopital - Initial contribution
 */

@NonNullByDefault
final class JdbcThingProviderConfiguration {

    static final String CONFIG_PID = "org.openhab.core.thing.jdbc";

    private static final String CFG_URL = "url";
    private static final String CFG_USERNAME = "username";
    private static final String CFG_PASSWORD = "password";
    private static final String CFG_DRIVER_CLASS = "driverClass";
    private static final String CFG_REFRESH_INTERVAL = "refreshInterval";

    private static final Duration DEFAULT_REFRESH_INTERVAL = Duration.ofMinutes(5);

    final String url;
    final @Nullable String username;
    final @Nullable String password;
    final @Nullable String driverClass;
    final Duration refreshInterval;

    private JdbcThingProviderConfiguration(String url, @Nullable String username, @Nullable String password,
            @Nullable String driverClass, Duration refreshInterval) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.driverClass = driverClass;
        this.refreshInterval = refreshInterval;
    }

    static Optional<JdbcThingProviderConfiguration> from(@Nullable Map<String, Object> properties) {
        if (properties == null) {
            return Optional.empty();
        }

        String url = readString(properties, CFG_URL);
        if (url == null || url.isBlank()) {
            return Optional.empty();
        }

        Duration refreshInterval = readDuration(properties, CFG_REFRESH_INTERVAL, DEFAULT_REFRESH_INTERVAL);

        String username = normalize(readString(properties, CFG_USERNAME));
        String password = normalize(readString(properties, CFG_PASSWORD));
        String driverClass = normalize(readString(properties, CFG_DRIVER_CLASS));

        return Optional.of(new JdbcThingProviderConfiguration(url, username, password, driverClass, refreshInterval));
    }

    private static @Nullable String normalize(@Nullable String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private static @Nullable String readString(Map<String, Object> properties, String key) {
        Object raw = properties.get(key);
        if (raw instanceof String s) {
            return s.trim();
        }
        if (raw instanceof String[] array && array.length > 0) {
            return array[0].trim();
        }
        return null;
    }

    private static Duration readDuration(Map<String, Object> properties, String key, Duration fallback) {
        Object raw = properties.get(key);
        if (raw == null) {
            return fallback;
        }

        try {
            if (raw instanceof Number number) {
                return sanitizeDuration(number.longValue(), fallback);
            }
            if (raw instanceof String s && !s.isBlank()) {
                return sanitizeDuration(Long.parseLong(s.trim()), fallback);
            }
        } catch (NumberFormatException e) {
            // ignore and use fallback
        }
        return fallback;
    }

    private static Duration sanitizeDuration(long seconds, Duration fallback) {
        if (seconds <= 0) {
            return Duration.ZERO;
        }
        return Duration.ofSeconds(seconds);
    }
}
