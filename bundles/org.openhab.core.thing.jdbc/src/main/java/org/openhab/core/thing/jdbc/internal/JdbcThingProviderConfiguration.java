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

import org.eclipse.jdt.annotation.NonNullByDefault;
import org.eclipse.jdt.annotation.Nullable;

/**
 * Immutable configuration holder for the {@link JdbcThingProvider}.
 *
 * @author Gaël L'hopital - Initial contribution
 */

@NonNullByDefault
final class JdbcThingProviderConfiguration {
    public static final String CONFIG_PID = "org.openhab.core.thing.jdbc";

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

    public static @Nullable JdbcThingProviderConfiguration from(@Nullable Map<String, Object> properties) {
        if (properties != null && readString(properties, CFG_URL) instanceof String url) {
            Duration refreshInterval = readDuration(properties, CFG_REFRESH_INTERVAL, DEFAULT_REFRESH_INTERVAL);
            String username = readString(properties, CFG_USERNAME);
            String password = readString(properties, CFG_PASSWORD);
            String driverClass = readString(properties, CFG_DRIVER_CLASS);

            return new JdbcThingProviderConfiguration(url, username, password, driverClass, refreshInterval);
        }
        return null;
    }

    private static @Nullable String readString(Map<String, Object> properties, String key) {
        Object raw = properties.get(key);
        if (raw == null) {
            return null;
        }

        return switch (raw) {
            case String s when s.isBlank() -> null;
            case String s -> s.trim();
            case String[] array when array.length > 0 -> array[0].trim();
            default -> null;
        };
    }

    private static Duration readDuration(Map<String, Object> properties, String key, Duration fallback) {
        Object raw = properties.get(key);
        if (raw == null) {
            return fallback;
        }

        return switch (raw) {
            case Number number -> sanitizeDuration(number.longValue(), fallback);
            case String s when !s.isBlank() -> {
                try {
                    yield sanitizeDuration(Long.parseLong(s.trim()), fallback);
                } catch (NumberFormatException ignore) {
                    yield fallback;
                }
            }
            default -> fallback;
        };
    }

    private static Duration sanitizeDuration(long seconds, Duration fallback) {
        return Duration.ofSeconds(seconds <= 0 ? 0 : seconds);
    }
}
