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

import java.net.URI;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.eclipse.jdt.annotation.NonNullByDefault;
import org.eclipse.jdt.annotation.Nullable;
import org.openhab.core.common.ThreadPoolManager;
import org.openhab.core.common.registry.AbstractProvider;
import org.openhab.core.config.core.ConfigDescription;
import org.openhab.core.config.core.ConfigDescriptionParameter;
import org.openhab.core.config.core.ConfigDescriptionRegistry;
import org.openhab.core.config.core.ConfigUtil;
import org.openhab.core.config.core.Configuration;
import org.openhab.core.thing.Channel;
import org.openhab.core.thing.ChannelUID;
import org.openhab.core.thing.Thing;
import org.openhab.core.thing.ThingProvider;
import org.openhab.core.thing.ThingTypeUID;
import org.openhab.core.thing.ThingUID;
import org.openhab.core.thing.binding.builder.BridgeBuilder;
import org.openhab.core.thing.binding.builder.ChannelBuilder;
import org.openhab.core.thing.binding.builder.ThingBuilder;
import org.openhab.core.thing.type.AutoUpdatePolicy;
import org.openhab.core.thing.type.BridgeType;
import org.openhab.core.thing.type.ChannelDefinition;
import org.openhab.core.thing.type.ChannelGroupDefinition;
import org.openhab.core.thing.type.ChannelGroupType;
import org.openhab.core.thing.type.ChannelGroupTypeRegistry;
import org.openhab.core.thing.type.ChannelKind;
import org.openhab.core.thing.type.ChannelType;
import org.openhab.core.thing.type.ChannelTypeRegistry;
import org.openhab.core.thing.type.ChannelTypeUID;
import org.openhab.core.thing.type.ThingType;
import org.openhab.core.thing.type.ThingTypeRegistry;
import org.openhab.core.thing.util.ThingHelper;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Modified;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation that retrieves thing definitions from a JDBC compatible data source.
 *
 * @author Gaël L'hopital - Initial contribution
 */
@Component(service = { ThingProvider.class,
        JdbcThingProvider.class }, immediate = false, configurationPid = JdbcThingProviderConfiguration.CONFIG_PID)
@NonNullByDefault
public class JdbcThingProvider extends AbstractProvider<Thing> implements ThingProvider {
    private static final String TABLE_THINGS = "things";
    private static final String TABLE_THING_PROPERTIES = "thing_properties";
    private static final String TABLE_THING_CONFIG = "thing_config";
    private static final String TABLE_CHANNELS = "channels";
    private static final String TABLE_CHANNEL_PROPERTIES = "channel_properties";
    private static final String TABLE_CHANNEL_CONFIG = "channel_config";
    private static final String TABLE_CHANNEL_TAGS = "channel_tags";

    private final Logger logger = LoggerFactory.getLogger(JdbcThingProvider.class);
    private final ThingTypeRegistry thingTypeRegistry;
    private final ConfigDescriptionRegistry configDescriptionRegistry;
    private final ChannelTypeRegistry channelTypeRegistry;
    private final ChannelGroupTypeRegistry channelGroupTypeRegistry;
    private final ScheduledExecutorService scheduler = ThreadPoolManager
            .getScheduledPool(ThreadPoolManager.THREAD_POOL_NAME_COMMON);
    private final ConcurrentMap<ThingUID, Thing> providedThings = new ConcurrentHashMap<>();
    private final ConcurrentMap<ThingUID, ThingSnapshot> thingSnapshots = new ConcurrentHashMap<>();
    private final ReentrantLock reloadLock = new ReentrantLock();
    private final ReentrantLock schemaLock = new ReentrantLock();

    private volatile @Nullable JdbcThingProviderConfiguration configuration;
    private volatile @Nullable ScheduledFuture<?> refreshFuture;
    private volatile boolean schemaInitialized = false;

    @Activate
    public JdbcThingProvider(final @Reference ThingTypeRegistry thingTypeRegistry,
            final @Reference ConfigDescriptionRegistry configDescriptionRegistry,
            final @Reference ChannelTypeRegistry channelTypeRegistry,
            final @Reference ChannelGroupTypeRegistry channelGroupTypeRegistry,
            final @Nullable Map<String, Object> properties) {
        this.thingTypeRegistry = thingTypeRegistry;
        this.configDescriptionRegistry = configDescriptionRegistry;
        this.channelTypeRegistry = channelTypeRegistry;
        this.channelGroupTypeRegistry = channelGroupTypeRegistry;
        applyConfiguration(properties);
    }

    @Modified
    protected void modified(final @Nullable Map<String, Object> properties) {
        applyConfiguration(properties);
    }

    @Deactivate
    protected void deactivate() {
        cancelRefresh();
        clearProvidedThings();
    }

    @Override
    public Collection<Thing> getAll() {
        return List.copyOf(providedThings.values());
    }

    /**
     * Forces an immediate refresh of the backing data source.
     */
    public void refresh() {
        try {
            reloadFromDatabase();
        } catch (Exception e) {
            logger.warn("Scheduled JDBC refresh failed: {}", e.getMessage(), e);
        }
    }

    private void applyConfiguration(@Nullable Map<String, Object> properties) {
        cancelRefresh();

        var parsedConfig = JdbcThingProviderConfiguration.from(properties);
        if (parsedConfig == null) {
            logger.warn("JDBC thing provider disabled - mandatory property 'url' is missing.");
            configuration = null;
            clearProvidedThings();
            return;
        }

        configuration = parsedConfig;
        schemaInitialized = false;
        reloadFromDatabase();
        scheduleRefreshIfNecessary(parsedConfig.refreshInterval);
    }

    private void scheduleRefreshIfNecessary(Duration refreshInterval) {
        if (refreshInterval.isZero() || refreshInterval.isNegative()) {
            return;
        }
        refreshFuture = scheduler.scheduleWithFixedDelay(this::refresh, refreshInterval.getSeconds(),
                refreshInterval.getSeconds(), TimeUnit.SECONDS);
    }

    private void reloadFromDatabase() {
        JdbcThingProviderConfiguration cfg = configuration;
        if (cfg == null) {
            return;
        }

        Map<ThingUID, ThingDescriptor> freshThings = fetchThings(cfg);
        List<Thing> removed = new ArrayList<>();
        List<Thing> added = new ArrayList<>();
        List<Map.Entry<Thing, Thing>> updated = new ArrayList<>();

        reloadLock.lock();
        try {
            for (ThingUID existingUid : List.copyOf(providedThings.keySet())) {
                if (!freshThings.containsKey(existingUid)) {
                    Thing removedThing = providedThings.remove(existingUid);
                    thingSnapshots.remove(existingUid);
                    if (removedThing != null) {
                        removed.add(removedThing);
                    }
                }
            }
            for (Map.Entry<ThingUID, ThingDescriptor> entry : freshThings.entrySet()) {
                ThingUID uid = entry.getKey();
                ThingDescriptor descriptor = entry.getValue();
                Thing newThing = descriptor.thing();
                ThingSnapshot snapshot = descriptor.snapshot();
                Thing currentThing = providedThings.get(uid);
                if (currentThing == null) {
                    providedThings.put(uid, newThing);
                    thingSnapshots.put(uid, snapshot);
                    added.add(newThing);
                } else {
                    ThingSnapshot existingSnapshot = thingSnapshots.get(uid);
                    boolean snapshotChanged = existingSnapshot == null || !existingSnapshot.equals(snapshot);
                    boolean thingChanged = !ThingHelper.equals(currentThing, newThing);
                    if (thingChanged || snapshotChanged) {
                        providedThings.put(uid, newThing);
                        thingSnapshots.put(uid, snapshot);
                        updated.add(Map.entry(currentThing, newThing));
                    } else {
                        thingSnapshots.put(uid, snapshot);
                    }
                }
            }
        } finally {
            reloadLock.unlock();
        }

        removed.forEach(this::notifyListenersAboutRemovedElement);
        added.forEach(this::notifyListenersAboutAddedElement);
        updated.forEach(entry -> notifyListenersAboutUpdatedElement(entry.getKey(), entry.getValue()));
    }

    private Map<ThingUID, ThingDescriptor> fetchThings(JdbcThingProviderConfiguration cfg) {
        Map<ThingUID, ThingDescriptor> result = new HashMap<>();
        try (Connection connection = openConnection(cfg)) {
            ensureSchema(connection);

            Map<String, ThingRow> thingRows = loadThingRows(connection);
            if (thingRows.isEmpty()) {
                return result;
            }

            Map<String, Map<String, String>> thingProperties = loadScopedKeyValueMap(connection, TABLE_THING_PROPERTIES,
                    "thing_uid");
            Map<String, Map<String, String>> thingConfigRaw = loadScopedKeyValueMap(connection, TABLE_THING_CONFIG,
                    "thing_uid");
            Map<String, List<ChannelRow>> channelRows = loadChannelRows(connection);
            Map<String, Map<String, String>> channelProperties = loadScopedKeyValueMap(connection,
                    TABLE_CHANNEL_PROPERTIES, "channel_uid");
            Map<String, Map<String, String>> channelConfigRaw = loadScopedKeyValueMap(connection, TABLE_CHANNEL_CONFIG,
                    "channel_uid");
            Map<String, Set<String>> channelTags = loadChannelTags(connection);

            for (ThingRow row : thingRows.values()) {
                ensureThingDefaults(connection, row, thingProperties, thingConfigRaw, channelRows, channelProperties,
                        channelConfigRaw, channelTags);
            }

            for (ThingRow row : thingRows.values()) {
                ThingUID thingUID;
                try {
                    thingUID = new ThingUID(row.uid());
                } catch (IllegalArgumentException e) {
                    logger.warn("Skipping thing with invalid UID '{}': {}", row.uid(), e.getMessage());
                    continue;
                }

                ThingTypeUID thingTypeUID;
                try {
                    thingTypeUID = new ThingTypeUID(row.thingTypeUid());
                } catch (IllegalArgumentException e) {
                    logger.warn("Skipping thing '{}' with invalid thing type UID '{}': {}", row.uid(),
                            row.thingTypeUid(), e.getMessage());
                    continue;
                }

                ThingBuilder builder = determineBridgeFlag(thingTypeUID) ? BridgeBuilder.create(thingTypeUID, thingUID)
                        : ThingBuilder.create(thingTypeUID, thingUID);

                if (row.label() != null) {
                    builder.withLabel(row.label());
                }
                if (row.location() != null) {
                    builder.withLocation(row.location());
                }
                if (row.semanticEquipmentTag() != null) {
                    builder.withSemanticEquipmentTag(row.semanticEquipmentTag());
                }
                builder.withBridge(parseThingUID(row.bridgeUid()));

                Map<String, String> props = thingProperties.getOrDefault(row.uid(), Map.of());
                if (!props.isEmpty()) {
                    builder.withProperties(props);
                }

                Map<String, Object> configValues = convertConfiguration(thingConfigRaw.get(row.uid()));
                builder.withConfiguration(new Configuration(configValues));

                List<Channel> channels = buildChannels(row.uid(), channelRows.getOrDefault(row.uid(), List.of()),
                        channelProperties, channelConfigRaw, channelTags);
                if (!channels.isEmpty()) {
                    builder.withChannels(channels);
                }

                Thing thing = builder.build();
                Map<String, String> configurationSnapshot = thingConfigRaw.get(row.uid());
                ThingSnapshot snapshot = new ThingSnapshot(row.label(), row.location(),
                        configurationSnapshot != null ? configurationSnapshot : Map.of());
                result.put(thing.getUID(), new ThingDescriptor(thing, snapshot));
            }
        } catch (SQLException e) {
            logger.error("Failed to fetch things from JDBC source: {}", e.getMessage(), e);
        } catch (ClassNotFoundException e) {
            logger.error("Failed to load MariaDB driver: {}", e.getMessage(), e);
        }
        return result;
    }

    private void ensureThingDefaults(Connection connection, ThingRow row,
            Map<String, Map<String, String>> thingProperties, Map<String, Map<String, @Nullable String>> thingConfigRaw,
            Map<String, List<ChannelRow>> channelRows, Map<String, Map<String, String>> channelProperties,
            Map<String, Map<String, String>> channelConfigRaw, Map<String, Set<String>> channelTags) {
        ThingTypeUID typeUID;
        try {
            typeUID = new ThingTypeUID(row.thingTypeUid());
        } catch (IllegalArgumentException e) {
            logger.debug("Skipping default population for thing '{}' due to invalid type UID: {}", row.uid(),
                    e.getMessage());
            return;
        }

        ThingType thingType = thingTypeRegistry.getThingType(typeUID);
        if (thingType == null) {
            logger.debug("Unable to populate defaults for thing '{}' - unknown thing type '{}'", row.uid(), typeUID);
            return;
        }

        populateThingProperties(connection, row.uid(), thingType, thingProperties);
        populateThingConfiguration(connection, row.uid(), thingType, thingConfigRaw);
        populateChannels(connection, row.uid(), thingType, channelRows, channelProperties, channelConfigRaw,
                channelTags);
    }

    private void populateThingProperties(Connection connection, String thingUid, ThingType thingType,
            Map<String, Map<String, String>> thingProperties) {
        Map<String, String> existing = Objects
                .requireNonNull(thingProperties.computeIfAbsent(thingUid, key -> new HashMap<>()));
        for (Map.Entry<String, String> entry : thingType.getProperties().entrySet()) {
            if (existing.containsKey(entry.getKey())) {
                continue;
            }
            if (insertScopedValue(connection, TABLE_THING_PROPERTIES, "thing_uid", thingUid, entry.getKey(),
                    entry.getValue())) {
                existing.put(entry.getKey(), entry.getValue());
            }
        }
    }

    private void populateThingConfiguration(Connection connection, String thingUid, ThingType thingType,
            Map<String, Map<String, @Nullable String>> thingConfigRaw) {
        Configuration config = new Configuration();
        ConfigDescription configDescription = applyDefaultThingConfiguration(config, thingType);
        Set<String> requiredParameters = Collections.emptySet();
        if (configDescription != null) {
            List<ConfigDescriptionParameter> parameters = configDescription.getParameters();
            if (!parameters.isEmpty()) {
                Set<String> names = new LinkedHashSet<>();
                for (ConfigDescriptionParameter parameter : parameters) {
                    if (parameter.isRequired()) {
                        names.add(parameter.getName());
                    }
                }
                requiredParameters = names;
            }
        }

        if (config.keySet().isEmpty() && requiredParameters.isEmpty()) {
            return;
        }

        Map<String, @Nullable String> existing = Objects
                .requireNonNull(thingConfigRaw.computeIfAbsent(thingUid, key -> new HashMap<>()));
        for (Map.Entry<String, Object> entry : config.getProperties().entrySet()) {
            if (existing.containsKey(entry.getKey())) {
                continue;
            }
            String value = convertConfigurationValue(entry.getValue());
            if (insertScopedValue(connection, TABLE_THING_CONFIG, "thing_uid", thingUid, entry.getKey(), value)) {
                existing.put(entry.getKey(), value);
            }
        }

        for (String parameter : requiredParameters) {
            if (existing.containsKey(parameter)) {
                continue;
            }
            if (insertScopedValue(connection, TABLE_THING_CONFIG, "thing_uid", thingUid, parameter, null)) {
                existing.put(parameter, null);
            }
        }
    }

    private void populateChannels(Connection connection, String thingUid, ThingType thingType,
            Map<String, List<ChannelRow>> channelRows, Map<String, Map<String, String>> channelProperties,
            Map<String, Map<String, String>> channelConfigRaw, Map<String, Set<String>> channelTags) {
        final ThingUID resolvedThingUid;
        try {
            resolvedThingUid = new ThingUID(thingUid);
        } catch (IllegalArgumentException e) {
            logger.debug("Cannot create default channels - invalid thing UID '{}': {}", thingUid, e.getMessage());
            return;
        }

        List<Channel> defaultChannels = createDefaultChannels(thingType, resolvedThingUid);
        if (defaultChannels.isEmpty()) {
            return;
        }

        List<ChannelRow> existingRows = Objects
                .requireNonNull(channelRows.computeIfAbsent(thingUid, key -> new ArrayList<>()));
        Map<String, ChannelRow> existingByUid = new HashMap<>();
        for (ChannelRow row : existingRows) {
            existingByUid.put(row.uid(), row);
        }

        for (Channel channel : defaultChannels) {
            String channelUid = channel.getUID().getAsString();
            ChannelRow currentRow = existingByUid.get(channelUid);
            if (currentRow == null && insertChannel(connection, channel, thingUid)) {
                var channelTypeUID = channel.getChannelTypeUID();
                var autoUpdatePolicy = channel.getAutoUpdatePolicy();
                ChannelRow newRow = new ChannelRow(channelUid, thingUid,
                        channelTypeUID != null ? channelTypeUID.toString() : null, channel.getAcceptedItemType(),
                        channel.getKind().name(), channel.getLabel(), channel.getDescription(),
                        autoUpdatePolicy != null ? autoUpdatePolicy.name() : null);
                existingRows.add(newRow);
                existingByUid.put(channelUid, newRow);
            }

            populateChannelProperties(connection, channelUid, channel.getProperties(), channelProperties);
            populateChannelConfiguration(connection, channelUid, channel.getConfiguration(), channelConfigRaw);
            populateChannelTags(connection, channelUid, channel.getDefaultTags(), channelTags);
        }
    }

    private void populateChannelProperties(Connection connection, String channelUid, Map<String, String> defaults,
            Map<String, Map<String, String>> channelProperties) {
        if (defaults.isEmpty()) {
            return;
        }
        Map<String, String> existing = Objects
                .requireNonNull(channelProperties.computeIfAbsent(channelUid, key -> new HashMap<>()));
        for (Map.Entry<String, String> entry : defaults.entrySet()) {
            if (existing.containsKey(entry.getKey())) {
                continue;
            }
            if (insertScopedValue(connection, TABLE_CHANNEL_PROPERTIES, "channel_uid", channelUid, entry.getKey(),
                    entry.getValue())) {
                existing.put(entry.getKey(), entry.getValue());
            }
        }
    }

    private void populateChannelConfiguration(Connection connection, String channelUid, Configuration configuration,
            Map<String, Map<String, String>> channelConfigRaw) {
        if (configuration.keySet().isEmpty()) {
            return;
        }
        Map<String, String> existing = Objects
                .requireNonNull(channelConfigRaw.computeIfAbsent(channelUid, key -> new HashMap<>()));
        for (Map.Entry<String, Object> entry : configuration.getProperties().entrySet()) {
            if (existing.containsKey(entry.getKey())) {
                continue;
            }
            String value = convertConfigurationValue(entry.getValue());
            if (insertScopedValue(connection, TABLE_CHANNEL_CONFIG, "channel_uid", channelUid, entry.getKey(), value)) {
                existing.put(entry.getKey(), value);
            }
        }
    }

    private void populateChannelTags(Connection connection, String channelUid, Set<String> defaults,
            Map<String, Set<String>> channelTags) {
        if (defaults.isEmpty()) {
            return;
        }
        Set<String> existing = Objects.requireNonNull(channelTags.computeIfAbsent(channelUid, key -> new HashSet<>()));
        for (String tag : defaults) {
            if (existing.contains(tag)) {
                continue;
            }
            if (insertChannelTag(connection, channelUid, tag)) {
                existing.add(tag);
            }
        }
    }

    private record ThingDescriptor(Thing thing, ThingSnapshot snapshot) {
    }

    private record ThingSnapshot(@Nullable String label, @Nullable String location, Map<String, String> configuration) {
        ThingSnapshot {
            configuration = Collections.unmodifiableMap(new HashMap<>(configuration));
        }
    }

    private @Nullable ConfigDescription applyDefaultThingConfiguration(Configuration configuration,
            ThingType thingType) {
        URI configDescriptionUri = thingType.getConfigDescriptionURI();
        if (configDescriptionUri == null) {
            return null;
        }
        ConfigDescription configDescription = configDescriptionRegistry.getConfigDescription(configDescriptionUri);
        if (configDescription == null) {
            return null;
        }
        ConfigUtil.applyDefaultConfiguration(configuration, configDescription);
        return configDescription;
    }

    private void applyDefaultChannelConfiguration(Configuration configuration, ChannelType channelType) {
        URI configDescriptionUri = channelType.getConfigDescriptionURI();
        if (configDescriptionUri == null) {
            return;
        }
        ConfigDescription configDescription = configDescriptionRegistry.getConfigDescription(configDescriptionUri);
        if (configDescription == null) {
            return;
        }
        ConfigUtil.applyDefaultConfiguration(configuration, configDescription);
    }

    private List<Channel> createDefaultChannels(ThingType thingType, ThingUID thingUID) {
        List<Channel> channels = new ArrayList<>();
        for (ChannelDefinition channelDefinition : thingType.getChannelDefinitions()) {
            Channel channel = createDefaultChannel(thingUID, null, channelDefinition);
            if (channel != null) {
                channels.add(channel);
            }
        }
        for (ChannelGroupDefinition channelGroupDefinition : thingType.getChannelGroupDefinitions()) {
            ChannelGroupType channelGroupType = channelGroupTypeRegistry
                    .getChannelGroupType(channelGroupDefinition.getTypeUID());
            if (channelGroupType == null) {
                logger.warn(
                        "Could not create channels for channel group '{}' for thing '{}', because channel group type '{}' could not be found.",
                        channelGroupDefinition.getId(), thingUID, channelGroupDefinition.getTypeUID());
                continue;
            }
            for (ChannelDefinition channelDefinition : channelGroupType.getChannelDefinitions()) {
                Channel channel = createDefaultChannel(thingUID, channelGroupDefinition.getId(), channelDefinition);
                if (channel != null) {
                    channels.add(channel);
                }
            }
        }
        return channels;
    }

    private @Nullable Channel createDefaultChannel(ThingUID thingUID, @Nullable String groupId,
            ChannelDefinition channelDefinition) {
        ChannelType channelType = channelTypeRegistry.getChannelType(channelDefinition.getChannelTypeUID());
        if (channelType == null) {
            logger.warn("Could not create channel '{}', because channel type '{}' could not be found.",
                    channelDefinition.getId(), channelDefinition.getChannelTypeUID());
            return null;
        }

        ChannelUID channelUID;
        if (groupId == null) {
            channelUID = new ChannelUID(thingUID, channelDefinition.getId());
        } else {
            channelUID = new ChannelUID(thingUID, groupId, channelDefinition.getId());

        }

        String label = channelDefinition.getLabel();
        if (label == null) {
            label = channelType.getLabel();
        }

        AutoUpdatePolicy autoUpdatePolicy = channelDefinition.getAutoUpdatePolicy();
        if (autoUpdatePolicy == null) {
            autoUpdatePolicy = channelType.getAutoUpdatePolicy();
        }

        ChannelBuilder channelBuilder = ChannelBuilder.create(channelUID, channelType.getItemType()) //
                .withType(channelType.getUID()) //
                .withDefaultTags(channelType.getTags()) //
                .withKind(channelType.getKind()) //
                .withLabel(label) //
                .withAutoUpdatePolicy(autoUpdatePolicy);

        String description = channelDefinition.getDescription();
        if (description == null) {
            description = channelType.getDescription();
        }
        if (description != null) {
            channelBuilder.withDescription(description);
        }

        if (channelType.getConfigDescriptionURI() != null) {
            Configuration configuration = new Configuration();
            applyDefaultChannelConfiguration(configuration, channelType);
            channelBuilder.withConfiguration(configuration);
        }

        return channelBuilder.withProperties(channelDefinition.getProperties()).build();
    }

    private boolean insertChannel(Connection connection, Channel channel, String thingUid) {
        String sql = "INSERT INTO " + TABLE_CHANNELS
                + " (uid, thing_uid, channel_type_uid, item_type, kind, label, description, auto_update_policy) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, channel.getUID().getAsString());
            statement.setString(2, thingUid);
            ChannelTypeUID channelTypeUID = channel.getChannelTypeUID();
            statement.setString(3, channelTypeUID != null ? channelTypeUID.toString() : null);
            statement.setString(4, channel.getAcceptedItemType());
            statement.setString(5, channel.getKind().name());
            statement.setString(6, channel.getLabel());
            statement.setString(7, channel.getDescription());
            AutoUpdatePolicy policy = channel.getAutoUpdatePolicy();
            statement.setString(8, policy != null ? policy.name() : null);
            statement.executeUpdate();
            logger.debug("Created default channel '{}' for thing '{}'", channel.getUID(), thingUid);
            return true;
        } catch (SQLException e) {
            if (isDuplicateKeyException(e)) {
                return false;
            }
            logger.warn("Failed to insert channel '{}' for thing '{}': {}", channel.getUID(), thingUid, e.getMessage(),
                    e);
            return false;
        }
    }

    private boolean insertChannelTag(Connection connection, String channelUid, String tag) {
        String sql = "INSERT INTO " + TABLE_CHANNEL_TAGS + " (channel_uid, tag) VALUES (?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, channelUid);
            statement.setString(2, tag);
            statement.executeUpdate();
            return true;
        } catch (SQLException e) {
            if (isDuplicateKeyException(e)) {
                return false;
            }
            logger.warn("Failed to insert tag '{}' for channel '{}': {}", tag, channelUid, e.getMessage(), e);
            return false;
        }
    }

    private boolean insertScopedValue(Connection connection, String table, String scopeColumn, String scope,
            String name, @Nullable String value) {
        String sql = "INSERT INTO " + table + " (" + scopeColumn + ", name, value) VALUES (?, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, scope);
            statement.setString(2, name);
            statement.setString(3, value);
            statement.executeUpdate();
            return true;
        } catch (SQLException e) {
            if (isDuplicateKeyException(e)) {
                return false;
            }
            logger.warn("Failed to insert '{}' for scope '{}' in table '{}': {}", name, scope, table, e.getMessage(),
                    e);
            return false;
        }
    }

    private String convertConfigurationValue(@Nullable Object value) {
        return value == null ? "" : String.valueOf(value);
    }

    private boolean isDuplicateKeyException(SQLException e) {
        if (e instanceof SQLIntegrityConstraintViolationException) {
            return true;
        }
        String sqlState = e.getSQLState();
        if (sqlState != null) {
            return sqlState.startsWith("23");
        }
        Throwable cause = e.getCause();
        return cause instanceof SQLIntegrityConstraintViolationException;
    }

    private List<Channel> buildChannels(String thingUid, List<ChannelRow> rows,
            Map<String, Map<String, String>> channelProperties, Map<String, Map<String, String>> channelConfig,
            Map<String, Set<String>> channelTags) {
        List<Channel> channels = new ArrayList<>(rows.size());
        for (ChannelRow row : rows) {
            ChannelUID channelUID;
            try {
                channelUID = new ChannelUID(row.uid());
            } catch (IllegalArgumentException e) {
                logger.warn("Skipping channel with invalid UID '{}' for thing '{}': {}", row.uid(), thingUid,
                        e.getMessage());
                continue;
            }

            if (!Objects.equals(channelUID.getThingUID().toString(), thingUid)) {
                logger.warn("Channel '{}' references thing '{}' but row belongs to thing '{}'. Skipping.", row.uid(),
                        channelUID.getThingUID(), thingUid);
                continue;
            }

            ChannelBuilder builder = ChannelBuilder.create(channelUID, row.itemType());
            builder.withKind(parseChannelKind(row.kind()));
            builder.withConfiguration(new Configuration(convertConfiguration(channelConfig.get(row.uid()))));

            Map<String, String> properties = channelProperties.getOrDefault(row.uid(), Map.of());
            if (!properties.isEmpty()) {
                builder.withProperties(Map.copyOf(properties));
            }

            Set<String> tags = channelTags.getOrDefault(row.uid(), Set.of());
            if (!tags.isEmpty()) {
                builder.withDefaultTags(Set.copyOf(tags));
            }

            if (row.channelTypeUid() instanceof String local && !local.isBlank()) {
                try {
                    builder.withType(new ChannelTypeUID(local));
                } catch (IllegalArgumentException e) {
                    logger.warn("Channel '{}' declares invalid channel type UID '{}': {}", row.uid(), local,
                            e.getMessage());
                }
            }

            if (row.label() instanceof String local) {
                builder.withLabel(local);
            }
            if (row.description() instanceof String local) {
                builder.withDescription(local);
            }
            builder.withAutoUpdatePolicy(parseAutoUpdatePolicy(row.autoUpdatePolicy()));

            channels.add(builder.build());
        }
        return channels;
    }

    private boolean determineBridgeFlag(ThingTypeUID thingTypeUID) {
        ThingType thingType = thingTypeRegistry.getThingType(thingTypeUID);
        return thingType instanceof BridgeType;
    }

    private Map<String, ThingRow> loadThingRows(Connection connection) throws SQLException {
        Map<String, ThingRow> rows = new HashMap<>();
        String sql = "SELECT * FROM " + TABLE_THINGS;
        try (PreparedStatement statement = connection.prepareStatement(sql); ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                String uid = rs.getString("uid");
                String thingTypeUid = rs.getString("thing_type_uid");
                if (uid == null || thingTypeUid == null) {
                    logger.warn("Encountered thing row with missing UID or thing type UID - skipping entry.");
                    continue;
                }
                rows.put(uid, new ThingRow(uid, thingTypeUid, rs.getString("label"), rs.getString("bridge_uid"),
                        rs.getString("location"), rs.getString("semantic_equipment_tag")));
            }
        }
        return rows;
    }

    private Map<String, List<ChannelRow>> loadChannelRows(Connection connection) throws SQLException {
        Map<String, List<ChannelRow>> rows = new HashMap<>();
        String sql = "SELECT * FROM " + TABLE_CHANNELS;
        try (PreparedStatement statement = connection.prepareStatement(sql); ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                String uid = rs.getString("uid");
                String thingUid = rs.getString("thing_uid");
                if (uid == null || thingUid == null) {
                    logger.warn("Encountered channel row with missing UID or thing UID - skipping entry.");
                    continue;
                }
                ChannelRow row = new ChannelRow(uid, thingUid, rs.getString("channel_type_uid"),
                        rs.getString("item_type"), rs.getString("kind"), rs.getString("label"),
                        rs.getString("description"), rs.getString("auto_update_policy"));
                Objects.requireNonNull(rows.computeIfAbsent(thingUid, key -> new ArrayList<>())).add(row);
            }
        }
        return rows;
    }

    private Map<String, Map<String, String>> loadScopedKeyValueMap(Connection connection, String table,
            String scopeColumn) throws SQLException {
        Map<String, Map<String, String>> map = new HashMap<>();
        String sql = "SELECT " + scopeColumn + ", name, value FROM " + table;
        try (PreparedStatement statement = connection.prepareStatement(sql); ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                String scope = rs.getString(scopeColumn);
                String name = rs.getString("name");
                if (scope == null || name == null) {
                    continue;
                }
                String value = rs.getString("value");
                Objects.requireNonNull(map.computeIfAbsent(scope, key -> new HashMap<>())).put(name, value);
            }
        }
        return map;
    }

    private Map<String, Set<String>> loadChannelTags(Connection connection) throws SQLException {
        Map<String, Set<String>> map = new HashMap<>();
        String sql = "SELECT * FROM " + TABLE_CHANNEL_TAGS;
        try (PreparedStatement statement = connection.prepareStatement(sql); ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                String channelUid = rs.getString("channel_uid");
                String tag = rs.getString("tag");
                if (channelUid == null || tag == null) {
                    continue;
                }
                Objects.requireNonNull(map.computeIfAbsent(channelUid, key -> new HashSet<>())).add(tag);
            }
        }
        return map;
    }

    private Map<String, Object> convertConfiguration(@Nullable Map<String, String> rawConfig) {
        if (rawConfig == null || rawConfig.isEmpty()) {
            return Map.of();
        }

        Map<String, Object> result = new HashMap<>();
        rawConfig.forEach((key, value) -> result.put(key, convertScalar(value)));
        return result;
    }

    private Object convertScalar(@Nullable String value) {
        return switch (value) {
            case null -> "";
            case String v -> {
                String trimmed = v.trim();
                yield switch (trimmed) {
                    case "" -> "";
                    case String bool when bool.equalsIgnoreCase("true") || bool.equalsIgnoreCase("false") ->
                        Boolean.parseBoolean(bool);
                    default -> parseNumberOrFallback(trimmed);
                };
            }
        };
    }

    private Object parseNumberOrFallback(String trimmed) {
        try {
            if (!trimmed.contains(".")) {
                return Long.parseLong(trimmed);
            }
        } catch (NumberFormatException ignore) {
            // fall through and try parsing as double
        }
        try {
            return Double.parseDouble(trimmed);
        } catch (NumberFormatException ignore) {
            return trimmed;
        }
    }

    private ChannelKind parseChannelKind(@Nullable String value) {
        return switch (value) {
            case null -> ChannelKind.STATE;
            case String s when s.isBlank() -> ChannelKind.STATE;
            case String s -> {
                try {
                    yield ChannelKind.valueOf(s.trim().toUpperCase(Locale.ROOT));
                } catch (IllegalArgumentException e) {
                    logger.warn("Unknown channel kind '{}', defaulting to STATE.", value);
                    yield ChannelKind.STATE;
                }
            }
        };
    }

    private @Nullable AutoUpdatePolicy parseAutoUpdatePolicy(@Nullable String value) {
        return switch (value) {
            case null -> null;
            case String s when s.isBlank() -> null;
            case String s -> {
                try {
                    yield AutoUpdatePolicy.valueOf(s.trim().toUpperCase(Locale.ROOT));
                } catch (IllegalArgumentException e) {
                    logger.warn("Unknown auto update policy '{}'.", value);
                    yield null;
                }
            }
        };
    }

    private @Nullable ThingUID parseThingUID(@Nullable String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return new ThingUID(value);
        } catch (IllegalArgumentException e) {
            logger.warn("Configured bridge UID '{}' is invalid: {}", value, e.getMessage());
            return null;
        }
    }

    private void ensureSchema(Connection connection) throws SQLException {
        if (schemaInitialized) {
            return;
        }
        schemaLock.lock();
        try {
            if (schemaInitialized) {
                return;
            }
            if (!tableExists(connection, TABLE_THINGS)) {
                createSchema(connection);
            }
            schemaInitialized = true;
        } finally {
            schemaLock.unlock();
        }
    }

    private boolean tableExists(Connection connection, String tableName) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet rs = metaData.getTables(null, null, tableName, null)) {
            if (rs.next()) {
                return true;
            }
        }
        String upper = tableName.toUpperCase(Locale.ROOT);
        if (!upper.equals(tableName)) {
            try (ResultSet rs = metaData.getTables(null, null, upper, null)) {
                if (rs.next()) {
                    return true;
                }
            }
        }
        String lower = tableName.toLowerCase(Locale.ROOT);
        if (!lower.equals(tableName)) {
            try (ResultSet rs = metaData.getTables(null, null, lower, null)) {
                return rs.next();
            }
        }
        return false;
    }

    private void createSchema(Connection connection) throws SQLException {
        logger.info("Creating JDBC thing provider schema.");
        List<String> statements = List.of("CREATE TABLE " + TABLE_THINGS
                + " (uid VARCHAR(255) NOT NULL, thing_type_uid VARCHAR(255) NOT NULL, "
                + "label VARCHAR(255), bridge_uid VARCHAR(255), location VARCHAR(255), semantic_equipment_tag VARCHAR(255), "
                + "PRIMARY KEY (uid))",
                "CREATE TABLE " + TABLE_THING_PROPERTIES
                        + " (thing_uid VARCHAR(255) NOT NULL, name VARCHAR(255) NOT NULL, value TEXT, "
                        + "PRIMARY KEY (thing_uid, name), " + "FOREIGN KEY (thing_uid) REFERENCES " + TABLE_THINGS
                        + "(uid) ON DELETE CASCADE ON UPDATE CASCADE)",
                "CREATE TABLE " + TABLE_THING_CONFIG
                        + " (thing_uid VARCHAR(255) NOT NULL, name VARCHAR(255) NOT NULL, value TEXT, "
                        + "PRIMARY KEY (thing_uid, name), " + "FOREIGN KEY (thing_uid) REFERENCES " + TABLE_THINGS
                        + "(uid) ON DELETE CASCADE ON UPDATE CASCADE)",
                "CREATE TABLE " + TABLE_CHANNELS
                        + " (uid VARCHAR(255) PRIMARY KEY, thing_uid VARCHAR(255) NOT NULL, channel_type_uid VARCHAR(255), "
                        + "item_type VARCHAR(255), kind VARCHAR(32) NOT NULL, label VARCHAR(255), description TEXT, "
                        + "auto_update_policy VARCHAR(32), FOREIGN KEY (thing_uid) REFERENCES " + TABLE_THINGS
                        + "(uid) ON DELETE CASCADE ON UPDATE CASCADE)",
                "CREATE TABLE " + TABLE_CHANNEL_PROPERTIES
                        + " (channel_uid VARCHAR(255) NOT NULL, name VARCHAR(255) NOT NULL, value TEXT, "
                        + "PRIMARY KEY (channel_uid, name), " + "FOREIGN KEY (channel_uid) REFERENCES " + TABLE_CHANNELS
                        + "(uid) ON DELETE CASCADE ON UPDATE CASCADE)",
                "CREATE TABLE " + TABLE_CHANNEL_CONFIG
                        + " (channel_uid VARCHAR(255) NOT NULL, name VARCHAR(255) NOT NULL, value TEXT, "
                        + "PRIMARY KEY (channel_uid, name), " + "FOREIGN KEY (channel_uid) REFERENCES " + TABLE_CHANNELS
                        + "(uid) ON DELETE CASCADE ON UPDATE CASCADE)",
                "CREATE TABLE " + TABLE_CHANNEL_TAGS
                        + " (channel_uid VARCHAR(255) NOT NULL, tag VARCHAR(255) NOT NULL, "
                        + "PRIMARY KEY (channel_uid, tag), " + "FOREIGN KEY (channel_uid) REFERENCES " + TABLE_CHANNELS
                        + "(uid) ON DELETE CASCADE ON UPDATE CASCADE)");

        try (Statement statement = connection.createStatement()) {
            for (String ddl : statements) {
                statement.executeUpdate(ddl);
            }
        }
    }

    private Connection openConnection(JdbcThingProviderConfiguration cfg) throws SQLException, ClassNotFoundException {
        Class.forName("org.mariadb.jdbc.Driver");

        Properties properties = new Properties();
        if (cfg.username instanceof String username) {
            properties.setProperty("user", username);
        }
        if (cfg.password instanceof String password) {
            properties.setProperty("password", password);
        }
        if (properties.isEmpty()) {
            return DriverManager.getConnection(cfg.url);
        }
        return DriverManager.getConnection(cfg.url, properties);
    }

    private void cancelRefresh() {
        if (refreshFuture instanceof ScheduledFuture future) {
            future.cancel(true);
        }
        refreshFuture = null;
    }

    private void clearProvidedThings() {
        List<Thing> removed;
        reloadLock.lock();
        try {
            if (providedThings.isEmpty()) {
                thingSnapshots.clear();
                return;
            }
            removed = List.copyOf(providedThings.values());
            providedThings.clear();
            thingSnapshots.clear();
        } finally {
            reloadLock.unlock();
        }
        removed.forEach(this::notifyListenersAboutRemovedElement);
    }

    private record ThingRow(String uid, String thingTypeUid, @Nullable String label, @Nullable String bridgeUid,
            @Nullable String location, @Nullable String semanticEquipmentTag) {
    }

    private record ChannelRow(String uid, String thingUid, @Nullable String channelTypeUid, @Nullable String itemType,
            String kind, @Nullable String label, @Nullable String description, @Nullable String autoUpdatePolicy) {
    }
}
