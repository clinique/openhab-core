# JDBC Thing Provider

This bundle contributes a `ThingProvider` implementation that reads statically defined
things from a relational database via JDBC. It is useful whenever the canonical
source for thing metadata lives outside the openHAB file system (e.g. when an
existing CMDB already stores the desired topology).

## Configuration

The provider is configured through the Config Admin PID `org.openhab.core.thing.jdbc`.
The following properties are recognised:

| Property | Required | Description |
|----------|----------|-------------|
| `url` | yes | JDBC connection URL (for example `jdbc:mariadb://localhost:3306/openhab`). |
| `username` | no | Username used when opening the connection. |
| `password` | no | Password used when opening the connection. |
| `driverClass` | no | Fully qualified class name of the JDBC driver. Normally not required when the JDBC persistence add-on is installed (its DataSourceFactory will be reused automatically). |
| `refreshInterval` | no | Interval in seconds for periodic reloads. Set to `0` to disable the scheduler. Defaults to 300 seconds. |

> Tip: on a classic installation the properties can be supplied in
> `conf/services/org.openhab.core.thing.jdbc.cfg`.

## Table layout & schema generation

When the provider connects to an empty database it will automatically create the schema it requires. The following
tables are managed:

* `things` – basic attributes for every thing (UID, thing type UID, label, bridge, location, semantic tag)
* `thing_properties` – key/value pairs attached to a thing
* `thing_config` – configuration entries for a thing (stored as scalar values; numbers and booleans are detected automatically)
* `channels` – channel definitions (UID, type UID, item type, kind, label, description, auto-update policy)
* `channel_properties` – key/value pairs attached to a channel
* `channel_config` – configuration entries for a channel
* `channel_tags` – default tags assigned to a channel

For existing databases you can create the schema manually by using the statements `JdbcThingProvider` emits on startup.
The auto-generation is idempotent and only runs when `things` is missing.

### Minimal example

```sql
INSERT INTO things (uid, thing_type_uid, label, bridge_uid)
VALUES ('mqtt:topic:office:lamp', 'mqtt:topic', 'Office Lamp', 'mqtt:broker:main');

INSERT INTO thing_config (thing_uid, name, value)
VALUES ('mqtt:topic:office:lamp', 'availabilityTopic', 'devices/lamp/availability');

INSERT INTO channels (uid, thing_uid, channel_type_uid, item_type, kind, label)
VALUES ('mqtt:topic:office:lamp:power', 'mqtt:topic:office:lamp', 'mqtt:switch', 'Switch', 'STATE', 'Power');

INSERT INTO channel_tags (channel_uid, tag)
VALUES ('mqtt:topic:office:lamp:power', 'Lighting');
```

All channel configuration values must reference the full channel UID (including the thing part) and the bridge UID
column can stay `NULL` for stand-alone things.

## Behaviour

* Missing or invalid rows are skipped and reported in the log.
* When a thing disappears from the database the provider notifies the registry about the removal.
* If `oh_things.is_bridge` is `NULL`, the bridge flag is derived from the registered `ThingType`.
* The provider exposes a `refresh()` method for manual reloading through scripting or the OSGi console.
