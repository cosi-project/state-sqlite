-- There are two tables:
-- 1. resources: stores the actual resource data
-- 2. events: stores events as they happened to resources

CREATE TABLE IF NOT EXISTS resources (
    namespace TEXT NOT NULL,
    type TEXT NOT NULL,
    id TEXT NOT NULL,
    version INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    labels BLOB NULL,
    phase INTEGER NOT NULL,
    owner TEXT NOT NULL,
    spec BLOB NOT NULL,
    PRIMARY KEY (namespace, type, id)
) WITHOUT ROWID, STRICT;

CREATE TABLE IF NOT EXISTS events (
    event_id INTEGER NOT NULL PRIMARY KEY,
    namespace TEXT NOT NULL,
    type TEXT NOT NULL,
    id TEXT NOT NULL,
    event_timestamp INTEGER NOT NULL,
    event_type INTEGER NOT NULL,
    spec_before BLOB NULL,
    spec_after BLOB NULL
) STRICT;

CREATE TRIGGER IF NOT EXISTS trg_resources_after_insert
AFTER INSERT ON resources
BEGIN
    INSERT INTO events (namespace, type, id, event_timestamp, event_type, spec_before, spec_after)
    VALUES (NEW.namespace, NEW.type, NEW.id, unixepoch(), 1, NULL, NEW.spec);
END;

CREATE TRIGGER IF NOT EXISTS trg_resources_after_update
AFTER UPDATE ON resources
BEGIN
    INSERT INTO events (namespace, type, id, event_timestamp, event_type, spec_before, spec_after)
    VALUES (NEW.namespace, NEW.type, NEW.id, unixepoch(), 2, OLD.spec, NEW.spec);
END;

CREATE TRIGGER IF NOT EXISTS trg_resources_after_delete
AFTER DELETE ON resources
BEGIN
    INSERT INTO events (namespace, type, id, event_timestamp, event_type, spec_before, spec_after)
    VALUES (OLD.namespace, OLD.type, OLD.id, unixepoch(), 3, OLD.spec, NULL);
END;
