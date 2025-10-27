-- There are two tables:
-- 1. resources: stores the actual resource data
-- 2. events: stores events as they happened to resources
--
-- Tables can be prefixed with a custom prefix to allow multiple COSI
-- state instances to share the same database.

CREATE TABLE IF NOT EXISTS %[1]sresources (
    namespace TEXT NOT NULL,
    type TEXT NOT NULL,
    id TEXT NOT NULL,
    -- resource metadata is pulled up as fields for easier access/search
    version INTEGER NOT NULL,
    created_at INTEGER NOT NULL, -- unix epoch timestamp
    updated_at INTEGER NOT NULL, -- unix epoch timestamp
    labels BLOB NULL, -- stored as JSONB
    finalizers BLOB NULL, -- stored as JSONB
    phase INTEGER NOT NULL, -- stored as integer value of Phase enum
    owner TEXT NOT NULL, -- stored as string
    spec BLOB NOT NULL, -- marshalled full resource contents
    PRIMARY KEY (namespace, type, id) -- not using ROWID, this is real primary key
) WITHOUT ROWID, STRICT;

CREATE TABLE IF NOT EXISTS %[1]sevents (
    event_id INTEGER NOT NULL PRIMARY KEY, -- eventid is going to be ROWID
    namespace TEXT NOT NULL,
    type TEXT NOT NULL,
    id TEXT NOT NULL,
    event_timestamp INTEGER NOT NULL, -- time the event got inserted
    event_type INTEGER NOT NULL, -- 1 = create, 2 = update, 3 = delete
    spec_before BLOB NULL, -- full resource contents before the event
    spec_after BLOB NULL -- full resource contents after the event
) STRICT;

CREATE TRIGGER IF NOT EXISTS trg_%[1]sresources_after_insert
AFTER INSERT ON %[1]sresources
BEGIN
    INSERT INTO %[1]sevents (namespace, type, id, event_timestamp, event_type, spec_before, spec_after)
    VALUES (NEW.namespace, NEW.type, NEW.id, unixepoch(), 1, NULL, NEW.spec);
END;

CREATE TRIGGER IF NOT EXISTS trg_%[1]sresources_after_update
AFTER UPDATE ON %[1]sresources
BEGIN
    INSERT INTO %[1]sevents (namespace, type, id, event_timestamp, event_type, spec_before, spec_after)
    VALUES (NEW.namespace, NEW.type, NEW.id, unixepoch(), 2, OLD.spec, NEW.spec);
END;

CREATE TRIGGER IF NOT EXISTS trg_%[1]sresources_after_delete
AFTER DELETE ON %[1]sresources
BEGIN
    INSERT INTO %[1]sevents (namespace, type, id, event_timestamp, event_type, spec_before, spec_after)
    VALUES (OLD.namespace, OLD.type, OLD.id, unixepoch(), 3, OLD.spec, NULL);
END;
