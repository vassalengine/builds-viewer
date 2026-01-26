CREATE TABLE builds (
    id INTEGER PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    url TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL
);
