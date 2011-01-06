-- Describe KEYVAL
CREATE TABLE keyval (
    "key" TEXT NOT NULL,
    "collection" TEXT NOT NULL,
    "value" BLOB NOT NULL
)

-- Describe OBJECT_UNIQ
CREATE UNIQUE INDEX "object_uniq" on keyval (key ASC, collection ASC)

