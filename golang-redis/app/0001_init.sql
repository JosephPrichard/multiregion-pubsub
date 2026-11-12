CREATE TABLE IF NOT EXISTS vending_machines (
    vending_machine_id       TEXT PRIMARY KEY,
    machine_state            TEXT NOT NULL,
    last_updated_timestamp   TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    last_purchase_product_id TEXT DEFAULT NULL -- NULLABLE
);

CREATE TABLE IF NOT EXISTS products (
    product_id         TEXT PRIMARY KEY,
    vending_machine_id TEXT NOT NULL,
    name               TEXT NOT NULL,
    price              NUMERIC NOT NULL,
    qty                INTEGER NOT NULL,
    FOREIGN KEY (vending_machine_id) REFERENCES vending_machines (vending_machine_id)
);

ALTER TABLE vending_machines DROP CONSTRAINT IF EXISTS fk_pending_product_purchase;

ALTER TABLE vending_machines
ADD CONSTRAINT fk_pending_product_purchase
FOREIGN KEY (last_purchase_product_id) REFERENCES products(product_id);